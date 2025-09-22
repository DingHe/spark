/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.{HashMap, HashSet, Queue}
import scala.concurrent.Future

import com.google.common.cache.CacheBuilder
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.{ExecutorAllocationClient, SparkEnv, TaskState}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.ExecutorLogUrlHandler
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Network._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend.ENDPOINT_NAME
import org.apache.spark.util.{RpcUtils, SerializableBuffer, ThreadUtils, Utils}

/**
 * A scheduler backend that waits for coarse-grained executors to connect.
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 */
//用于与集群进行通信的调度后端类，特别是在执行器连接后，保持每个执行器的生命周期，避免每次任务完成后都启动新的执行器
private[spark]
class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv)
  extends ExecutorAllocationClient with SchedulerBackend with Logging {

  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  protected val totalCoreCount = new AtomicInteger(0)  //用于追踪集群中总的 CPU 核心数
  // Total number of executors that are currently registered
  protected val totalRegisteredExecutors = new AtomicInteger(0)  //当前已注册的执行器数量
  protected val conf = scheduler.sc.conf
  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)  //表示 Spark RPC 消息的最大大小
  private val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)        //默认的 RPC 请求超时
  // Submit tasks only after (registered resources / total expected resources)
  // is equal to at least this value, that is double between 0 and 1.
  private val _minRegisteredRatio =
    math.min(1, conf.get(SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO).getOrElse(0.0))  //调度任务之前，必须满足的执行器注册比例。这个值介于 0 和 1 之间
  // Submit tasks after maxRegisteredWaitingTime milliseconds
  // if minRegisteredRatio has not yet been reached
  private val maxRegisteredWaitingTimeNs = TimeUnit.MILLISECONDS.toNanos(
    conf.get(SCHEDULER_MAX_REGISTERED_RESOURCE_WAITING_TIME))  //等待已注册执行器达到 _minRegisteredRatio 所需的最大时间
  private val createTimeNs = System.nanoTime()

  // Accessing `executorDataMap` in the inherited methods from ThreadSafeRpcEndpoint doesn't need
  // any protection. But accessing `executorDataMap` out of the inherited methods must be
  // protected by `CoarseGrainedSchedulerBackend.this`. Besides, `executorDataMap` should only
  // be modified in the inherited methods from ThreadSafeRpcEndpoint with protection by
  // `CoarseGrainedSchedulerBackend.this`.
  private val executorDataMap = new HashMap[String, ExecutorData] //用来存储执行器的相关信息，key 是执行器 ID，value 是 ExecutorData 对象

  // Number of executors for each ResourceProfile requested by the cluster
  // manager, [[ExecutorAllocationManager]]
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private val requestedTotalExecutorsPerResourceProfile = new HashMap[ResourceProfile, Int] //存储每个资源配置文件（ResourceProfile）请求的总执行器数

  // Profile IDs to the times that executors were requested for.
  // The operations we do on queue are all amortized constant cost
  // see https://www.scala-lang.org/api/2.13.x/scala/collection/mutable/ArrayDeque.html
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private val execRequestTimes = new HashMap[Int, Queue[(Int, Long)]]  //存储执行器请求的时间戳信息，按资源配置文件 ID 分类

  private val listenerBus = scheduler.sc.listenerBus

  // Executors we have requested the cluster manager to kill that have not died yet; maps
  // the executor ID to whether it was explicitly killed by the driver (and thus shouldn't
  // be considered an app-related failure). Visible for testing only.
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  private[scheduler] val executorsPendingToRemove = new HashMap[String, Boolean]  //存储待移除执行器的 ID 和是否由驱动显式请求删除的标记

  // Executors that have been lost, but for which we don't yet know the real exit reason.
  protected val executorsPendingLossReason = new HashSet[String] //存储失去连接的执行器 ID，等待确认其退出原因

  // Executors which are being decommissioned. Maps from executorId to ExecutorDecommissionInfo.
  protected val executorsPendingDecommission = new HashMap[String, ExecutorDecommissionInfo]  //存储退役的执行器信息

  // Unknown Executors which are being decommissioned. This could be caused by unregistered executor
  // This executor should be decommissioned after registration.
  // Maps from executorId to (ExecutorDecommissionInfo, adjustTargetNumExecutors,
  // triggeredByExecutor).
  protected val unknownExecutorsPendingDecommission =
    CacheBuilder.newBuilder()
      .maximumSize(conf.get(SCHEDULER_MAX_RETAINED_UNKNOWN_EXECUTORS))
      .build[String, (ExecutorDecommissionInfo, Boolean, Boolean)]()  //存储未知执行器的退役信息

  // A map of ResourceProfile id to map of hostname with its possible task number running on it
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  protected var rpHostToLocalTaskCount: Map[Int, Map[String, Int]] = Map.empty   //按资源配置文件 ID 和主机名分类，存储每个主机上等待执行的任务数

  // The number of pending tasks per ResourceProfile id which is locality required
  @GuardedBy("CoarseGrainedSchedulerBackend.this")
  protected var numLocalityAwareTasksPerResourceProfileId = Map.empty[Int, Int]  //按资源配置文件 ID 分类，存储每个资源配置文件 ID 的局部任务数量

  // The num of current max ExecutorId used to re-register appMaster
  @volatile protected var currentExecutorIdCounter = 0 //当前执行器的 ID 计数器，确保执行器 ID 的唯一性

  // Current set of delegation tokens to send to executors.
  private val delegationTokens = new AtomicReference[Array[Byte]]()  //存储当前的 Hadoop 委托令牌，用于安全通信

  // The token manager used to create security tokens.
  private var delegationTokenManager: Option[HadoopDelegationTokenManager] = None  //管理 Hadoop 委托令牌的可选对象

  private val reviveThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-revive-thread")  //用于定期恢复调度 offer，从而实现延迟调度

  private val cleanupService: Option[ScheduledExecutorService] =
    conf.get(EXECUTOR_DECOMMISSION_FORCE_KILL_TIMEOUT).map { _ =>
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("cleanup-decommission-execs")  //如果启用执行器降级强制杀死超时，则使用定时任务线程清理这些执行器
    }
  //处理 Spark 驱动程序与执行器之间的通信
  class DriverEndpoint extends IsolatedThreadSafeRpcEndpoint with Logging {

    override val rpcEnv: RpcEnv = CoarseGrainedSchedulerBackend.this.rpcEnv  //驱动程序的 RPC 环境引用

    protected val addressToExecutorId = new HashMap[RpcAddress, String]  //将执行器的 RPC 地址映射到其执行器 ID

    // Spark configuration sent to executors. This is a lazy val so that subclasses of the
    // scheduler can modify the SparkConf object before this view is created.
    // 用来存储Spark配置的惰性加载，过滤出以" spark." 开头的配置项
    private lazy val sparkProperties = scheduler.sc.conf.getAll
      .filter { case (k, _) => k.startsWith("spark.") }
      .toSeq
    // 执行器日志的URL处理器
    private val logUrlHandler: ExecutorLogUrlHandler = new ExecutorLogUrlHandler(
      conf.get(UI.CUSTOM_EXECUTOR_LOG_URL))

    override def onStart(): Unit = {
      // Periodically revive offers to allow delay scheduling to work
      val reviveIntervalMs = conf.get(SCHEDULER_REVIVE_INTERVAL).getOrElse(1000L)

      reviveThread.scheduleAtFixedRate(() => Utils.tryLogNonFatalError {
        //// 定期触发资源复活（revive offers）以支持延迟调度
        Option(self).foreach(_.send(ReviveOffers))
      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)
    }
    // 处理接收到的消息
    override def receive: PartialFunction[Any, Unit] = {
      // 处理任务状态更新，更新调度器的状态
      case StatusUpdate(executorId, taskId, state, data, taskCpus, resources) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              // 如果执行器状态更新，释放对应的资源
              executorInfo.freeCores += taskCpus
              resources.foreach { case (k, v) =>
                executorInfo.resourcesInfo.get(k).foreach { r =>
                  r.release(v.addresses)  // 释放资源
                }
              }
              makeOffers(executorId)  // 重新生成资源报价
            case None =>
              // Ignoring the update since we don't know about the executor.
              logWarning(s"Ignored task status update ($taskId state $state) " +
                s"from unknown executor with ID $executorId")
          }
        }
      // 处理Shuffle操作完成
      case ShufflePushCompletion(shuffleId, shuffleMergeId, mapIndex) =>
        scheduler.dagScheduler.shufflePushCompleted(shuffleId, shuffleMergeId, mapIndex)
        // 处理ReviveOffers消息，生成资源报价
      case ReviveOffers =>
        makeOffers()
      // 杀死指定任务
      case KillTask(taskId, executorId, interruptThread, reason) =>
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            // 向执行器发送KillTask消息
            executorInfo.executorEndpoint.send(
              KillTask(taskId, executorId, interruptThread, reason))
          // 如果执行器不存在，忽略该请求
          case None =>
            // Ignoring the task kill since the executor is not registered.
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }
      // 终止指定主机上的所有执行器
      case KillExecutorsOnHost(host) =>
        scheduler.getExecutorsAliveOnHost(host).foreach { execs =>
          killExecutors(execs.toSeq, adjustTargetNumExecutors = false, countFailures = false,
            force = true)
        }
      // 将指定主机上的所有执行器标记为退役
      case DecommissionExecutorsOnHost(host) =>
        val reason = ExecutorDecommissionInfo(s"Decommissioning all executors on $host.")
        scheduler.getExecutorsAliveOnHost(host).foreach { execs =>
          val execsWithReasons = execs.map(exec => (exec, reason)).toArray

          decommissionExecutors(execsWithReasons, adjustTargetNumExecutors = false,
            triggeredByExecutor = false)
        }
      // 更新代理令牌
      case UpdateDelegationTokens(newDelegationTokens) =>
        updateDelegationTokens(newDelegationTokens)
      // 移除执行器并通知它停止自己
      case RemoveExecutor(executorId, reason) =>
        // We will remove the executor's state and cannot restore it. However, the connection
        // between the driver and the executor may be still alive so that the executor won't exit
        // automatically, so try to tell the executor to stop itself. See SPARK-13519.
        executorDataMap.get(executorId).foreach(_.executorEndpoint.send(StopExecutor))
        removeExecutor(executorId, reason)
      // 移除worker节点
      case RemoveWorker(workerId, host, message) =>
        removeWorker(workerId, host, message)
      // 启动新的执行器
      case LaunchedExecutor(executorId) =>
        executorDataMap.get(executorId).foreach { data =>
          data.freeCores = data.totalCores
        }
        makeOffers(executorId)
      // 处理额外进程的添加
      case MiscellaneousProcessAdded(time: Long,
          processId: String, info: MiscellaneousProcessDetails) =>
        listenerBus.post(SparkListenerMiscellaneousProcessAdded(time, processId, info))

      case e =>
        logError(s"Received unexpected message. ${e}")
    }
    // 处理带有回复的RPC消息
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      // 处理执行器注册请求
      case RegisterExecutor(executorId, executorRef, hostname, cores, logUrls,
          attributes, resources, resourceProfileId) =>
        // 如果执行器已经注册，返回错误
        if (executorDataMap.contains(executorId)) {
          context.sendFailure(new IllegalStateException(s"Duplicate executor ID: $executorId"))
        } else if (scheduler.excludedNodes.contains(hostname) ||
            isExecutorExcluded(executorId, hostname)) {
          // If the cluster manager gives us an executor on an excluded node (because it
          // already started allocating those resources before we informed it of our exclusion,
          // or if it ignored our exclusion), then we reject that executor immediately.
          // 如果执行器所在的主机被排除，拒绝注册
          logInfo(s"Rejecting $executorId as it has been excluded.")
          context.sendFailure(
            new IllegalStateException(s"Executor is excluded due to failures: $executorId"))
        } else {
          // If the executor's rpc env is not listening for incoming connections, `hostPort`
          // will be null, and the client connection should be used to contact the executor.
          // 处理正常的注册流程
          val executorAddress = if (executorRef.address != null) {
              executorRef.address
            } else {
              context.senderAddress  // 如果RPC地址为空，使用上下文中的发送者地址
            }
          logInfo(s"Registered executor $executorRef ($executorAddress) with ID $executorId, " +
            s" ResourceProfileId $resourceProfileId")
          addressToExecutorId(executorAddress) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          val resourcesInfo = resources.map { case (rName, info) =>
            // tell the executor it can schedule resources up to numSlotsPerAddress times,
            // as configured by the user, or set to 1 as that is the default (1 task/resource)
            // 为执行器创建资源信息
            val numParts = scheduler.sc.resourceProfileManager
              .resourceProfileFromId(resourceProfileId).getNumSlotsPerAddress(rName, conf)
            (info.name, new ExecutorResourceInfo(info.name, info.addresses, numParts))
          }
          // If we've requested the executor figure out when we did.
          val reqTs: Option[Long] = CoarseGrainedSchedulerBackend.this.synchronized {
            execRequestTimes.get(resourceProfileId).flatMap {
              times =>
              times.headOption.map {
                h =>
                // Take off the top element
                times.dequeue()
                // If we requested more than one exec reduce the req count by 1 and prepend it back
                if (h._1 > 1) {
                  ((h._1 - 1, h._2)) +=: times
                }
                h._2
              }
            }
          }
          // 创建ExecutorData对象，存储执行器的状态信息
          val data = new ExecutorData(executorRef, executorAddress, hostname,
            0, cores, logUrlHandler.applyPattern(logUrls, attributes), attributes,
            resourcesInfo, resourceProfileId, registrationTs = System.currentTimeMillis(),
            requestTs = reqTs)
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          CoarseGrainedSchedulerBackend.this.synchronized {
            executorDataMap.put(executorId, data)
            if (currentExecutorIdCounter < executorId.toInt) {
              currentExecutorIdCounter = executorId.toInt
            }
          }
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
          // Note: some tests expect the reply to come after we put the executor in the map
          // Decommission executor whose request received before registration
          Option(unknownExecutorsPendingDecommission.getIfPresent(executorId))
            .foreach(v => {
              decommissionExecutors(Array((executorId, v._1)), v._2, v._3)
              unknownExecutorsPendingDecommission.invalidate(executorId)
            })
          context.reply(true)
        }
      // 处理停止驱动程序的请求
      case StopDriver =>
        context.reply(true)
        stop()
      // 处理停止所有执行器的请求
      case StopExecutors =>
        logInfo("Asking each executor to shut down")
        for ((_, executorData) <- executorDataMap) {
          executorData.executorEndpoint.send(StopExecutor)
        }
        context.reply(true)
      // 处理移除worker节点
      case RemoveWorker(workerId, host, message) =>
        removeWorker(workerId, host, message)
        context.reply(true)

      // Do not change this code without running the K8s integration suites
      // 处理执行器退役
      case ExecutorDecommissioning(executorId) =>
        logWarning(s"Received executor $executorId decommissioned message")
        context.reply(
          decommissionExecutor(
            executorId,
            ExecutorDecommissionInfo(s"Executor $executorId is decommissioned."),
            adjustTargetNumExecutors = false,
            triggeredByExecutor = true))

      case RetrieveSparkAppConfig(resourceProfileId) =>
        val rp = scheduler.sc.resourceProfileManager.resourceProfileFromId(resourceProfileId)
        val reply = SparkAppConfig(
          sparkProperties,
          SparkEnv.get.securityManager.getIOEncryptionKey(),
          Option(delegationTokens.get()),
          rp)
        context.reply(reply)

      case IsExecutorAlive(executorId) => context.reply(isExecutorActive(executorId))

      case e =>
        logError(s"Received unexpected ask ${e}")
    }

    // Make fake resource offers on all executors
    //该方法的作用是模拟向所有活跃执行器发出资源申请（offers），并根据这些资源创建并启动任务
    private def makeOffers(): Unit = {
      // Make sure no executor is killed while some task is launching on it
      val taskDescs = withLock {
        // Filter out executors under killing
        val activeExecutors = executorDataMap.filterKeys(isExecutorActive)  //过滤出所有活跃的执行器
        val workOffers = activeExecutors.map {
          //资源申请表示执行器上的可用资源以及它能够提供的资源池
          case (id, executorData) => buildWorkerOffer(id, executorData)
        }.toIndexedSeq
        scheduler.resourceOffers(workOffers, true)  //调用TaskScheduler根据workOffers资源分配任务
      }
      if (taskDescs.nonEmpty) {
        launchTasks(taskDescs)
      }
    }
    //构建一个 WorkerOffer 对象，表示给定的 executor 提供的资源
    private def buildWorkerOffer(executorId: String, executorData: ExecutorData) = {
      val resources = executorData.resourcesInfo.map { case (rName, rInfo) =>
        (rName, rInfo.availableAddrs.toBuffer)
      }
      WorkerOffer(
        executorId,
        executorData.executorHost,
        executorData.freeCores,
        Some(executorData.executorAddress.hostPort),
        resources,
        executorData.resourceProfileId)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId
        .get(remoteAddress)
        .foreach(removeExecutor(_,
          ExecutorProcessLost("Remote RPC client disassociated. Likely due to " +
            "containers exceeding thresholds, or network issues. Check driver logs for WARN " +
            "messages.")))
    }

    // Make fake resource offers on just one executor
    private def makeOffers(executorId: String): Unit = {
      // Make sure no executor is killed while some task is launching on it
      val taskDescs = withLock {
        // Filter out executors under killing
        if (isExecutorActive(executorId)) {
          val executorData = executorDataMap(executorId)
          val workOffers = IndexedSeq(buildWorkerOffer(executorId, executorData))
          scheduler.resourceOffers(workOffers, false)
        } else {
          Seq.empty
        }
      }
      if (taskDescs.nonEmpty) {
        launchTasks(taskDescs)
      }
    }

    // Launch tasks returned by a set of resource offers
    // 根据资源提供的任务描述序列来启动任务
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]): Unit = {
      //tasks 是一个包含任务描述的二维序列。外层序列表示一个资源提供的任务集合，内层序列表示具体的任务描述（TaskDescription）
      for (task <- tasks.flatten) {
        val serializedTask = TaskDescription.encode(task)
        if (serializedTask.limit() >= maxRpcMessageSize) {
          Option(scheduler.taskIdToTaskSetManager.get(task.taskId)).foreach { taskSetMgr =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                s"${RPC_MESSAGE_MAX_SIZE.key} (%d bytes). Consider increasing " +
                s"${RPC_MESSAGE_MAX_SIZE.key} or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit(), maxRpcMessageSize)
              taskSetMgr.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else {
          val executorData = executorDataMap(task.executorId)
          // Do resources allocation here. The allocated resources will get released after the task
          // finishes.
          //表示这些核心已经分配给当前任务
          executorData.freeCores -= task.cpus
          task.resources.foreach { case (rName, rInfo) =>
            assert(executorData.resourcesInfo.contains(rName))
            executorData.resourcesInfo(rName).acquire(rInfo.addresses)
          }

          logDebug(s"Launching task ${task.taskId} on executor id: ${task.executorId} hostname: " +
            s"${executorData.executorHost}.")
          //表示与 executor 的通信端点。通过它发送 LaunchTask 消息，告诉 executor 启动任务
          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    // Remove a disconnected executor from the cluster
    private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
      logDebug(s"Asked to remove executor $executorId with reason $reason")
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          val lossReason = CoarseGrainedSchedulerBackend.this.synchronized {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
            executorsPendingLossReason -= executorId
            val killedByDriver = executorsPendingToRemove.remove(executorId).getOrElse(false)
            val decommissionInfoOpt = executorsPendingDecommission.remove(executorId)
            if (killedByDriver) {
              ExecutorKilled
            } else if (decommissionInfoOpt.isDefined) {
              val decommissionInfo = decommissionInfoOpt.get
              ExecutorDecommission(decommissionInfo.workerHost, decommissionInfo.message)
            } else {
              reason
            }
          }
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          totalRegisteredExecutors.addAndGet(-1)
          scheduler.executorLost(executorId, lossReason)
          listenerBus.post(SparkListenerExecutorRemoved(
            System.currentTimeMillis(), executorId, lossReason.toString))
        case None =>
          // SPARK-15262: If an executor is still alive even after the scheduler has removed
          // its metadata, we may receive a heartbeat from that executor and tell its block
          // manager to reregister itself. If that happens, the block manager master will know
          // about the executor, but the scheduler will not. Therefore, we should remove the
          // executor from the block manager when we hit this case.
          scheduler.sc.env.blockManager.master.removeExecutorAsync(executorId)
          // SPARK-35011: If we reach this code path, which means the executor has been
          // already removed from the scheduler backend but the block manager master may
          // still know it. In this case, removing the executor from block manager master
          // would only post the event `SparkListenerBlockManagerRemoved`, which is unfortunately
          // ignored by `AppStatusListener`. As a result, the executor would be shown on the UI
          // forever. Therefore, we should also post `SparkListenerExecutorRemoved` here.
          listenerBus.post(SparkListenerExecutorRemoved(
            System.currentTimeMillis(), executorId, reason.toString))
          logInfo(s"Asked to remove non-existent executor $executorId")
      }
    }

    // Remove a lost worker from the cluster
    private def removeWorker(workerId: String, host: String, message: String): Unit = {
      logDebug(s"Asked to remove worker $workerId with reason $message")
      scheduler.workerRemoved(workerId, host, message)
    }

    /**
     * Stop making resource offers for the given executor. The executor is marked as lost with
     * the loss reason still pending.
     *
     * @return Whether executor should be disabled
     */
    protected def disableExecutor(executorId: String): Boolean = {
      val shouldDisable = CoarseGrainedSchedulerBackend.this.synchronized {
        if (isExecutorActive(executorId)) {
          executorsPendingLossReason += executorId
          true
        } else {
          // Returns true for explicitly killed executors, we also need to get pending loss reasons;
          // For others return false.
          executorsPendingToRemove.contains(executorId)
        }
      }

      if (shouldDisable) {
        logInfo(s"Disabling executor $executorId.")
        scheduler.executorLost(executorId, LossReasonPending)
      }

      shouldDisable
    }
  }
   //驱动器的endpoint
  val driverEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint())

  protected def minRegisteredRatio: Double = _minRegisteredRatio

  /**
   * Request that the cluster manager decommission the specified executors.
   *
   * @param executorsAndDecomInfo Identifiers of executors & decommission info.
   * @param adjustTargetNumExecutors whether the target number of executors will be adjusted down
   *                                 after these executors have been decommissioned.
   * @param triggeredByExecutor whether the decommission is triggered at executor.
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  override def decommissionExecutors(
      executorsAndDecomInfo: Array[(String, ExecutorDecommissionInfo)],
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean): Seq[String] = withLock {
    // Do not change this code without running the K8s integration suites
    val executorsToDecommission = executorsAndDecomInfo.flatMap { case (executorId, decomInfo) =>
      // Only bother decommissioning executors which are alive.
      // Keep executor decommission info in case executor started, but not registered yet
      if (isExecutorActive(executorId)) {
        scheduler.executorDecommission(executorId, decomInfo)
        executorsPendingDecommission(executorId) = decomInfo
        Some(executorId)
      } else {
        unknownExecutorsPendingDecommission.put(executorId,
          (decomInfo, adjustTargetNumExecutors, triggeredByExecutor))
        None
      }
    }

    if (executorsToDecommission.isEmpty) {
      return executorsToDecommission
    }

    logInfo(s"Decommission executors: ${executorsToDecommission.mkString(", ")}")

    // If we don't want to replace the executors we are decommissioning
    if (adjustTargetNumExecutors) {
      adjustExecutors(executorsToDecommission)
    }

    // Mark those corresponding BlockManagers as decommissioned first before we sending
    // decommission notification to executors. So, it's less likely to lead to the race
    // condition where `getPeer` request from the decommissioned executor comes first
    // before the BlockManagers are marked as decommissioned.
    // Note that marking BlockManager as decommissioned doesn't need depend on
    // `spark.storage.decommission.enabled`. Because it's meaningless to save more blocks
    // for the BlockManager since the executor will be shutdown soon.
    scheduler.sc.env.blockManager.master.decommissionBlockManagers(executorsToDecommission)

    if (!triggeredByExecutor) {
      executorsToDecommission.foreach { executorId =>
        logInfo(s"Notify executor $executorId to decommission.")
        executorDataMap(executorId).executorEndpoint.send(DecommissionExecutor)
      }
    }

    conf.get(EXECUTOR_DECOMMISSION_FORCE_KILL_TIMEOUT).map { cleanupInterval =>
      val cleanupTask = new Runnable() {
        override def run(): Unit = Utils.tryLogNonFatalError {
          val stragglers = CoarseGrainedSchedulerBackend.this.synchronized {
            executorsToDecommission.filter(executorsPendingDecommission.contains)
          }
          if (stragglers.nonEmpty) {
            logInfo(s"${stragglers.toList} failed to decommission in ${cleanupInterval}, killing.")
            killExecutors(stragglers, false, false, true)
          }
        }
      }
      cleanupService.map(_.schedule(cleanupTask, cleanupInterval, TimeUnit.SECONDS))
    }

    executorsToDecommission
  }

  override def start(): Unit = {
    if (UserGroupInformation.isSecurityEnabled()) {
      delegationTokenManager = createTokenManager()
      delegationTokenManager.foreach { dtm =>
        val ugi = UserGroupInformation.getCurrentUser()
        val tokens = if (dtm.renewalEnabled) {
          dtm.start()
        } else {
          val creds = ugi.getCredentials()
          dtm.obtainDelegationTokens(creds)
          if (creds.numberOfTokens() > 0 || creds.numberOfSecretKeys() > 0) {
            SparkHadoopUtil.get.serialize(creds)
          } else {
            null
          }
        }
        if (tokens != null) {
          updateDelegationTokens(tokens)
        }
      }
    }
  }
  //创建驱动器的endpoint
  protected def createDriverEndpoint(): DriverEndpoint = new DriverEndpoint()

  def stopExecutors(): Unit = {
    try {
      if (driverEndpoint != null) {
        logInfo("Shutting down all executors")
        driverEndpoint.askSync[Boolean](StopExecutors)
      }
    } catch {
      case e: Exception =>
        throw SparkCoreErrors.askStandaloneSchedulerToShutDownExecutorsError(e)
    }
  }

  override def stop(): Unit = {
    reviveThread.shutdownNow()
    cleanupService.foreach(_.shutdownNow())
    stopExecutors()
    delegationTokenManager.foreach(_.stop())
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askSync[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw SparkCoreErrors.stopStandaloneSchedulerDriverEndpointError(e)
    }
  }

  /**
   * Reset the state of CoarseGrainedSchedulerBackend to the initial state. Currently it will only
   * be called in the yarn-client mode when AM re-registers after a failure.
   * Visible for testing only.
   * */
  protected[scheduler] def reset(): Unit = {
    val executors: Set[String] = synchronized {
      requestedTotalExecutorsPerResourceProfile.clear()
      executorDataMap.keys.toSet
    }

    // Remove all the lingering executors that should be removed but not yet. The reason might be
    // because (1) disconnected event is not yet received; (2) executors die silently.
    executors.foreach { eid =>
      removeExecutor(eid,
        ExecutorProcessLost("Stale executor after cluster manager re-registered."))
    }
  }

  override def reviveOffers(): Unit = Utils.tryLogNonFatalError {
    driverEndpoint.send(ReviveOffers)
  }

  override def killTask(
      taskId: Long, executorId: String, interruptThread: Boolean, reason: String): Unit = {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread, reason))
  }

  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  /**
   * Called by subclasses when notified of a lost worker. It just fires the message and returns
   * at once.
   */
  protected def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    driverEndpoint.send(RemoveExecutor(executorId, reason))
  }

  protected def removeWorker(workerId: String, host: String, message: String): Unit = {
    driverEndpoint.send(RemoveWorker(workerId, host, message))
  }

  def sufficientResourcesRegistered(): Boolean = true

  override def isReady(): Boolean = {
    if (sufficientResourcesRegistered) {
      logInfo("SchedulerBackend is ready for scheduling beginning after " +
        s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
      return true
    }
    if ((System.nanoTime() - createTimeNs) >= maxRegisteredWaitingTimeNs) {
      logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
        s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTimeNs(ns)")
      return true
    }
    false
  }

  /**
   * Return the number of executors currently registered with this backend.
   */
  private def numExistingExecutors: Int = synchronized { executorDataMap.size }

  override def getExecutorIds(): Seq[String] = synchronized {
    executorDataMap.keySet.toSeq
  }

  def getExecutorsWithRegistrationTs(): Map[String, Long] = synchronized {
    executorDataMap.mapValues(v => v.registrationTs).toMap
  }
  //判断执行器是否还有效
  override def isExecutorActive(id: String): Boolean = synchronized {
    executorDataMap.contains(id) &&
      !executorsPendingToRemove.contains(id) &&
      !executorsPendingLossReason.contains(id) &&
      !executorsPendingDecommission.contains(id)
  }

  /**
   * Get the max number of tasks that can be concurrent launched based on the ResourceProfile
   * could be used, even if some of them are being used at the moment.
   * Note that please don't cache the value returned by this method, because the number can change
   * due to add/remove executors.
   *
   * @param rp ResourceProfile which to use to calculate max concurrent tasks.
   * @return The max number of tasks that can be concurrent launched currently.
   */
  override def maxNumConcurrentTasks(rp: ResourceProfile): Int = synchronized {
    val (rpIds, cpus, resources) = {
      executorDataMap
        .filter { case (id, _) => isExecutorActive(id) }
        .values.toArray.map { executor =>
          (
            executor.resourceProfileId,
            executor.totalCores,
            executor.resourcesInfo.map { case (name, rInfo) => (name, rInfo.totalAddressAmount) }
          )
        }.unzip3
    }
    TaskSchedulerImpl.calculateAvailableSlots(scheduler, conf, rp.id, rpIds, cpus, resources)
  }

  // this function is for testing only
  def getExecutorAvailableResources(
      executorId: String): Map[String, ExecutorResourceInfo] = synchronized {
    executorDataMap.get(executorId).map(_.resourcesInfo).getOrElse(Map.empty)
  }

  // this function is for testing only
  private[spark] def getExecutorAvailableCpus(
      executorId: String): Option[Int] = synchronized {
    executorDataMap.get(executorId).map(_.freeCores)
  }

  // this function is for testing only
  def getExecutorResourceProfileId(executorId: String): Int = synchronized {
    val execDataOption = executorDataMap.get(executorId)
    execDataOption.map(_.resourceProfileId).getOrElse(ResourceProfile.UNKNOWN_RESOURCE_PROFILE_ID)
  }

  /**
   * Request an additional number of executors from the cluster manager. This is
   * requesting against the default ResourceProfile, we will need an API change to
   * allow against other profiles.
   * @return whether the request is acknowledged.
   */
  final override def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    if (numAdditionalExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of additional executor(s) " +
        s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
    }
    logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")

    val response = synchronized {
      val defaultProf = scheduler.sc.resourceProfileManager.defaultResourceProfile
      val numExisting = requestedTotalExecutorsPerResourceProfile.getOrElse(defaultProf, 0)
      requestedTotalExecutorsPerResourceProfile(defaultProf) = numExisting + numAdditionalExecutors
      // Account for executors pending to be added or removed
      updateExecRequestTime(defaultProf.id, numAdditionalExecutors)
      doRequestTotalExecutors(requestedTotalExecutorsPerResourceProfile.toMap)
    }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * @param resourceProfileIdToNumExecutors The total number of executors we'd like to have per
   *                                      ResourceProfile. The cluster manager shouldn't kill any
   *                                      running executor to reach this number, but, if all
   *                                      existing executors were to die, this is the number
   *                                      of executors we'd want to be allocated.
   * @param numLocalityAwareTasksPerResourceProfileId The number of tasks in all active stages that
   *                                                  have a locality preferences per
   *                                                  ResourceProfile. This includes running,
   *                                                  pending, and completed tasks.
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  final override def requestTotalExecutors(
      resourceProfileIdToNumExecutors: Map[Int, Int],
      numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
      hostToLocalTaskCount: Map[Int, Map[String, Int]]
  ): Boolean = {
    val totalExecs = resourceProfileIdToNumExecutors.values.sum
    if (totalExecs < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of executor(s) " +
          s"$totalExecs from the cluster manager. Please specify a positive number!")
    }
    val resourceProfileToNumExecutors = resourceProfileIdToNumExecutors.map { case (rpid, num) =>
      (scheduler.sc.resourceProfileManager.resourceProfileFromId(rpid), num)
    }
    val response = synchronized {
      val oldResourceProfileToNumExecutors = requestedTotalExecutorsPerResourceProfile.map {
        case (rp, num) =>
          (rp.id, num)
      }.toMap
      this.requestedTotalExecutorsPerResourceProfile.clear()
      this.requestedTotalExecutorsPerResourceProfile ++= resourceProfileToNumExecutors
      this.numLocalityAwareTasksPerResourceProfileId = numLocalityAwareTasksPerResourceProfileId
      this.rpHostToLocalTaskCount = hostToLocalTaskCount
      updateExecRequestTimes(oldResourceProfileToNumExecutors, resourceProfileIdToNumExecutors)
      doRequestTotalExecutors(requestedTotalExecutorsPerResourceProfile.toMap)
    }
    defaultAskTimeout.awaitResult(response)
  }

  private def updateExecRequestTimes(oldProfile: Map[Int, Int], newProfile: Map[Int, Int]): Unit = {
    newProfile.map {
      case (k, v) =>
        val delta = v - oldProfile.getOrElse(k, 0)
        if (delta != 0) {
          updateExecRequestTime(k, delta)
        }
    }
  }

  private def updateExecRequestTime(profileId: Int, delta: Int) = {
    val times = execRequestTimes.getOrElseUpdate(profileId, Queue[(Int, Long)]())
    if (delta > 0) {
      // Add the request to the end, constant time op
      times += ((delta, System.currentTimeMillis()))
    } else if (delta < 0) {
      // Consume as if |delta| had been allocated
      var toConsume = -delta
      // Note: it's possible that something else allocated an executor and we have
      // a negative delta, we can just avoid mutating the queue.
      while (toConsume > 0 && times.nonEmpty) {
        val h = times.dequeue
        if (h._1 > toConsume) {
          // Prepend updated first req to times, constant time op
          ((h._1 - toConsume, h._2)) +=: times
          toConsume = 0
        } else {
          toConsume = toConsume - h._1
        }
      }
    }
  }

  /**
   * Request executors from the cluster manager by specifying the total number desired,
   * including existing pending and running executors.
   *
   * The semantics here guarantee that we do not over-allocate executors for this application,
   * since a later request overrides the value of any prior request. The alternative interface
   * of requesting a delta of executors risks double counting new executors when there are
   * insufficient resources to satisfy the first request. We make the assumption here that the
   * cluster manager will eventually fulfill all requests when resources free up.
   *
   * @return a future whose evaluation indicates whether the request is acknowledged.
   */
  protected def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] =
    Future.successful(false)

  /**
   * Adjust the number of executors being requested to no longer include the provided executors.
   */
  private def adjustExecutors(executorIds: Seq[String]) = {
    if (executorIds.nonEmpty) {
      executorIds.foreach { exec =>
        withLock {
          val rpId = executorDataMap(exec).resourceProfileId
          val rp = scheduler.sc.resourceProfileManager.resourceProfileFromId(rpId)
          if (requestedTotalExecutorsPerResourceProfile.isEmpty) {
            // Assume that we are killing an executor that was started by default and
            // not through the request api
            requestedTotalExecutorsPerResourceProfile(rp) = 0
          } else {
            val requestedTotalForRp = requestedTotalExecutorsPerResourceProfile(rp)
            requestedTotalExecutorsPerResourceProfile(rp) = math.max(requestedTotalForRp - 1, 0)
          }
        }
      }
      doRequestTotalExecutors(requestedTotalExecutorsPerResourceProfile.toMap)
    } else {
      Future.successful(true)
    }
  }

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * @param executorIds identifiers of executors to kill
   * @param adjustTargetNumExecutors whether the target number of executors be adjusted down
   *                                 after these executors have been killed
   * @param countFailures if there are tasks running on the executors when they are killed, whether
   *                      those failures be counted to task failure limits?
   * @param force whether to force kill busy executors, default false
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  final override def killExecutors(
      executorIds: Seq[String],
      adjustTargetNumExecutors: Boolean,
      countFailures: Boolean,
      force: Boolean): Seq[String] = {
    logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")

    val response = withLock {
      val (knownExecutors, unknownExecutors) = executorIds.partition(executorDataMap.contains)
      unknownExecutors.foreach { id =>
        logWarning(s"Executor to kill $id does not exist!")
      }

      // If an executor is already pending to be removed, do not kill it again (SPARK-9795)
      // If this executor is busy, do not kill it unless we are told to force kill it (SPARK-9552)
      val executorsToKill = knownExecutors
        .filter { id => !executorsPendingToRemove.contains(id) }
        .filter { id => force || !scheduler.isExecutorBusy(id) }
      executorsToKill.foreach { id => executorsPendingToRemove(id) = !countFailures }

      logInfo(s"Actual list of executor(s) to be killed is ${executorsToKill.mkString(", ")}")

      // If we do not wish to replace the executors we kill, sync the target number of executors
      // with the cluster manager to avoid allocating new ones. When computing the new target,
      // take into account executors that are pending to be added or removed.
      val adjustTotalExecutors =
        if (adjustTargetNumExecutors) {
          adjustExecutors(executorsToKill)
        } else {
          Future.successful(true)
        }

      val killExecutors: Boolean => Future[Boolean] =
        if (executorsToKill.nonEmpty) {
          _ => doKillExecutors(executorsToKill)
        } else {
          _ => Future.successful(false)
        }

      val killResponse = adjustTotalExecutors.flatMap(killExecutors)(ThreadUtils.sameThread)

      killResponse.flatMap(killSuccessful =>
        Future.successful (if (killSuccessful) executorsToKill else Seq.empty[String])
      )(ThreadUtils.sameThread)
    }

    defaultAskTimeout.awaitResult(response)
  }

  /**
   * Kill the given list of executors through the cluster manager.
   * @return whether the kill request is acknowledged.
   */
  protected def doKillExecutors(executorIds: Seq[String]): Future[Boolean] =
    Future.successful(false)

  /**
   * Request that the cluster manager decommissions all executors on a given host.
   * @return whether the decommission request is acknowledged.
   */
  final override def decommissionExecutorsOnHost(host: String): Boolean = {
    logInfo(s"Requesting to kill any and all executors on host $host")
    // A potential race exists if a new executor attempts to register on a host
    // that is on the exclude list and is no longer valid. To avoid this race,
    // all executor registration and decommissioning happens in the event loop. This way, either
    // an executor will fail to register, or will be decommed when all executors on a host
    // are decommed.
    // Decommission all the executors on this host in an event loop to ensure serialization.
    driverEndpoint.send(DecommissionExecutorsOnHost(host))
    true
  }

  /**
   * Request that the cluster manager kill all executors on a given host.
   * @return whether the kill request is acknowledged.
   */
  final override def killExecutorsOnHost(host: String): Boolean = {
    logInfo(s"Requesting to kill any and all executors on host $host")
    // A potential race exists if a new executor attempts to register on a host
    // that is on the exclude list and is no longer valid. To avoid this race,
    // all executor registration and killing happens in the event loop. This way, either
    // an executor will fail to register, or will be killed when all executors on a host
    // are killed.
    // Kill all the executors on this host in an event loop to ensure serialization.
    driverEndpoint.send(KillExecutorsOnHost(host))
    true
  }

  /**
   * Create the delegation token manager to be used for the application. This method is called
   * once during the start of the scheduler backend (so after the object has already been
   * fully constructed), only if security is enabled in the Hadoop configuration.
   */
  protected def createTokenManager(): Option[HadoopDelegationTokenManager] = None

  /**
   * Called when a new set of delegation tokens is sent to the driver. Child classes can override
   * this method but should always call this implementation, which handles token distribution to
   * executors.
   */
  protected def updateDelegationTokens(tokens: Array[Byte]): Unit = {
    SparkHadoopUtil.get.addDelegationTokens(tokens, conf)
    delegationTokens.set(tokens)
    executorDataMap.values.foreach { ed =>
      ed.executorEndpoint.send(UpdateDelegationTokens(tokens))
    }
  }

  protected def currentDelegationTokens: Array[Byte] = delegationTokens.get()

  /**
   * Checks whether the executor is excluded due to failure(s). This is called when the executor
   * tries to register with the scheduler, and will deny registration if this method returns true.
   *
   * This is in addition to the exclude list kept by the task scheduler, so custom implementations
   * don't need to check there.
   */
  protected def isExecutorExcluded(executorId: String, hostname: String): Boolean = false

  // SPARK-27112: We need to ensure that there is ordering of lock acquisition
  // between TaskSchedulerImpl and CoarseGrainedSchedulerBackend objects in order to fix
  // the deadlock issue exposed in SPARK-27112
  private def withLock[T](fn: => T): T = scheduler.synchronized {
    CoarseGrainedSchedulerBackend.this.synchronized { fn }
  }

}

private[spark] object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}
