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

package org.apache.spark

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.Tests.TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceProfile.UNKNOWN_RESOURCE_PROFILE_ID
import org.apache.spark.resource.ResourceProfileManager
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.dynalloc.ExecutorMonitor
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 * 这是一个代理（agent），它根据工作负载动态地分配和移除执行器（Executors）
 * The ExecutorAllocationManager maintains a moving target number of executors, for each
 * ResourceProfile, which is periodically synced to the cluster manager. The target starts
 * at a configured initial value and changes with the number of pending and running tasks.
 * ExecutorAllocationManager 为每个资源配置文件（ResourceProfile）维护一个浮动目标执行器数量，并周期性地将此目标同步给集群管理器。
 * 该目标值从配置的初始值开始，并随着**待处理（Pending）和正在运行（Running）**任务的数量而变化。
 * Decreasing the target number of executors happens when the current target is more than needed to
 * handle the current load. The target number of executors is always truncated to the number of
 * executors that could run all current running and pending tasks at once.
 * 当当前目标数量超过处理当前负载所需时，会减少执行器的目标数量。目标执行器数量总是被截断为能够同时运行所有当前正在运行和待处理任务所需的执行器数量。
 * Increasing the target number of executors happens in response to backlogged tasks waiting to be
 * scheduled. If the scheduler queue is not drained in M seconds, then new executors are added. If
 * the queue persists for another N seconds, then more executors are added and so on. The number
 * added in each round increases exponentially from the previous round until an upper bound has been
 * reached. The upper bound is based both on a configured property and on the current number of
 * running and pending tasks, as described above.
 * 增加目标执行器数量是为了响应**积压（backlogged）**的、等待调度的任务。
 * 如果调度器队列在 M 秒内没有被清空，则会添加新的执行器。
 * 如果队列又持续了 N 秒，则会添加更多的执行器，以此类推。
 * 每一轮添加的执行器数量会从前一轮指数级增加，直到达到上限。该上限是基于配置属性和当前正在运行和待处理任务数量来确定的，如上所述。
 * The rationale for the exponential increase is twofold: (1) Executors should be added slowly
 * in the beginning in case the number of extra executors needed turns out to be small. Otherwise,
 * we may add more executors than we need just to remove them later. (2) Executors should be added
 * quickly over time in case the maximum number of executors is very high. Otherwise, it will take
 * a long time to ramp up under heavy workloads.
 * 指数级增加的原理是双重的：
 * 初期慢速： 避免在开始阶段添加过多执行器，以防所需的额外执行器数量很小。否则，我们可能会添加比实际需要更多的执行器，之后又不得不移除。
 *后期快速： 随着时间推移快速添加执行器，以防最大执行器数量很高。否则，在重负载下，执行器数量的增加会花费很长时间。
 * The remove policy is simpler and is applied on each ResourceProfile separately. If an executor
 * for that ResourceProfile has been idle for K seconds and the number of executors is more
 * then what is needed for that ResourceProfile, meaning there are not enough tasks that could use
 * the executor, then it is removed. Note that an executor caching any data
 * blocks will be removed if it has been idle for more than L seconds.
 * 移除策略较为简单，并针对每个 ResourceProfile 分别应用：
 * 如果属于该 ResourceProfile 的一个执行器已经空闲了 K 秒，并且执行器总数超过了该 ResourceProfile 所需的数量（即没有足够的任务来使用该执行器），则该执行器被移除。
 * 需要注意的是，缓存了任何数据块的执行器，如果在空闲时间超过 L 秒后，才会被移除。
 * There is no retry logic in either case because we make the assumption that the cluster manager
 * will eventually fulfill all requests it receives asynchronously.
 * 在任何情况下都没有重试逻辑，因为我们假设集群管理器最终会异步地满足它收到的所有请求。
 * The relevant Spark properties are below. Each of these properties applies separately to
 * every ResourceProfile. So if you set a minimum number of executors, that is a minimum
 * for each ResourceProfile.
 *  以下是相关的 Spark 配置属性。请注意，每个属性都分别应用于每个 ResourceProfile。因此，如果您设置了最小执行器数量，那是每个 ResourceProfile 的最小数量。
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *   spark.dynamicAllocation.initialExecutors - Number of executors to start with
 *
 *   spark.dynamicAllocation.executorAllocationRatio -
 *     This is used to reduce the parallelism of the dynamic allocation that can waste
 *     resources when tasks are small
 *
 *   spark.dynamicAllocation.schedulerBacklogTimeout (M) -
 *     If there are backlogged tasks for this duration, add new executors
 *
 *   spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N) -
 *     If the backlog is sustained for this duration, add more executors
 *     This is used only after the initial backlog timeout is exceeded
 *
 *   spark.dynamicAllocation.executorIdleTimeout (K) -
 *     If an executor without caching any data blocks has been idle for this duration, remove it
 *
 *   spark.dynamicAllocation.cachedExecutorIdleTimeout (L) -
 *     If an executor with caching data blocks has been idle for more than this duration,
 *     the executor will be removed
 *
 */
// Spark 动态资源分配（Dynamic Allocation） 机制的核心组件。
// 它的主要作用是根据当前应用程序的负载（待处理任务和运行中的任务）动态地请求和释放执行器（Executors），以优化资源利用率和吞吐量
// 放大（Scale Up）： 当任务积压（backlogged tasks）超过一定阈值时，它会按指数级增加地请求新的执行器。
// 缩小（Scale Down）： 当执行器空闲时间超过配置的超时时间时，它会标记并请求集群管理器杀死这些执行器
// 多资源配置支持： 它支持多达 512 种不同的 ResourceProfile，能够为不同需求的任务（例如，需要 GPU 的任务）动态地分配和管理具有相应资源的执行器
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient, //用于与底层的集群管理器（如 YARN、Kubernetes）通信，执行实际的请求（增加）和杀死（移除）执行器的操作。
    listenerBus: LiveListenerBus, // Spark 的事件总线
    conf: SparkConf,
    cleaner: Option[ContextCleaner] = None,
    clock: Clock = new SystemClock(), // 用于获取当前时间，以便计算执行器空闲超时和任务积压超时，便于测试
    resourceProfileManager: ResourceProfileManager, // 用于获取和管理应用程序中定义的各种 ResourceProfile
    reliableShuffleStorage: Boolean) // 指示是否使用了可靠的 Shuffle 存储（如外部 Shuffle Service 或特定配置的 Shuffle 插件），这影响到执行器移除的安全性（避免数据丢失）
  extends Logging {

  allocationManager =>

  import ExecutorAllocationManager._

  // Lower and upper bounds on the number of executors.
  // 应用程序必须始终保持的最小执行器数量（配置项：spark.dynamicAllocation.minExecutors）
  private val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
  // 应用程序可以拥有的最大执行器数量（配置项：spark.dynamicAllocation.maxExecutors）
  private val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
  // 应用程序启动时立即请求的执行器数量（配置项：spark.dynamicAllocation.initialExecutors）
  private val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)

  // How long there must be backlogged tasks for before an addition is triggered (seconds)
  // 调度队列中出现积压任务后，第一次触发增加执行器请求所需的持续时间（秒）
  private val schedulerBacklogTimeoutS = conf.get(DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT)

  // Same as above, but used only after `schedulerBacklogTimeoutS` is exceeded
  // 第一次触发增加执行器后，后续请求增加执行器所需的持续时间（秒）
  private val sustainedSchedulerBacklogTimeoutS =
    conf.get(DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT)

  // During testing, the methods to actually kill and add executors are mocked out
  private val testing = conf.get(DYN_ALLOCATION_TESTING)
  // 用于计算所需的执行器数量时，在任务数基础上乘以的一个比率（配置项：spark.dynamicAllocation.executorAllocationRatio），用于提供缓冲
  private val executorAllocationRatio =
    conf.get(DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO)

  private val decommissionEnabled = conf.get(DECOMMISSION_ENABLED)
  // 默认 ResourceProfile 的 ID
  private val defaultProfileId = resourceProfileManager.defaultResourceProfile.id

  validateSettings()

  // Number of executors to add for each ResourceProfile in the next round
  // 存储每个 ResourceProfile ID 在下一次调度循环中计划额外增加的执行器数量。采用指数退避策略（初始为 1，每次翻倍）
  private[spark] val numExecutorsToAddPerResourceProfileId = new mutable.HashMap[Int, Int]
  numExecutorsToAddPerResourceProfileId(defaultProfileId) = 1

  // The desired number of executors at this moment in time. If all our executors were to die, this
  // is the number of executors we would immediately want from the cluster manager.
  // Note every profile will be allowed to have initial number,
  // we may want to make this configurable per Profile in the future
  // 存储每个 ResourceProfile ID 当前期望的执行器总数（包括已运行和待分配的）
  private[spark] val numExecutorsTargetPerResourceProfileId = new mutable.HashMap[Int, Int]
  numExecutorsTargetPerResourceProfileId(defaultProfileId) = initialNumExecutors

  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  // 一个时间戳（纳秒），指示何时可以再次检查并可能增加执行器。如果任务队列为空，则为 NOT_SET
  private var addTime: Long = NOT_SET

  // Polling loop interval (ms)
  private val intervalMillis: Long = 100

  // Listener for Spark events that impact the allocation policy
  val listener = new ExecutorAllocationListener

  // Executor that handles the scheduling task.
  // 一个单线程的调度执行器，用于定期运行 schedule() 方法
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-dynamic-executor-allocation")

  // Metric source for ExecutorAllocationManager to expose internal status to MetricsSystem.
  val executorAllocationManagerSource = new ExecutorAllocationManagerSource(this)

  //负责追踪和监控所有执行器的状态，特别是其空闲时间，以决定何时移除它们
  val executorMonitor =
    new ExecutorMonitor(conf, client, listenerBus, clock, executorAllocationManagerSource)

  // Whether we are still waiting for the initial set of executors to be allocated.
  // While this is true, we will not cancel outstanding executor requests. This is
  // set to false when:
  //   (1) a stage is submitted, or
  //   (2) an executor idle timeout has elapsed.
  // 表示是否仍在等待初始执行器集合被分配。在此阶段不会取消待处理的执行器请求
  @volatile private var initializing: Boolean = true

  // Number of locality aware tasks for each ResourceProfile, used for executor placement.
  // 存储每个 ResourceProfile ID 到主机名的映射，以及该主机上具有本地性偏好的待调度任务数量。用于帮助集群管理器进行最优的执行器放置
  private var numLocalityAwareTasksPerResourceProfileId = new mutable.HashMap[Int, Int]
  numLocalityAwareTasksPerResourceProfileId(defaultProfileId) = 0

  // ResourceProfile id to Host to possible task running on it, used for executor placement.
  private var rpIdToHostToLocalTaskCount: Map[Int, Map[String, Int]] = Map.empty

  /**
   * Verify that the settings specified through the config are valid.
   * If not, throw an appropriate exception.
   */
  // 检查配置参数（如 min/max 执行器数、超时时间、Shuffle 服务启用状态）的有效性。如果发现不合法配置，则抛出 SparkException
  private def validateSettings(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException(
        s"${DYN_ALLOCATION_MIN_EXECUTORS.key} and ${DYN_ALLOCATION_MAX_EXECUTORS.key} must be " +
          "positive!")
    }
    if (maxNumExecutors == 0) {
      throw new SparkException(s"${DYN_ALLOCATION_MAX_EXECUTORS.key} cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"${DYN_ALLOCATION_MIN_EXECUTORS.key} ($minNumExecutors) must " +
        s"be less than or equal to ${DYN_ALLOCATION_MAX_EXECUTORS.key} ($maxNumExecutors)!")
    }
    if (schedulerBacklogTimeoutS <= 0) {
      throw new SparkException(s"${DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT.key} must be > 0!")
    }
    if (sustainedSchedulerBacklogTimeoutS <= 0) {
      throw new SparkException(
        s"s${DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT.key} must be > 0!")
    }
    if (!conf.get(config.SHUFFLE_SERVICE_ENABLED) && !reliableShuffleStorage) {
      if (conf.get(config.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED)) {
        logInfo("Dynamic allocation is enabled without a shuffle service.")
      } else if (decommissionEnabled &&
          conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
        logInfo("Shuffle data decommission is enabled without a shuffle service.")
      } else if (!testing) {
        throw new SparkException("Dynamic allocation of executors requires one of the " +
          "following conditions: 1) enabling external shuffle service through " +
          s"${config.SHUFFLE_SERVICE_ENABLED.key}. 2) enabling shuffle tracking through " +
          s"${DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED.key}. 3) enabling shuffle blocks " +
          s"decommission through ${DECOMMISSION_ENABLED.key} and " +
          s"${STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key}. 4) (Experimental) " +
          s"configuring ${SHUFFLE_IO_PLUGIN_CLASS.key} to use a custom ShuffleDataIO who's " +
          "ShuffleDriverComponents supports reliable storage.")
      }
    }

    if (executorAllocationRatio > 1.0 || executorAllocationRatio <= 0.0) {
      throw new SparkException(
        s"${DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO.key} must be > 0 and <= 1.0")
    }
  }

  /**
   * Register for scheduler callbacks to decide when to add and remove executors, and start
   * the scheduling task.
   */
  // 启动动态分配管理器。
  // 它将 listener 和 executorMonitor 注册到事件总线，启动定时调度任务（运行 schedule()），并向集群管理器发送初始执行器请求
  def start(): Unit = {
    listenerBus.addToManagementQueue(listener)
    listenerBus.addToManagementQueue(executorMonitor)
    cleaner.foreach(_.attachListener(executorMonitor))

    val scheduleTask = new Runnable() {
      override def run(): Unit = Utils.tryLog(schedule())
    }

    if (!testing || conf.get(TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED)) {
      executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)
    }

    // copy the maps inside synchronize to ensure not being modified
    val (numExecutorsTarget, numLocalityAware) = synchronized {
      val numTarget = numExecutorsTargetPerResourceProfileId.toMap
      val numLocality = numLocalityAwareTasksPerResourceProfileId.toMap
      (numTarget, numLocality)
    }

    client.requestTotalExecutors(numExecutorsTarget, numLocalityAware, rpIdToHostToLocalTaskCount)
  }

  /**
   * Stop the allocation manager.
   */
  def stop(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  /**
   * Reset the allocation manager when the cluster manager loses track of the driver's state.
   * This is currently only done in YARN client mode, when the AM is restarted.
   *
   * This method forgets about any state about existing executors, and forces the scheduler to
   * re-evaluate the number of needed executors the next time it's run.
   */
  def reset(): Unit = synchronized {
    addTime = 0L
    numExecutorsTargetPerResourceProfileId.keys.foreach { rpId =>
      numExecutorsTargetPerResourceProfileId(rpId) = initialNumExecutors
    }
    numExecutorsToAddPerResourceProfileId.keys.foreach { rpId =>
      numExecutorsToAddPerResourceProfileId(rpId) = 1
    }
    executorMonitor.reset()
  }

  /**
   * The maximum number of executors, for the ResourceProfile id passed in, that we would need
   * under the current load to satisfy all running and pending tasks, rounded up.
   */
  // 用于计算满足特定资源配置（ResourceProfile）下当前所有待处理和运行中的任务所需的最大执行器数量
  // 它接受一个 ResourceProfile ID (rpId) 作为参数，并返回一个整数 (Int)，表示该配置下所需的总执行器数。
  private[spark] def maxNumExecutorsNeededPerResourceProfile(rpId: Int): Int = {
    // 获取当前使用 rpId 的非推测执行（non-speculative）待处理任务的数量
    val pendingTask = listener.pendingTasksPerResourceProfile(rpId)
    // 待处理推测执行（speculative）任务的数量
    val pendingSpeculative = listener.pendingSpeculativeTasksPerResourceProfile(rpId)
    // 获取由于执行器/节点黑名单（通常是由于任务失败次数过多）而无法调度的 Task Set 的数量
    val unschedulableTaskSets = listener.pendingUnschedulableTaskSetsPerResourceProfile(rpId)
    // 获取当前使用 rpId 的正在运行的任务数量
    val running = listener.totalRunningTasksPerResourceProfile(rpId)
    // 计算总任务负载，即所有正在运行或等待调度的任务总数（包括投机性任务）
    val numRunningOrPendingTasks = pendingTask + pendingSpeculative + running
    val rp = resourceProfileManager.resourceProfileFromId(rpId)
    //计算此 ResourceProfile 配置下的一个执行器最多可以并发运行的任务数量
    val tasksPerExecutor = rp.maxTasksPerExecutor(conf)
    logDebug(s"max needed for rpId: $rpId numpending: $numRunningOrPendingTasks," +
      s" tasksperexecutor: $tasksPerExecutor")
    // 计算基础需求执行器数量。
    val maxNeeded = math.ceil(numRunningOrPendingTasks * executorAllocationRatio /
      tasksPerExecutor).toInt
    // 为了满足推测执行任务的本地性要求而可能需要增加额外执行器的情况
    val maxNeededWithSpeculationLocalityOffset =
      if (tasksPerExecutor > 1 && maxNeeded == 1 && pendingSpeculative > 0) {
      // If we have pending speculative tasks and only need a single executor, allocate one more
      // to satisfy the locality requirements of speculation
      maxNeeded + 1
    } else {
      maxNeeded
    }
    //处理由于执行器黑名单等问题导致任务集不可调度时，需要额外请求执行器的情况
    if (unschedulableTaskSets > 0) {
      // Request additional executors to account for task sets having tasks that are unschedulable
      // due to executors excluded for failures when the active executor count has already reached
      // the max needed which we would normally get.
      val maxNeededForUnschedulables = math.ceil(unschedulableTaskSets * executorAllocationRatio /
        tasksPerExecutor).toInt
      math.max(maxNeededWithSpeculationLocalityOffset,
        executorMonitor.executorCountWithResourceProfile(rpId) + maxNeededForUnschedulables)
    } else {
      maxNeededWithSpeculationLocalityOffset
    }
  }
  // 每个ResourceProfile当前运行的总任务数
  private def totalRunningTasksPerResourceProfile(id: Int): Int = synchronized {
    listener.totalRunningTasksPerResourceProfile(id)
  }

  /**
   * This is called at a fixed interval to regulate the number of pending executor requests
   * and number of executors running.
   *
   * First, adjust our requested executors based on the add time and our current needs.
   * Then, if the remove time for an existing executor has expired, kill the executor.
   *
   * This is factored out into its own method for testing.
   */
  private def schedule(): Unit = synchronized {
    // 获取所有空闲超时的执行器 ID
    val executorIdsToBeRemoved = executorMonitor.timedOutExecutors()
    if (executorIdsToBeRemoved.nonEmpty) {
      initializing = false
    }

    // Update executor target number only after initializing flag is unset
    // 根据任务积压情况调整目标执行器数量
    updateAndSyncNumExecutorsTarget(clock.nanoTime())
    if (executorIdsToBeRemoved.nonEmpty) {
      //移除超时执行器
      removeExecutors(executorIdsToBeRemoved)
    }
  }

  /**
   * Updates our target number of executors for each ResourceProfile and then syncs the result
   * with the cluster manager.
   *
   * Check to see whether our existing allocation and the requests we've made previously exceed our
   * current needs. If so, truncate our target and let the cluster manager know so that it can
   * cancel pending requests that are unneeded.
   *
   * If not, and the add time has expired, see if we can request new executors and refresh the add
   * time.
   *
   * @return the delta in the target number of executors.
   */
  // 动态资源分配策略中调整和同步目标执行器数量的核心
  // 接收当前时间（now，纳秒）作为参数，并返回目标执行器数量的变化量（Delta）
  private def updateAndSyncNumExecutorsTarget(now: Long): Int = synchronized {
    // 标志在应用程序刚启动，正在等待初始执行器分配时为 true
    if (initializing) {
      // Do not change our target while we are still initializing,
      // Otherwise the first job may have to ramp up unnecessarily
      0
    } else {
      // 用于存储本次调度循环中目标数量发生变化的 ResourceProfile 的更新信息。这些信息稍后会被用来通知集群管理器
      val updatesNeeded = new mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]

      // Update targets for all ResourceProfiles then do a single request to the cluster manager
      //遍历当前管理器追踪的所有 ResourceProfile ID 及其对应的当前目标执行器数量（targetExecs）
      numExecutorsTargetPerResourceProfileId.foreach { case (rpId, targetExecs) =>
        // 调用计算方法，得出该 rpId 下当前实际需要的最大执行器数量
        val maxNeeded = maxNumExecutorsNeededPerResourceProfile(rpId)
        //判断是否需要缩小目标
        if (maxNeeded < targetExecs) {
          // The target number exceeds the number we actually need, so stop adding new
          // executors and inform the cluster manager to cancel the extra pending requests

          // We lower the target number of executors but don't actively kill any yet.  Killing is
          // controlled separately by an idle timeout.  It's still helpful to reduce
          // the target number in case an executor just happens to get lost (e.g., bad hardware,
          // or the cluster manager preempts it) -- in that case, there is no point in trying
          // to immediately  get a new executor, since we wouldn't even use it yet.
          decrementExecutorsFromTarget(maxNeeded, rpId, updatesNeeded)
        } else if (addTime != NOT_SET && now >= addTime) {
          addExecutorsToTarget(maxNeeded, rpId, updatesNeeded)
        }
      }
      doUpdateRequest(updatesNeeded.toMap, now)
    }
  }

  private def addExecutorsToTarget(
      maxNeeded: Int,
      rpId: Int,
      updatesNeeded: mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]): Int = {
    updateTargetExecs(addExecutors, maxNeeded, rpId, updatesNeeded)
  }

  private def decrementExecutorsFromTarget(
      maxNeeded: Int,
      rpId: Int,
      updatesNeeded: mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]): Int = {
    updateTargetExecs(decrementExecutors, maxNeeded, rpId, updatesNeeded)
  }

  private def updateTargetExecs(
      updateTargetFn: (Int, Int) => Int,
      maxNeeded: Int,
      rpId: Int,
      updatesNeeded: mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]): Int = {
    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
    // update the target number (add or remove)
    val delta = updateTargetFn(maxNeeded, rpId)
    if (delta != 0) {
      updatesNeeded(rpId) = ExecutorAllocationManager.TargetNumUpdates(delta, oldNumExecutorsTarget)
    }
    delta
  }

  private def doUpdateRequest(
      updates: Map[Int, ExecutorAllocationManager.TargetNumUpdates],
      now: Long): Int = {
    // Only call cluster manager if target has changed.
    if (updates.size > 0) {
      val requestAcknowledged = try {
        logDebug("requesting updates: " + updates)
        testing ||
          client.requestTotalExecutors(
            numExecutorsTargetPerResourceProfileId.toMap,
            numLocalityAwareTasksPerResourceProfileId.toMap,
            rpIdToHostToLocalTaskCount)
      } catch {
        case NonFatal(e) =>
          // Use INFO level so the error it doesn't show up by default in shells.
          // Errors here are more commonly caused by YARN AM restarts, which is a recoverable
          // issue, and generate a lot of noisy output.
          logInfo("Error reaching cluster manager.", e)
          false
      }
      if (requestAcknowledged) {
        // have to go through all resource profiles that changed
        var totalDelta = 0
        updates.foreach { case (rpId, targetNum) =>
          val delta = targetNum.delta
          totalDelta += delta
          if (delta > 0) {
            val executorsString = "executor" + { if (delta > 1) "s" else "" }
            logInfo(s"Requesting $delta new $executorsString because tasks are backlogged " +
              s"(new desired total will be ${numExecutorsTargetPerResourceProfileId(rpId)} " +
              s"for resource profile id: ${rpId})")
            numExecutorsToAddPerResourceProfileId(rpId) =
              if (delta == numExecutorsToAddPerResourceProfileId(rpId)) {
                numExecutorsToAddPerResourceProfileId(rpId) * 2
              } else {
                1
              }
            logDebug(s"Starting timer to add more executors (to " +
              s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
            addTime = now + TimeUnit.SECONDS.toNanos(sustainedSchedulerBacklogTimeoutS)
          } else {
            logDebug(s"Lowering target number of executors to" +
              s" ${numExecutorsTargetPerResourceProfileId(rpId)} (previously " +
              s"${targetNum.oldNumExecutorsTarget} for resource profile id: ${rpId}) " +
              "because not all requested executors " +
              "are actually needed")
          }
        }
        totalDelta
      } else {
        // request was for all profiles so we have to go through all to reset to old num
        updates.foreach { case (rpId, targetNum) =>
          logWarning("Unable to reach the cluster manager to request more executors!")
          numExecutorsTargetPerResourceProfileId(rpId) = targetNum.oldNumExecutorsTarget
        }
        0
      }
    } else {
      logDebug("No change in number of executors")
      0
    }
  }

  private def decrementExecutors(maxNeeded: Int, rpId: Int): Int = {
    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
    numExecutorsTargetPerResourceProfileId(rpId) = math.max(maxNeeded, minNumExecutors)
    numExecutorsToAddPerResourceProfileId(rpId) = 1
    numExecutorsTargetPerResourceProfileId(rpId) - oldNumExecutorsTarget
  }

  /**
   * Update the target number of executors and figure out how many to add.
   * If the cap on the number of executors is reached, give up and reset the
   * number of executors to add next round instead of continuing to double it.
   *
   * @param maxNumExecutorsNeeded the maximum number of executors all currently running or pending
   *                              tasks could fill
   * @param rpId                  the ResourceProfile id of the executors
   * @return the number of additional executors actually requested.
   */
  private def addExecutors(maxNumExecutorsNeeded: Int, rpId: Int): Int = {
    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
    // Do not request more executors if it would put our target over the upper bound
    // this is doing a max check per ResourceProfile
    if (oldNumExecutorsTarget >= maxNumExecutors) {
      logDebug("Not adding executors because our current target total " +
        s"is already ${oldNumExecutorsTarget} (limit $maxNumExecutors)")
      numExecutorsToAddPerResourceProfileId(rpId) = 1
      return 0
    }
    // There's no point in wasting time ramping up to the number of executors we already have, so
    // make sure our target is at least as much as our current allocation:
    var numExecutorsTarget = math.max(numExecutorsTargetPerResourceProfileId(rpId),
        executorMonitor.executorCountWithResourceProfile(rpId))
    // Boost our target with the number to add for this round:
    numExecutorsTarget += numExecutorsToAddPerResourceProfileId(rpId)
    // Ensure that our target doesn't exceed what we need at the present moment:
    numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
    // Ensure that our target fits within configured bounds:
    numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors), minNumExecutors)
    val delta = numExecutorsTarget - oldNumExecutorsTarget
    numExecutorsTargetPerResourceProfileId(rpId) = numExecutorsTarget

    // If our target has not changed, do not send a message
    // to the cluster manager and reset our exponential growth
    if (delta == 0) {
      numExecutorsToAddPerResourceProfileId(rpId) = 1
    }
    delta
  }

  /**
   * Request the cluster manager to remove the given executors.
   * Returns the list of executors which are removed.
   */
  private def removeExecutors(executors: Seq[(String, Int)]): Seq[String] = synchronized {
    val executorIdsToBeRemoved = new ArrayBuffer[String]
    logDebug(s"Request to remove executorIds: ${executors.mkString(", ")}")
    val numExecutorsTotalPerRpId = mutable.Map[Int, Int]()
    executors.foreach { case (executorIdToBeRemoved, rpId) =>
      if (rpId == UNKNOWN_RESOURCE_PROFILE_ID) {
        if (testing) {
          throw new SparkException("ResourceProfile Id was UNKNOWN, this is not expected")
        }
        logWarning(s"Not removing executor $executorIdToBeRemoved because the " +
          "ResourceProfile was UNKNOWN!")
      } else {
        // get the running total as we remove or initialize it to the count - pendingRemoval
        val newExecutorTotal = numExecutorsTotalPerRpId.getOrElseUpdate(rpId,
          (executorMonitor.executorCountWithResourceProfile(rpId) -
            executorMonitor.pendingRemovalCountPerResourceProfileId(rpId) -
            executorMonitor.decommissioningPerResourceProfileId(rpId)
          ))
        if (newExecutorTotal - 1 < minNumExecutors) {
          logDebug(s"Not removing idle executor $executorIdToBeRemoved because there " +
            s"are only $newExecutorTotal executor(s) left (minimum number of executor limit " +
            s"$minNumExecutors)")
        } else if (newExecutorTotal - 1 < numExecutorsTargetPerResourceProfileId(rpId)) {
          logDebug(s"Not removing idle executor $executorIdToBeRemoved because there " +
            s"are only $newExecutorTotal executor(s) left (number of executor " +
            s"target ${numExecutorsTargetPerResourceProfileId(rpId)})")
        } else {
          executorIdsToBeRemoved += executorIdToBeRemoved
          numExecutorsTotalPerRpId(rpId) -= 1
        }
      }
    }

    if (executorIdsToBeRemoved.isEmpty) {
      return Seq.empty[String]
    }

    // Send a request to the backend to kill this executor(s)
    val executorsRemoved = if (testing) {
      executorIdsToBeRemoved
    } else {
      // We don't want to change our target number of executors, because we already did that
      // when the task backlog decreased.
      if (decommissionEnabled) {
        val executorIdsWithoutHostLoss = executorIdsToBeRemoved.map(
          id => (id, ExecutorDecommissionInfo("spark scale down"))).toArray
        client.decommissionExecutors(
          executorIdsWithoutHostLoss,
          adjustTargetNumExecutors = false,
          triggeredByExecutor = false)
      } else {
        client.killExecutors(executorIdsToBeRemoved.toSeq, adjustTargetNumExecutors = false,
          countFailures = false, force = false)
      }
    }

    // [SPARK-21834] killExecutors api reduces the target number of executors.
    // So we need to update the target with desired value.
    client.requestTotalExecutors(
      numExecutorsTargetPerResourceProfileId.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap,
      rpIdToHostToLocalTaskCount)

    // reset the newExecutorTotal to the existing number of executors
    if (testing || executorsRemoved.nonEmpty) {
      if (decommissionEnabled) {
        executorMonitor.executorsDecommissioned(executorsRemoved.toSeq)
      } else {
        executorMonitor.executorsKilled(executorsRemoved.toSeq)
      }
      logInfo(s"Executors ${executorsRemoved.mkString(",")} removed due to idle timeout.")
      executorsRemoved.toSeq
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor/s " +
        s"${executorIdsToBeRemoved.mkString(",")} or no executor eligible to kill!")
      Seq.empty[String]
    }
  }

  /**
   * Callback invoked when the scheduler receives new pending tasks.
   * This sets a time in the future that decides when executors should be added
   * if it is not already set.
   */
  // 记录任务调度的时间
  private def onSchedulerBacklogged(): Unit = synchronized {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeoutS seconds)")
      addTime = clock.nanoTime() + TimeUnit.SECONDS.toNanos(schedulerBacklogTimeoutS)
    }
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors.
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug("Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAddPerResourceProfileId.transform { case (_, _) => 1 }
  }

  private case class StageAttempt(stageId: Int, stageAttemptId: Int) {
    override def toString: String = s"Stage $stageId (Attempt $stageAttemptId)"
  }

  /**
   * A listener that notifies the given allocation manager of when to add and remove executors.
   *
   * This class is intentionally conservative in its assumptions about the relative ordering
   * and consistency of events returned by the listener.
   */
  // 核心作用是作为 Spark 事件系统和动态资源分配逻辑之间的桥梁。
  // 它实时监听所有与任务调度相关的事件（如 Stage 提交/完成、Task 开始/结束、投机性任务、不可调度任务集），
  // 并维护最新的任务负载状态和本地性信息
  // 1 通知资源管理器放大需求： 当任务开始积压时（例如，Stage 提交或 Task 失败），它会调用 ExecutorAllocationManager.onSchedulerBacklogged()，启动计时器来请求更多的执行器
  // 2 通知资源管理器缩小需求： 当调度队列清空时（所有任务都开始运行或完成），它会调用 ExecutorAllocationManager.onSchedulerQueueEmpty()，停止请求执行器的计时器
  // 提供数据： 它向 ExecutorAllocationManager 提供精确的“运行中”和“待处理”任务计数，以及按 ResourceProfile 划分的本地性偏好信息，供 maxNumExecutorsNeededPerResourceProfile 方法计算所需的执行器数量
  private[spark] class ExecutorAllocationListener extends SparkListener {
    // 该 Stage 尝试的总任务数。用于计算剩余多少任务待调度。
    private val stageAttemptToNumTasks = new mutable.HashMap[StageAttempt, Int]
    // Number of running tasks per stageAttempt including speculative tasks.
    // Should be 0 when no stages are active.
    // 当前正在运行的任务总数（包括投机性任务）
    private val stageAttemptToNumRunningTask = new mutable.HashMap[StageAttempt, Int]
    // 正在运行的非推测执行任务的索引集合。
    private val stageAttemptToTaskIndices = new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]
    // Map from each stageAttempt to a set of running speculative task indexes
    // TODO(SPARK-41192): We simply need an Int for this.
    // 正在运行的推测执行任务的索引集合
    private val stageAttemptToSpeculativeTaskIndices =
      new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]()
    // Map from each stageAttempt to a set of pending speculative task indexes
    // 处于等待状态的推测执行任务的索引集合
    private val stageAttemptToPendingSpeculativeTasks =
      new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]

    // ResourceProfile ID → 使用此配置的所有活跃 StageAttempt 集合。这是实现多资源配置动态分配的关键。
    private val resourceProfileIdToStageAttempt =
      new mutable.HashMap[Int, mutable.Set[StageAttempt]]

    // Keep track of unschedulable task sets because of executor/node exclusions from too many task
    // failures. This is a Set of StageAttempt's because we'll only take the last unschedulable task
    // in a taskset although there can be more. This is done in order to avoid costly loops in the
    // scheduling. Check TaskSetManager#getCompletelyExcludedTaskIfAny for more details.
    // 存储由于执行器或节点因任务失败次数过多而被排除（黑名单）而无法调度的 Task Set 对应的 StageAttempt 集合
    private val unschedulableTaskSets = new mutable.HashSet[StageAttempt]

    // stageAttempt to tuple (the number of task with locality preferences, a map where each pair
    // is a node and the number of tasks that would like to be scheduled on that node, and
    // the resource profile id) map,
    // maintain the executor placement hints for each stageAttempt used by resource framework
    // to better place the executors.
    // 一个三元组 (任务本地性数量, 主机本地性计数, RP ID)。用于计算全局本地性偏好，以指导集群管理器更好地放置新的执行器
    private val stageAttemptToExecutorPlacementHints =
      new mutable.HashMap[StageAttempt, (Int, Map[String, Int], Int)]
    // Stage 提交
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      initializing = false
      val stageId = stageSubmitted.stageInfo.stageId
      val stageAttemptId = stageSubmitted.stageInfo.attemptNumber()
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val numTasks = stageSubmitted.stageInfo.numTasks
      allocationManager.synchronized {
        // 记录 Stage 的总任务数 (stageAttemptToNumTasks)
        stageAttemptToNumTasks(stageAttempt) = numTasks
        // 启动资源请求计时
        allocationManager.onSchedulerBacklogged()
        // need to keep stage task requirements to ask for the right containers
        // 计算并更新 执行器放置提示
        val profId = stageSubmitted.stageInfo.resourceProfileId
        logDebug(s"Stage resource profile id is: $profId with numTasks: $numTasks")
        resourceProfileIdToStageAttempt.getOrElseUpdate(
          profId, new mutable.HashSet[StageAttempt]) += stageAttempt
        numExecutorsToAddPerResourceProfileId.getOrElseUpdate(profId, 1)

        // Compute the number of tasks requested by the stage on each host
        var numTasksPending = 0
        val hostToLocalTaskCountPerStage = new mutable.HashMap[String, Int]()
        stageSubmitted.stageInfo.taskLocalityPreferences.foreach { locality =>
          if (!locality.isEmpty) {
            numTasksPending += 1
            locality.foreach { location =>
              val count = hostToLocalTaskCountPerStage.getOrElse(location.host, 0) + 1
              hostToLocalTaskCountPerStage(location.host) = count
            }
          }
        }
        stageAttemptToExecutorPlacementHints.put(stageAttempt,
          (numTasksPending, hostToLocalTaskCountPerStage.toMap, profId))
        // Update the executor placement hints
        updateExecutorPlacementHints()

        // 如果是新的 ResourceProfile 且设置了初始执行器数，则立即请求这些执行器
        if (!numExecutorsTargetPerResourceProfileId.contains(profId)) {
          numExecutorsTargetPerResourceProfileId.put(profId, initialNumExecutors)
          if (initialNumExecutors > 0) {
            logDebug(s"requesting executors, rpId: $profId, initial number is $initialNumExecutors")
            // we need to trigger a schedule since we add an initial number here.
            client.requestTotalExecutors(
              numExecutorsTargetPerResourceProfileId.toMap,
              numLocalityAwareTasksPerResourceProfileId.toMap,
              rpIdToHostToLocalTaskCount)
          }
        }
      }
    }
    // Stage 完成
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      val stageAttemptId = stageCompleted.stageInfo.attemptNumber()
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      //从所有跟踪映射（任务总数、待处理投机性、索引等）中移除已完成的 StageAttempt 信息
      allocationManager.synchronized {
        // do NOT remove stageAttempt from stageAttemptToNumRunningTask
        // because the attempt may still have running tasks,
        // even after another attempt for the stage is submitted.
        stageAttemptToNumTasks -= stageAttempt
        stageAttemptToPendingSpeculativeTasks -= stageAttempt
        stageAttemptToTaskIndices -= stageAttempt
        stageAttemptToSpeculativeTaskIndices -= stageAttempt
        stageAttemptToExecutorPlacementHints -= stageAttempt

        removeStageFromResourceProfileIfUnused(stageAttempt)

        // Update the executor placement hints
        // 更新执行器放置提示
        updateExecutorPlacementHints()

        // If this is the last stage with pending tasks, mark the scheduler queue as empty
        // This is needed in case the stage is aborted for any reason
        //如果所有 Stage 都没有待处理任务
        if (stageAttemptToNumTasks.isEmpty
          && stageAttemptToPendingSpeculativeTasks.isEmpty
          && stageAttemptToSpeculativeTaskIndices.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
        }
      }
    }
    // 任务开始
    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val stageId = taskStart.stageId
      val stageAttemptId = taskStart.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val taskIndex = taskStart.taskInfo.index
      allocationManager.synchronized {
        // 增加对应 Stage 的运行中任务计数
        stageAttemptToNumRunningTask(stageAttempt) =
          stageAttemptToNumRunningTask.getOrElse(stageAttempt, 0) + 1
        // If this is the last pending task, mark the scheduler queue as empty
        //将任务索引移到运行中的任务索引集合
        if (taskStart.taskInfo.speculative) {
          stageAttemptToSpeculativeTaskIndices.getOrElseUpdate(stageAttempt,
            new mutable.HashSet[Int]) += taskIndex
          stageAttemptToPendingSpeculativeTasks
            .get(stageAttempt).foreach(_.remove(taskIndex))
        } else {
          stageAttemptToTaskIndices.getOrElseUpdate(stageAttempt,
            new mutable.HashSet[Int]) += taskIndex
        }
        // 如果所有任务都已开始运行
        if (!hasPendingTasks) {
          allocationManager.onSchedulerQueueEmpty()
        }
      }
    }
    // 任务结束
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val stageId = taskEnd.stageId
      val stageAttemptId = taskEnd.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val taskIndex = taskEnd.taskInfo.index
      allocationManager.synchronized {
        // 减少对应 Stage 的运行中任务计数
        if (stageAttemptToNumRunningTask.contains(stageAttempt)) {
          stageAttemptToNumRunningTask(stageAttempt) -= 1
          if (stageAttemptToNumRunningTask(stageAttempt) == 0) {
            stageAttemptToNumRunningTask -= stageAttempt
            removeStageFromResourceProfileIfUnused(stageAttempt)
          }
        }
        if (taskEnd.taskInfo.speculative) {
          stageAttemptToSpeculativeTaskIndices.get(stageAttempt).foreach {_.remove{taskIndex}}
        }

        taskEnd.reason match {
          case Success =>
            // Remove pending speculative task in case the normal task
            // is finished before starting the speculative task
            stageAttemptToPendingSpeculativeTasks.get(stageAttempt).foreach(_.remove(taskIndex))
          case _: TaskKilled =>
          case _ =>
            if (!hasPendingTasks) {
              // If the task failed (not intentionally killed), we expect it to be resubmitted
              // later. To ensure we have enough resources to run the resubmitted task, we need to
              // mark the scheduler as backlogged again if it's not already marked as such
              // (SPARK-8366)
              // 如果任务失败且不是被故意杀死，且当前没有待处理任务
              allocationManager.onSchedulerBacklogged()
            }
            if (!taskEnd.taskInfo.speculative) {
              // If a non-speculative task is intentionally killed, it means the speculative task
              // has succeeded, and no further task of this task index will be resubmitted. In this
              // case, the task index is completed and we shouldn't remove it from
              // stageAttemptToTaskIndices. Otherwise, we will have a pending non-speculative task
              // for the task index (SPARK-30511)
              //清理对应的任务索引
              stageAttemptToTaskIndices.get(stageAttempt).foreach {_.remove(taskIndex)}
            }
        }
      }
    }
    //推测执行任务提交
    override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted)
      : Unit = {
      val stageId = speculativeTask.stageId
      val stageAttemptId = speculativeTask.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val taskIndex = speculativeTask.taskIndex
      //将新提交的投机性任务索引添加到 stageAttemptToPendingSpeculativeTasks
      allocationManager.synchronized {
        stageAttemptToPendingSpeculativeTasks.getOrElseUpdate(stageAttempt,
          new mutable.HashSet[Int]).add(taskIndex)
        //因为调度队列再次出现积压
        allocationManager.onSchedulerBacklogged()
      }
    }
    // 不可调度任务集添加
    override def onUnschedulableTaskSetAdded(
        unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit = {
      val stageId = unschedulableTaskSetAdded.stageId
      val stageAttemptId = unschedulableTaskSetAdded.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        // 将 Stage 尝试添加到 unschedulableTaskSets 集合
        unschedulableTaskSets.add(stageAttempt)
        // 因为需要更多执行器来解除任务集的阻塞
        allocationManager.onSchedulerBacklogged()
      }
    }

    override def onUnschedulableTaskSetRemoved(
        unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit = {
      val stageId = unschedulableTaskSetRemoved.stageId
      val stageAttemptId = unschedulableTaskSetRemoved.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        // Clear unschedulableTaskSets since atleast one task becomes schedulable now
        unschedulableTaskSets.remove(stageAttempt)
        removeStageFromResourceProfileIfUnused(stageAttempt)
      }
    }

    def removeStageFromResourceProfileIfUnused(stageAttempt: StageAttempt): Unit = {
      if (!stageAttemptToNumRunningTask.contains(stageAttempt) &&
          !stageAttemptToNumTasks.contains(stageAttempt) &&
          !stageAttemptToPendingSpeculativeTasks.contains(stageAttempt) &&
          !stageAttemptToTaskIndices.contains(stageAttempt) &&
          !stageAttemptToSpeculativeTaskIndices.contains(stageAttempt)
      ) {
        val rpForStage = resourceProfileIdToStageAttempt.filter { case (k, v) =>
          v.contains(stageAttempt)
        }.keys
        if (rpForStage.size == 1) {
          // be careful about the removal from here due to late tasks, make sure stage is
          // really complete and no tasks left
          resourceProfileIdToStageAttempt(rpForStage.head) -= stageAttempt
        } else {
          logWarning(s"Should have exactly one resource profile for stage $stageAttempt," +
              s" but have $rpForStage")
        }
      }
    }

    /**
     * An estimate of the total number of pending tasks remaining for currently running stages. Does
     * not account for tasks which may have failed and been resubmitted.
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     */
    def pendingTasksPerResourceProfile(rpId: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rpId, Set.empty).toSeq
      attempts.map(attempt => getPendingTaskSum(attempt)).sum
    }

    def hasPendingRegularTasks: Boolean = {
      val attemptSets = resourceProfileIdToStageAttempt.values
      attemptSets.exists(attempts => attempts.exists(getPendingTaskSum(_) > 0))
    }

    private def getPendingTaskSum(attempt: StageAttempt): Int = {
      val numTotalTasks = stageAttemptToNumTasks.getOrElse(attempt, 0)
      val numRunning = stageAttemptToTaskIndices.get(attempt).map(_.size).getOrElse(0)
      numTotalTasks - numRunning
    }

    def pendingSpeculativeTasksPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty).toSeq
      attempts.map(attempt => getPendingSpeculativeTaskSum(attempt)).sum
    }

    def hasPendingSpeculativeTasks: Boolean = {
      val attemptSets = resourceProfileIdToStageAttempt.values
      attemptSets.exists { attempts =>
        attempts.exists(getPendingSpeculativeTaskSum(_) > 0)
      }
    }

    private def getPendingSpeculativeTaskSum(attempt: StageAttempt): Int = {
      stageAttemptToPendingSpeculativeTasks.get(attempt).map(_.size).getOrElse(0)
    }

    /**
     * Currently we only know when a task set has an unschedulable task, we don't know
     * the exact number and since the allocation manager isn't tied closely with the scheduler,
     * we use the number of tasks sets that are unschedulable as a heuristic to add more executors.
     */
    def pendingUnschedulableTaskSetsPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty).toSeq
      attempts.count(attempt => unschedulableTaskSets.contains(attempt))
    }

    def hasPendingTasks: Boolean = {
      hasPendingSpeculativeTasks || hasPendingRegularTasks
    }

    def totalRunningTasksPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty).toSeq
      // attempts is a Set, change to Seq so we keep all values
      attempts.map { attempt =>
        stageAttemptToNumRunningTask.getOrElse(attempt, 0)
      }.sum
    }

    /**
     * Update the Executor placement hints (the number of tasks with locality preferences,
     * a map where each pair is a node and the number of tasks that would like to be scheduled
     * on that node).
     *
     * These hints are updated when stages arrive and complete, so are not up-to-date at task
     * granularity within stages.
     */
    def updateExecutorPlacementHints(): Unit = {
      val localityAwareTasksPerResourceProfileId = new mutable.HashMap[Int, Int]

      // ResourceProfile id => map[host, count]
      val rplocalityToCount = new mutable.HashMap[Int, mutable.HashMap[String, Int]]()
      stageAttemptToExecutorPlacementHints.values.foreach {
        case (numTasksPending, localities, rpId) =>
          val rpNumPending =
            localityAwareTasksPerResourceProfileId.getOrElse(rpId, 0)
          localityAwareTasksPerResourceProfileId(rpId) = rpNumPending + numTasksPending
          localities.foreach { case (hostname, count) =>
            val rpBasedHostToCount =
              rplocalityToCount.getOrElseUpdate(rpId, new mutable.HashMap[String, Int])
            val newUpdated = rpBasedHostToCount.getOrElse(hostname, 0) + count
            rpBasedHostToCount(hostname) = newUpdated
          }
      }

      allocationManager.numLocalityAwareTasksPerResourceProfileId =
        localityAwareTasksPerResourceProfileId
      allocationManager.rpIdToHostToLocalTaskCount =
        rplocalityToCount.map { case (k, v) => (k, v.toMap)}.toMap
    }
  }
}

/**
 * Metric source for ExecutorAllocationManager to expose its internal executor allocation
 * status to MetricsSystem.
 * Note: These metrics heavily rely on the internal implementation of
 * ExecutorAllocationManager, metrics or value of metrics will be changed when internal
 * implementation is changed, so these metrics are not stable across Spark version.
 */
private[spark] class ExecutorAllocationManagerSource(
    executorAllocationManager: ExecutorAllocationManager) extends Source {
  val sourceName = "ExecutorAllocationManager"
  val metricRegistry = new MetricRegistry()

  private def registerGauge[T](name: String, value: => T, defaultValue: T): Unit = {
    metricRegistry.register(MetricRegistry.name("executors", name), new Gauge[T] {
      override def getValue: T = synchronized { Option(value).getOrElse(defaultValue) }
    })
  }

  private def getCounter(name: String): Counter = {
    metricRegistry.counter(MetricRegistry.name("executors", name))
  }

  val gracefullyDecommissioned: Counter = getCounter("numberExecutorsGracefullyDecommissioned")
  val decommissionUnfinished: Counter = getCounter("numberExecutorsDecommissionUnfinished")
  val driverKilled: Counter = getCounter("numberExecutorsKilledByDriver")
  val exitedUnexpectedly: Counter = getCounter("numberExecutorsExitedUnexpectedly")

  // The metrics are going to return the sum for all the different ResourceProfiles.
  registerGauge("numberExecutorsToAdd",
    executorAllocationManager.numExecutorsToAddPerResourceProfileId.values.sum, 0)
  registerGauge("numberExecutorsPendingToRemove",
    executorAllocationManager.executorMonitor.pendingRemovalCount, 0)
  registerGauge("numberAllExecutors",
    executorAllocationManager.executorMonitor.executorCount, 0)
  registerGauge("numberTargetExecutors",
    executorAllocationManager.numExecutorsTargetPerResourceProfileId.values.sum, 0)
  registerGauge("numberMaxNeededExecutors",
    executorAllocationManager.numExecutorsTargetPerResourceProfileId.keys
      .map(executorAllocationManager.maxNumExecutorsNeededPerResourceProfile(_)).sum, 0)
  registerGauge("numberDecommissioningExecutors",
    executorAllocationManager.executorMonitor.decommissioningCount, 0)
}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue

  // helper case class for requesting executors, here to be visible for testing
  private[spark] case class TargetNumUpdates(delta: Int, oldNumExecutorsTarget: Int)

}
