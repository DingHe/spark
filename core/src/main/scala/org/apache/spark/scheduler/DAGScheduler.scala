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

package org.apache.spark.scheduler

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, ScheduledFuture, TimeoutException, TimeUnit}
import java.util.concurrent.{Future => JFutrue}
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import com.google.common.util.concurrent.{Futures, SettableFuture}

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.internal.config.RDD_CACHE_VISIBILITY_TRACKING_ENABLED
import org.apache.spark.internal.config.Tests.TEST_NO_STAGE_RETRY
import org.apache.spark.network.shuffle.{BlockStoreClient, MergeFinalizerListener}
import org.apache.spark.network.shuffle.protocol.MergeStatuses
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.{RDD, RDDCheckpointData}
import org.apache.spark.resource.{ResourceProfile, TaskResourceProfile}
import org.apache.spark.resource.ResourceProfile.{DEFAULT_RESOURCE_PROFILE_ID, EXECUTOR_CORES_LOCAL_PROPERTY, PYSPARK_MEMORY_LOCAL_PROPERTY}
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
 * tasks that can run right away based on the data that's already on the cluster (e.g. map output
 * files from previous stages), though it may fail if this data becomes unavailable.
 *
 * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
 * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
 * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
 * set of map output files, and another to read those files after a barrier). In the end, every
 * stage will have only shuffle dependencies on other stages, and may compute multiple operations
 * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
 * various RDDs
 *
 * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * When looking through this code, there are several key concepts:
 *
 *  - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
 *    For example, when the user calls an action, like count(), a job will be submitted through
 *    submitJob. Each Job may require the execution of multiple stages to build intermediate data.
 *
 *  - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
 *    task computes the same function on partitions of the same RDD. Stages are separated at shuffle
 *    boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
 *    fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
 *    executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
 *    Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
 *
 *  - Tasks are individual units of work, each sent to one machine.
 *
 *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
 *    and likewise remembers which shuffle map stages have already produced output files to avoid
 *    redoing the map side of a shuffle.
 *
 *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
 *    on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
 *
 *  - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
 *    to prevent memory leaks in a long-running application.
 *
 * To recover from failures, the same stage might need to run multiple times, which are called
 * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
 * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
 * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
 * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
 * stage(s) that compute the missing tasks. As part of this process, we might also have to create
 * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
 * tasks from the old attempt of a stage could still be running, care must be taken to map any
 * events received in the correct Stage object.
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 *
 *  - All data structures should be cleared when the jobs involving them end to avoid indefinite
 *    accumulation of state in long-running programs.
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 */
//DAGScheduler 是高层调度器，负责基于阶段（stage）进行任务调度。
// 它通过计算一个包含多个阶段（stage）的DAG（有向无环图）来调度任务，并将任务以 TaskSet 的形式提交给底层的 TaskScheduler 来执行。
private[spark] class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,  //任务调度器，负责将任务调度到集群中的各个节点上执行，负责将由 DAGScheduler 生成的 TaskSet 提交给执行器来执行
    listenerBus: LiveListenerBus,//事件总线，用于发送和接收 Spark 事件，很多组件通过事件进行交互，这个事件总线是一个重要的通信机制
    mapOutputTracker: MapOutputTrackerMaster, //追踪 Map 任务的输出。它保存了 Shuffle 阶段的输出信息，并在任务失败时提供相关的 Map 输出文件。
    blockManagerMaster: BlockManagerMaster, //负责管理 Spark 中的数据块，它控制数据块的存储和访问。
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)
  //度量（metrics）系统的集成。它用于跟踪 DAGScheduler 的性能数据和事件，比如任务执行的时间、调度的延迟等
  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)
  //用于生成新的作业 ID
  private[scheduler] val nextJobId = new AtomicInteger(0)
  //返回当前已提交的作业总数
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  //生成新的阶段 ID。每个作业会被划分为多个阶段，nextStageId 会为每个新的阶段生成一个唯一的 ID
  private val nextStageId = new AtomicInteger(0)
  //将作业 ID 映射到其相关阶段的 ID 集合。每个作业可能由多个阶段（stage）组成，因此每个作业 ID 对应一个阶段 ID 的集合。
  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  //阶段 ID 映射到对应的 Stage 对象。Stage 对象表示 Spark 执行中的一个阶段，其中包含了该阶段的任务、依赖关系等信息
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  /**
   * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
   * that dependency. Only includes stages that are part of currently running job (when the job(s)
   * that require the shuffle stage complete, the mapping will be removed, and the only record of
   * the shuffle data will be in the MapOutputTracker).
   */
   //将 shuffle 依赖的 ID 映射到生成该数据的 ShuffleMapStage 对象。ShuffleMapStage 是一个表示写 Shuffle 输出数据的阶段。
  private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]
  //作业 ID 映射到相应的 ActiveJob 对象。ActiveJob 表示当前正在执行的作业
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  //存储那些需要运行，但由于其父阶段尚未完成而被阻塞的阶段（Stage）
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  //存储当前正在运行的阶段（Stage）。当一个阶段被提交并开始执行时，它会被添加到 runningStages 中
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  //存储因任务失败而需要重新提交的阶段（Stage）
  private[scheduler] val failedStages = new HashSet[Stage]
  //储当前正在执行的作业（ActiveJob）
  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   * If you need to access any RDD while synchronizing on the cache locations,
   * first synchronize on the RDD, and then synchronize on this map to avoid deadlocks. The RDD
   * could try to access the cache locations after synchronizing on the RDD.
   */
   //存储每个 RDD 分区的缓存位置
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  /**
   * Tracks the latest epoch of a fully processed error related to the given executor. (We use
   * the MapOutputTracker's epoch number, which is sent with every task.)
   *
   * When an executor fails, it can affect the results of many tasks, and we have to deal with
   * all of them consistently. We don't simply ignore all future results from that executor,
   * as the failures may have been transient; but we also don't want to "overreact" to follow-
   * on errors we receive. Furthermore, we might receive notification of a task success, after
   * we find out the executor has actually failed; we'll assume those successes are, in fact,
   * simply delayed notifications and the results have been lost, if the tasks started in the
   * same or an earlier epoch. In particular, we use this to control when we tell the
   * BlockManagerMaster that the BlockManager has been lost.
   */
   //踪每个执行器的最新错误纪元
  private val executorFailureEpoch = new HashMap[String, Long]

  /**
   * Tracks the latest epoch of a fully processed error where shuffle files have been lost from
   * the given executor.
   *
   * This is closely related to executorFailureEpoch. They only differ for the executor when
   * there is an external shuffle service serving shuffle files and we haven't been notified that
   * the entire worker has been lost. In that case, when an executor is lost, we do not update
   * the shuffleFileLostEpoch; we wait for a fetch failure. This way, if only the executor
   * fails, we do not unregister the shuffle data as it can still be served; but if there is
   * a failure in the shuffle service (resulting in fetch failure), we unregister the shuffle
   * data only once, even if we get many fetch failures.
   */
   //追踪执行器丢失的 Shuffle 文件的最新错误纪元
  private val shuffleFileLostEpoch = new HashMap[String, Long]
  //用于协调输出提交的过程，确保所有输出结果都能够正确提交，并避免不同节点间的冲突。
  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  //一个闭包序列化器，用于序列化闭包
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
    //该属性会禁止由于 FetchFailed 错误而导致的阶段重试，以便突出显示潜在的错误
  private val disallowStageRetryForTest = sc.getConf.get(TEST_NO_STAGE_RETRY)
  //如果启用，当多个资源配置冲突时，会合并资源配置
  private val shouldMergeResourceProfiles = sc.getConf.get(config.RESOURCE_PROFILE_MERGE_CONFLICTS)

  /**
   * Whether to unregister all the outputs on the host in condition that we receive a FetchFailure,
   * this is set default to false, which means, we only unregister the outputs related to the exact
   * executor(instead of the host) on a FetchFailure.
   */
    //在 FetchFailure 错误发生时，决定是否注销与主机相关的所有输出。
  // 默认情况下，只会注销与具体执行器相关的输出，而不涉及整个主机。如果启用，则会注销主机上的所有输出
  private[scheduler] val unRegisterOutputOnHostOnFetchFailure =
    sc.getConf.get(config.UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE)

  /**
   * Number of consecutive stage attempts allowed before a stage is aborted.
   */
    //允许阶段的最大连续尝试次数。在达到最大次数时，阶段将被中止。这有助于避免无限重试失败的任务
  private[scheduler] val maxConsecutiveStageAttempts =
    sc.getConf.getInt("spark.stage.maxConsecutiveAttempts",
      DAGScheduler.DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS)

  /**允许阶段的最大重试次数，包括跨不同执行器的重试。
   * Max stage attempts allowed before a stage is aborted.
   */
  private[scheduler] val maxStageAttempts: Int = {
    Math.max(maxConsecutiveStageAttempts, sc.getConf.get(config.STAGE_MAX_ATTEMPTS))
  }

  /** 如果启用，在执行器退役时，FetchFailed 错误不会计入阶段重试的次数。这样可以避免因执行器退役而导致的错误影响作业的重试机制。
   * Whether ignore stage fetch failure caused by executor decommission when
   * count spark.stage.maxConsecutiveAttempts
   */
  private[scheduler] val ignoreDecommissionFetchFailure =
    sc.getConf.get(config.STAGE_IGNORE_DECOMMISSION_FETCH_FAILURE)

  /** 用于记录每个屏障作业的最大并发任务检查失败次数。
   * Number of max concurrent tasks check failures for each barrier job.
   */
  private[scheduler] val barrierJobIdToNumTasksCheckFailures = new ConcurrentHashMap[Int, Int]

  /**检查失败后，等待下次检查的时间间隔。
   * Time in seconds to wait between a max concurrent tasks check failure and the next check.
   */
  private val timeIntervalNumTasksCheck = sc.getConf
    .get(config.BARRIER_MAX_CONCURRENT_TASKS_CHECK_INTERVAL)

  /** 在屏障作业中，最大允许的任务检查失败次数。
   * Max number of max concurrent tasks check failures allowed for a job before fail the job
   * submission.
   */
  private val maxFailureNumTasksCheck = sc.getConf
    .get(config.BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES)

   //用于调度处理 DAGScheduler 消息的任务。它是一个守护线程，处理与调度相关的定时任务。
  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")
  //事件处理循环，用于处理调度过程中发生的各种事件
  private[spark] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)
  //是否启用推式 Shuffle（Push-Based Shuffle）
  private val pushBasedShuffleEnabled = Utils.isPushBasedShuffleEnabled(sc.getConf, isDriver = true)
  //控制 BlockManagerMaster 与驱动程序之间的心跳超时时间
  private val blockManagerMasterDriverHeartbeatTimeout =
    sc.getConf.get(config.STORAGE_BLOCKMANAGER_MASTER_DRIVER_HEARTBEAT_TIMEOUT).millis
  //用于配置 Shuffle 合并结果时的超时时间
  private val shuffleMergeResultsTimeoutSec =
    sc.getConf.get(config.PUSH_BASED_SHUFFLE_MERGE_RESULTS_TIMEOUT)
  //在 Shuffle 合并完成后，等待 finalize 操作的超时时间
  private val shuffleMergeFinalizeWaitSec =
    sc.getConf.get(config.PUSH_BASED_SHUFFLE_MERGE_FINALIZE_TIMEOUT)
  //在 Shuffle 合并操作中，合并数据的最小大小阈值。小于该阈值的数据会跳过合并过程
  private val shuffleMergeWaitMinSizeThreshold =
    sc.getConf.get(config.PUSH_BASED_SHUFFLE_SIZE_MIN_SHUFFLE_SIZE_TO_WAIT)
  //启用 Push-Based Shuffle 时，指定最小的推送比例。如果推送的数据量低于该比例，则不会使用 Push-Based Shuffle。
  private val shufflePushMinRatio = sc.getConf.get(config.PUSH_BASED_SHUFFLE_MIN_PUSH_RATIO)
  //用于 Shuffle 合并过程中的线程数。通过多线程来提高合并效率。
  private val shuffleMergeFinalizeNumThreads =
    sc.getConf.get(config.PUSH_BASED_SHUFFLE_MERGE_FINALIZE_THREADS)
  //用于完成 Shuffle finalization 的 RPC 线程数。用于并发处理与 Shuffle 相关的远程过程调
  private val shuffleFinalizeRpcThreads = sc.getConf.get(config.PUSH_SHUFFLE_FINALIZE_RPC_THREADS)
  //启用了推式 Shuffle（pushBasedShuffleEnabled）的情况下，初始化一个 BlockStoreClient 客户端，通常用于与外部 Shuffle 服务进行通信
  // Since SparkEnv gets initialized after DAGScheduler, externalShuffleClient needs to be
  // initialized lazily
  private lazy val externalShuffleClient: Option[BlockStoreClient] =
    if (pushBasedShuffleEnabled) {
      Some(env.blockManager.blockStoreClient)
    } else {
      None
    }

  // When push-based shuffle is enabled, spark driver will submit a finalize task which will send
  // a finalize rpc to each merger ESS after the shuffle map stage is complete. The merge
  // finalization takes up to PUSH_BASED_SHUFFLE_MERGE_RESULTS_TIMEOUT.
  //定时任务调度器，它用于在 Shuffle Map 阶段完成后，提交最终合并（finalization）任务，
  // 并向每个合并的外部 Shuffle 服务（ESS）发送合并 finalize RPC
  private val shuffleMergeFinalizeScheduler =
    ThreadUtils.newDaemonThreadPoolScheduledExecutor("shuffle-merge-finalizer",
      shuffleMergeFinalizeNumThreads)

  // Send finalize RPC tasks to merger ESS
  private val shuffleSendFinalizeRpcExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(shuffleFinalizeRpcThreads, "shuffle-merge-finalize-rpc")

  /** Whether rdd cache visibility tracking is enabled. */
    //是否启用了 RDD 缓存可见性跟踪
  private val trackingCacheVisibility: Boolean =
    sc.getConf.get(RDD_CACHE_VISIBILITY_TRACKING_ENABLED)

  /** 任务开始时被调用
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**任务完成并开始从远程位置获取结果时被调用
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /** 任务完成时被调用，不论任务是成功完成还是失败
   * Called by the TaskSetManager to report task completions or failures.
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      metricPeaks: Array[Long],
      taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, metricPeaks, taskInfo))
  }

  /** 处理来自执行器（Executor）的心跳消息。
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String, //执行器的 ID，标识具体的执行器
      // (taskId, stageId, stageAttemptId, accumUpdates)
      accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])], //任务的累加器更新
      blockManagerId: BlockManagerId, //执行器中 BlockManager 的标识符，表示执行器的块管理器。
      // (stageId, stageAttemptId) -> metrics  执行器的度量信息，按 (stageId, stageAttemptId) 键值对存储
      executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = {

    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates,
      executorUpdates))
    //发送心跳信息给 BlockManagerMaster
    blockManagerMaster.driverHeartbeatEndPoint.askSync[Boolean](
      BlockManagerHeartbeat(blockManagerId),
      new RpcTimeout(blockManagerMasterDriverHeartbeatTimeout, "BlockManagerHeartbeat"))
  }

  /** 当一个执行器丢失时（例如，因故障退出），该方法会被调用
   * Called by TaskScheduler implementation when an executor fails.
   */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /** 当一个工作节点（worker）被移除时，调用此方法。
   * Called by TaskScheduler implementation when a worker is removed.
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit = {
    eventProcessLoop.post(WorkerRemoved(workerId, host, message))
  }

  /** 当一个新的执行器添加时，调用此方法。
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /** 当一个任务集失败时（例如，因重复失败或工作被取消），调用此方法
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  /** 当投机任务被提交时（即，为了提高性能而提前运行某些任务），调用此方法
   * Called by the TaskSetManager when it decides a speculative task is needed.
   */
  def speculativeTaskSubmitted(task: Task[_], taskIndex: Int): Unit = {
    eventProcessLoop.post(SpeculativeTaskSubmitted(task, taskIndex))
  }

  /** 当由于任务失败过多导致任务集无法调度时（且启用了动态资源分配），调用此方法。
   * Called by the TaskSetManager when a taskset becomes unschedulable due to executors being
   * excluded because of too many task failures and dynamic allocation is enabled.
   */
  def unschedulableTaskSetAdded(
      stageId: Int,
      stageAttemptId: Int): Unit = {
    eventProcessLoop.post(UnschedulableTaskSetAdded(stageId, stageAttemptId))
  }

  /** 当一个不可调度的任务集重新变得可以调度时（且启用了动态资源分配），调用此方法。
   * Called by the TaskSetManager when an unschedulable taskset becomes schedulable and dynamic
   * allocation is enabled.
   */
  def unschedulableTaskSetRemoved(
      stageId: Int,
      stageAttemptId: Int): Unit = {
    eventProcessLoop.post(UnschedulableTaskSetRemoved(stageId, stageAttemptId))
  }
  //获取指定 RDD 的缓存位置
  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = rdd.stateLock.synchronized {
    cacheLocs.synchronized {
      // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
      if (!cacheLocs.contains(rdd.id)) {
        // Note: if the storage level is NONE, we don't need to get locations from block manager.
        //没有缓存位置，且 RDD 的存储级别（StorageLevel）为 NONE，则说明该 RDD 不需要缓存，直接返回一个包含空列表的 IndexedSeq
        val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
          IndexedSeq.fill(rdd.partitions.length)(Nil)
        } else {
          //调用 blockManagerMaster.getLocations 获取各个 RDD 分区的缓存位置。每个位置将被转换为 TaskLocation 对象，表示主机和执行器 ID
          val blockIds =
            rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
          blockManagerMaster.getLocations(blockIds).map { bms =>
            bms.map(bm => TaskLocation(bm.host, bm.executorId))
          }
        }
        cacheLocs(rdd.id) = locs
      }
      cacheLocs(rdd.id)
    }
  }
//清除 cacheLocs 中保存的缓存位置
  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
   * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
   * addition to any missing ancestor shuffle map stages.
   */
  private def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      //查找是否已存在某个 shuffle map stage（即通过 shuffleIdToMapStage 查找）
      case Some(stage) =>
        stage

      case None =>
        // Create stages for all missing ancestor shuffle dependencies.
        // 如果 shuffle map stage 不存在，首先创建所有缺失的祖先 shuffle 依赖的 shuffle map stage
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }

  /**
   * Check to make sure we don't launch a barrier stage with unsupported RDD chain pattern. The
   * following patterns are not supported:
   * 1. Ancestor RDDs that have different number of partitions from the resulting RDD (e.g.
   * union()/coalesce()/first()/take()/PartitionPruningRDD);
   * 2. An RDD that depends on multiple barrier RDDs (e.g. barrierRdd1.zip(barrierRdd2)).
   */
    //检查barrier阶段是否符合链式依赖模式
  private def checkBarrierStageWithRDDChainPattern(rdd: RDD[_], numTasksInStage: Int): Unit = {
    if (rdd.isBarrier() &&
        !traverseParentRDDsWithinStage(rdd, (r: RDD[_]) =>
          r.getNumPartitions == numTasksInStage &&
          r.dependencies.count(_.rdd.isBarrier()) <= 1)) {
      throw SparkCoreErrors.barrierStageWithRDDChainPatternError()
    }
  }

  /**
   * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
   * previously run stage generated the same shuffle data, this function will copy the output
   * locations that are still available from the previous shuffle to avoid unnecessarily
   * regenerating data.
   */
    //创建ShuffleMapStage阶段
  def createShuffleMapStage[K, V, C](
      shuffleDep: ShuffleDependency[K, V, C], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val (shuffleDeps, resourceProfiles) = getShuffleDependenciesAndResourceProfiles(rdd) //获取 shuffle 依赖和资源配置
    val resourceProfile = mergeResourceProfilesForStage(resourceProfiles)  // 合并资源配置
    checkBarrierStageWithDynamicAllocation(rdd)  // 检查是否是动态资源分配的 barrier 阶段
    checkBarrierStageWithNumSlots(rdd, resourceProfile)  // 检查是否符合所需的插槽数
    checkBarrierStageWithRDDChainPattern(rdd, rdd.getNumPartitions)  // 检查 RDD 是否符合链式依赖模式
    val numTasks = rdd.partitions.length
    val parents = getOrCreateParentStages(shuffleDeps, jobId) //// 获取或创建父阶段
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(
      id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep, mapOutputTracker,
      resourceProfile.id)

    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage) // 更新 `jobIdToStageIds` 映射

    if (!mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo(s"Registering RDD ${rdd.id} (${rdd.getCreationSite}) as input to " +
        s"shuffle ${shuffleDep.shuffleId}")
      //注册 shuffle 数据到 MapOutputTracker
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length,
        shuffleDep.partitioner.numPartitions)
    }
    stage
  }

  /**
   * We don't support run a barrier stage with dynamic resource allocation enabled, it shall lead
   * to some confusing behaviors (e.g. with dynamic resource allocation enabled, it may happen that
   * we acquire some executors (but not enough to launch all the tasks in a barrier stage) and
   * later release them due to executor idle time expire, and then acquire again).
   *
   * We perform the check on job submit and fail fast if running a barrier stage with dynamic
   * resource allocation enabled.
   *
   * TODO SPARK-24942 Improve cluster resource management with jobs containing barrier stage
   */
    //barrier节点不支持动态分配资源
  private def checkBarrierStageWithDynamicAllocation(rdd: RDD[_]): Unit = {
    if (rdd.isBarrier() && Utils.isDynamicAllocationEnabled(sc.getConf)) {
      throw SparkCoreErrors.barrierStageWithDynamicAllocationError()
    }
  }

  /**
   * Check whether the barrier stage requires more slots (to be able to launch all tasks in the
   * barrier stage together) than the total number of active slots currently. Fail current check
   * if trying to submit a barrier stage that requires more slots than current total number. If
   * the check fails consecutively beyond a configured number for a job, then fail current job
   * submission.
   */
    //barrier节点的任务并发数要大于分区数
  private def checkBarrierStageWithNumSlots(rdd: RDD[_], rp: ResourceProfile): Unit = {
    if (rdd.isBarrier()) {
      val numPartitions = rdd.getNumPartitions
      val maxNumConcurrentTasks = sc.maxNumConcurrentTasks(rp)
      if (numPartitions > maxNumConcurrentTasks) {
        throw SparkCoreErrors.numPartitionsGreaterThanMaxNumConcurrentTasksError(numPartitions,
          maxNumConcurrentTasks)
      }
    }
  }
  //作用是合并多个 ResourceProfile，并确保在合并过程中不会丢失原有的资源配置。这通常在处理 Spark 作业时，涉及到不同阶段的资源需求配置。
  private[scheduler] def mergeResourceProfilesForStage(
      stageResourceProfiles: HashSet[ResourceProfile]): ResourceProfile = {
    logDebug(s"Merging stage rdd profiles: $stageResourceProfiles")
    val resourceProfile = if (stageResourceProfiles.size > 1) { //// 如果有多个资源配置
      if (shouldMergeResourceProfiles) { //// 如果允许合并
        val startResourceProfile = stageResourceProfiles.head //获取第一个资源配置
        val mergedProfile = stageResourceProfiles.drop(1) // 获取剩余的资源配置
          .foldLeft(startResourceProfile)((a, b) => mergeResourceProfiles(a, b)) // 合并所有资源配置
        // compared merged profile with existing ones so we don't add it over and over again
        // if the user runs the same operation multiple times
        val resProfile = sc.resourceProfileManager.getEquivalentProfile(mergedProfile)
        resProfile match {
          case Some(existingRp) => existingRp  //// 如果已存在相同的配置，返回已存在的配置
          case None =>
            // this ResourceProfile could be different if it was merged so we have to add it to
            // our ResourceProfileManager
            // 否则，向资源配置管理器添加新的合并配置
            sc.resourceProfileManager.addResourceProfile(mergedProfile)
            mergedProfile
        }
      } else {
        throw new IllegalArgumentException("Multiple ResourceProfiles specified in the RDDs for " +
          "this stage, either resolve the conflicting ResourceProfiles yourself or enable " +
          s"${config.RESOURCE_PROFILE_MERGE_CONFLICTS.key} and understand how Spark handles " +
          "the merging them.")
      }
    } else {
      if (stageResourceProfiles.size == 1) {   //如果只有一个，不用合并
        stageResourceProfiles.head
      } else {
        sc.resourceProfileManager.defaultResourceProfile
      }
    }
    resourceProfile
  }

  // This is a basic function to merge resource profiles that takes the max
  // value of the profiles. We may want to make this more complex in the future as
  // you may want to sum some resources (like memory).
  //作用是将两个 ResourceProfile 对象合并，并且对于每个资源，选择其中更大的值。
  private[scheduler] def mergeResourceProfiles(
      r1: ResourceProfile,
      r2: ResourceProfile): ResourceProfile = {
    //// 合并 executor 资源：将两个 profile 的 executor 资源合并
    val mergedExecKeys = r1.executorResources ++ r2.executorResources
    val mergedExecReq = mergedExecKeys.map { case (k, v) =>
        val larger = r1.executorResources.get(k).map( x =>
          if (x.amount > v.amount) x else v).getOrElse(v) //// 选择资源中较大的一个
        k -> larger
    }
    //// 合并 task 资源：同样采用与 executor 资源相同的方式
    val mergedTaskKeys = r1.taskResources ++ r2.taskResources
    val mergedTaskReq = mergedTaskKeys.map { case (k, v) =>
      val larger = r1.taskResources.get(k).map( x =>
        if (x.amount > v.amount) x else v).getOrElse(v)
      k -> larger
    }

    if (mergedExecReq.isEmpty) {
      new TaskResourceProfile(mergedTaskReq)
    } else {
      new ResourceProfile(mergedExecReq, mergedTaskReq)
    }
  }

  /** ResultStage 是 Spark 执行计划中的一个阶段，通常在计算作业的结果时使用。
   * Create a ResultStage associated with the provided jobId.
   */
  private def createResultStage(
      rdd: RDD[_], //该阶段的输入 RDD。
      func: (TaskContext, Iterator[_]) => _,  //执行任务的函数，Iterator[_]：当前任务的输入数据迭代器。
      partitions: Array[Int], //该阶段操作的分区数组。每个分区对应一个任务。
      jobId: Int, //与该阶段关联的作业 ID。
      callSite: CallSite): ResultStage = {
    val (shuffleDeps, resourceProfiles) = getShuffleDependenciesAndResourceProfiles(rdd) //获取 RDD 的 Shuffle 依赖和资源配置。
    val resourceProfile = mergeResourceProfilesForStage(resourceProfiles)
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd, resourceProfile)
    checkBarrierStageWithRDDChainPattern(rdd, partitions.toSet.size)
    val parents = getOrCreateParentStages(shuffleDeps, jobId) //获取父级阶段
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId,
      callSite, resourceProfile.id)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
   * Get or create the list of parent stages for the given shuffle dependencies. The new
   * Stages will be created with the provided firstJobId.
   */
    //递归调用
  private def getOrCreateParentStages(shuffleDeps: HashSet[ShuffleDependency[_, _, _]],
      firstJobId: Int): List[Stage] = {
    shuffleDeps.map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }
  //目的是查找那些尚未注册的祖先 shuffle 依赖。
  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getMissingAncestorShuffleDependencies(
      rdd: RDD[_]): ListBuffer[ShuffleDependency[_, _, _]] = {
    val ancestors = new ListBuffer[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]] // 用于记录已访问的 RDD，避免重复访问
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        val (shuffleDeps, _) = getShuffleDependenciesAndResourceProfiles(toVisit)
        shuffleDeps.foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            ancestors.prepend(shuffleDep)
            waitingForVisit.prepend(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    ancestors
  }

  /**
   * Returns shuffle dependencies that are immediate parents of the given RDD and the
   * ResourceProfiles associated with the RDDs for this stage.
   *
   * This function will not return more distant ancestors for shuffle dependencies. For example,
   * if C has a shuffle dependency on B which has a shuffle dependency on A:
   *
   * A <-- B <-- C
   *
   * calling this function with rdd C will only return the B <-- C dependency.
   *
   * This function is scheduler-visible for the purpose of unit testing.
   */
    //返回给定 RDD 的直接父级 shuffle 依赖及与这些 RDD 关联的资源配置
  private[scheduler] def getShuffleDependenciesAndResourceProfiles(
      rdd: RDD[_]): (HashSet[ShuffleDependency[_, _, _]], HashSet[ResourceProfile]) = {
    val parents = new HashSet[ShuffleDependency[_, _, _]] // 存储直接父级 shuffle 依赖
    val resourceProfiles = new HashSet[ResourceProfile]   // 存储与 RDD 关联的资源配置
    val visited = new HashSet[RDD[_]]                     // 存储已访问的 RDD，避免重复访问
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        Option(toVisit.getResourceProfile).foreach(resourceProfiles += _)
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep  //shuffle依赖，不会继续递归
          case dependency =>
            waitingForVisit.prepend(dependency.rdd)   //其他依赖会继续递归
        }
      }
    }
    (parents, resourceProfiles)
  }

  /**
   * Traverses the given RDD and its ancestors within the same stage and checks whether all of the
   * RDDs satisfy a given predicate.
   */
    //遍历给定 RDD 及其同一阶段中的祖先 RDD，并检查是否所有这些 RDD 都满足指定的条件（通过 predicate 进行检查）
  private def traverseParentRDDsWithinStage(rdd: RDD[_], predicate: RDD[_] => Boolean): Boolean = {
    //rdd：当前检查的 RDD。
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        if (!predicate(toVisit)) {
          return false
        }
        visited += toVisit
        toVisit.dependencies.foreach {
          case _: ShuffleDependency[_, _, _] =>
            //shuffle依赖则不在同一个stage内
            // Not within the same stage with current rdd, do nothing.
          case dependency =>
            waitingForVisit.prepend(dependency.rdd) //将其加入待访问列表，继续递归遍历
        }
      }
    }
    true
  }
  //通过遍历涉及该阶段及其依赖的 RDD，识别出那些未缓存或者需要重新计算的父阶段
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += stage.rdd
    def visit(rdd: RDD[_]): Unit = {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil) //检查该 RDD 的分区是否有未缓存的部分（通过 getCacheLocs(rdd) 来获取缓存位置
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              //如果发现某个分区未缓存，则检查它的依赖关系
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                // Mark mapStage as available with shuffle outputs only after shuffle merge is
                // finalized with push based shuffle. If not, subsequent ShuffleMapStage won't
                // read from merged output as the MergeStatuses are not available.
                //如果是 ShuffleDependency（洗牌依赖），则获取或创建对应的 ShuffleMapStage，并检查该阶段的可用性以及洗牌合并是否已完成
                if (!mapStage.isAvailable || !mapStage.shuffleDep.shuffleMergeFinalized) {
                  missing += mapStage
                } else {
                  // Forward the nextAttemptId if skipped and get visited for the first time.
                  // Otherwise, once it gets retried,
                  // 1) the stuffs in stage info become distorting, e.g. task num, input byte, e.t.c
                  // 2) the first attempt starts from 0-idx, it will not be marked as a retry
                  mapStage.increaseAttemptIdOnFirstSkip()
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.prepend(narrowDep.rdd)
            }
          }
        }
      }
    }
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.remove(0))
    }
    missing.toList
  }
  //用于在给定的 RDD 及其所有祖先 RDD 上预先计算分区。
  // 通过调用 .partitions，它会强制触发分区计算。其主要目的是确保所有相关的 RDD 和其依赖项都被访问并准备好。
  /** Invoke `.partitions` on the given RDD and all of its ancestors  */
  private def eagerlyComputePartitionsForRddAndAncestors(rdd: RDD[_]): Unit = {
    val startTime = System.nanoTime
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd

    def visit(rdd: RDD[_]): Unit = {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd

        // Eagerly compute:
        //强制计算并触发该 RDD 的分区信息（可能是实际的数据加载或计算操作）
        rdd.partitions

        for (dep <- rdd.dependencies) {
          waitingForVisit.prepend(dep.rdd)
        }
      }
    }

    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.remove(0))
    }
    logDebug("eagerlyComputePartitionsForRddAndAncestors for RDD %d took %f seconds"
      .format(rdd.id, (System.nanoTime - startTime) / 1e9))
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    //jobId：需要注册的作业 ID。
    // stage：当前阶段（Stage 对象），它的祖先阶段也需要更新映射关系
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]): Unit = {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId  //把新的jobId加入到stage里面
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parentsWithoutThisJobId = s.parents.filter { ! _.jobIds.contains(jobId) }  //过滤出还没有包含此jobId的stage
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail) //递归加入
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   *
   * @param job The job whose state to cleanup.
   */
    //负责清理与给定job相关的状态，并移除所有不再需要的阶段。
  // 具体来说，它会从不同的内部数据结构中清理已完成或不再需要的阶段。
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int): Unit = {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                  shuffleIdToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
   * Submit an action job to the scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
    //提交一个动作类型的作业（action job）到调度器的实现。它会负责检查作业的合法性、计算分区以及将作业提交给调度器进行执行
  def submitJob[T, U](
      rdd: RDD[T], //目标 RDD，作业将在该 RDD 上运行任务
      func: (TaskContext, Iterator[T]) => U, //指定每个分区上的任务应执行的操作
      partitions: Seq[Int], //待处理的分区列表，有些作业可能不需要计算所有分区（如操作 first()）。
      callSite: CallSite,
      resultHandler: (Int, U) => Unit, //回调函数，用于处理每个任务的结果。
      properties: Properties): JobWaiter[U] = {  //JobWaiter 对象，可以用来在作业执行完成之前阻塞当前线程，或者在作业执行期间取消作业。
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }   //检查分区的合法性

    // SPARK-23626: `RDD.getPartitions()` can be slow, so we eagerly compute
    // `.partitions` on every RDD in the DAG to ensure that `getPartitions()`
    // is evaluated outside of the DAGScheduler's single-threaded event loop:
    eagerlyComputePartitionsForRddAndAncestors(rdd)   //提前计算 RDD 的分区：为了避免后续在 DAGScheduler 的单线程事件循环中多次计算 .partitions，此方法会提前计算所有 RDD 及其祖先的分区信息

    val jobId = nextJobId.getAndIncrement()
    if (partitions.isEmpty) { //如果传入的分区为空，则立即创建一个 JobWaiter，并发送作业开始和结束的事件，同时直接返回一个已完成的作业。
      val clonedProperties = Utils.cloneProperties(properties)
      if (sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION) == null) {
        clonedProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, callSite.shortForm)
      }
      val time = clock.getTimeMillis()
      listenerBus.post(
        SparkListenerJobStart(jobId, time, Seq.empty, clonedProperties))
      listenerBus.post(
        SparkListenerJobEnd(jobId, time, JobSucceeded))
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    //如果分区非空，则创建一个 JobWaiter，并将作业信息（包括 RDD、任务函数、分区等）发送到 eventProcessLoop，以便调度器处理
    assert(partitions.nonEmpty)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter[U](this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      JobArtifactSet.getActiveOrDefault(sc),
      Utils.cloneProperties(properties)))
    waiter
  }

  /**
   * Run an action job on the given RDD and pass all the results to the resultHandler function as
   * they arrive.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @note Throws `Exception` when the job fails
   */
    //它用于在给定的 RDD 上运行一个动作作业（action job），并将结果传递给 resultHandler 函数。
  // 这是一个常见的 Spark 操作方法，可以用于执行分布式计算并在计算完成时处理结果。
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)  //提交作用
    ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)  //等待完成
    waiter.completionFuture.value.get match {  //打印成功失败信息
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
   * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
   * as they arrive. Returns a partial result object from the evaluator.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param evaluator `ApproximateEvaluator` to receive the partial results
   * @param callSite where in the user program this job was called
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
    //方法允许用户获取作业的近似结果。该作业会并行运行在各个分区上，并根据设定的时间限制返回部分结果
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val jobId = nextJobId.getAndIncrement()
    val clonedProperties = Utils.cloneProperties(properties)
    if (rdd.partitions.isEmpty) {
      // Return immediately if the job is running 0 tasks
      val time = clock.getTimeMillis()
      listenerBus.post(SparkListenerJobStart(jobId, time, Seq[StageInfo](), clonedProperties))
      listenerBus.post(SparkListenerJobEnd(jobId, time, JobSucceeded))
      return new PartialResult(evaluator.currentResult(), true)
    }

    // SPARK-23626: `RDD.getPartitions()` can be slow, so we eagerly compute
    // `.partitions` on every RDD in the DAG to ensure that `getPartitions()`
    // is evaluated outside of the DAGScheduler's single-threaded event loop:
    eagerlyComputePartitionsForRddAndAncestors(rdd)

    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, rdd.partitions.indices.toArray, callSite, listener,
      JobArtifactSet.getActiveOrDefault(sc), clonedProperties))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
   * can be used to block until the job finishes executing or can be used to cancel the job.
   * This method is used for adaptive query planning, to run map stages and look at statistics
   * about their outputs before submitting downstream stages.
   *
   * @param dependency the ShuffleDependency to run a map stage for
   * @param callback function called with the result of the job, which in this case will be a
   *   single MapOutputStatistics object showing how much data was produced for each partition
   * @param callSite where in the user program this job was submitted
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
    //用于提交一个独立运行的 shuffle map 阶段
  def submitMapStage[K, V, C](
      dependency: ShuffleDependency[K, V, C], //表示该 map 阶段需要处理的 shuffle 依赖关系。它定义了 shuffle 操作、键值对及 map 阶段的结果
      callback: MapOutputStatistics => Unit, //包含每个分区产生的数据量等统计信息
      callSite: CallSite,
      properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw SparkCoreErrors.cannotRunSubmitMapStageOnZeroPartitionRDDError()
    }

    // SPARK-23626: `RDD.getPartitions()` can be slow, so we eagerly compute
    // `.partitions` on every RDD in the DAG to ensure that `getPartitions()`
    // is evaluated outside of the DAGScheduler's single-threaded event loop:
    eagerlyComputePartitionsForRddAndAncestors(rdd)   //提前计算

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter[MapOutputStatistics](
      this, jobId, 1,
      (_: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, JobArtifactSet.getActiveOrDefault(sc),
      Utils.cloneProperties(properties)))
    waiter
  }

  /** 取消作用
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int, reason: Option[String]): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId, reason))
  }

  /** 取消作用组
   * Cancel all jobs in the given job group ID.
   */
  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**取消标签为tag的作用
   * Cancel all jobs with a given tag.
   */
  def cancelJobsWithTag(tag: String): Unit = {
    SparkContext.throwIfInvalidTag(tag)
    logInfo(s"Asked to cancel jobs with tag $tag")
    eventProcessLoop.post(JobTagCancelled(tag))
  }

  /**取消所有的job
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }
  //取消所有的job
  private[scheduler] def doCancelAllJobs(): Unit = {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      Option("as part of cancellation of all jobs")))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
  }

  /**取消stage
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int, reason: Option[String]): Unit = {
    eventProcessLoop.post(StageCancelled(stageId, reason))
  }

  /**通知shuffle push完成
   * Receives notification about shuffle push for a given shuffle from one map
   * task has completed
   */
  def shufflePushCompleted(shuffleId: Int, shuffleMergeId: Int, mapIndex: Int): Unit = {
    eventProcessLoop.post(ShufflePushCompleted(shuffleId, shuffleMergeId, mapIndex))
  }

  /**
   * Kill a given task. It will be retried.
   *
   * @return Whether the task was successfully killed.
   */
    //通过taskScheduler kill task 的Attempt
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    taskScheduler.killTaskAttempt(taskId, interruptThread, reason)
  }

  /** 重新提交失败的stage
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages(): Unit = {
    if (failedStages.nonEmpty) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
  }

  /** 提交等待提交的stage
   * Check for waiting stages which are now eligible for resubmission.
   * Submits stages that depend on the given parent stage. Called when the parent stage completes
   * successfully.
   */
  private def submitWaitingChildStages(parent: Stage): Unit = {
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    waitingStages --= childStages
    for (stage <- childStages.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
    //查找某个stage活跃的job
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String): Unit = {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_,
        Option("part of cancelled job group %s".format(groupId))))
  }

  private[scheduler] def handleJobTagCancelled(tag: String): Unit = {
    // Cancel all jobs belonging that have this tag.
    // First finds all active jobs with this group id, and then kill stages for them.
    val jobIds = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists { properties =>
        Option(properties.getProperty(SparkContext.SPARK_JOB_TAGS)).getOrElse("")
          .split(SparkContext.SPARK_JOB_TAGS_SEP).filter(!_.isEmpty).toSet.contains(tag)
      }
    }.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_,
      Option(s"part of cancelled job tag $tag")))
  }
   //处理任务开始调度的事件
  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo): Unit = {
    listenerBus.post(SparkListenerTaskStart(task.stageId, task.stageAttemptId, taskInfo))
  }

  private[scheduler] def handleSpeculativeTaskSubmitted(task: Task[_], taskIndex: Int): Unit = {
    val speculativeTaskSubmittedEvent = new SparkListenerSpeculativeTaskSubmitted(
      task.stageId, task.stageAttemptId, taskIndex, task.partitionId)
    listenerBus.post(speculativeTaskSubmittedEvent)
  }

  private[scheduler] def handleUnschedulableTaskSetAdded(
      stageId: Int,
      stageAttemptId: Int): Unit = {
    listenerBus.post(SparkListenerUnschedulableTaskSetAdded(stageId, stageAttemptId))
  }

  private[scheduler] def handleUnschedulableTaskSetRemoved(
      stageId: Int,
      stageAttemptId: Int): Unit = {
    listenerBus.post(SparkListenerUnschedulableTaskSetRemoved(stageId, stageAttemptId))
  }

  private[scheduler] def handleStageFailed(
      stageId: Int,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(stageId).foreach { abortStage(_, reason, exception) }
  }

  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
  }

  private[scheduler] def cleanUpAfterSchedulerStop(): Unit = {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo): Unit = {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }

  private[scheduler] def handleJobSubmitted(jobId: Int, //作业的唯一标识符。
      finalRDD: RDD[_], //最终结果的 RDD，作业将在这个 RDD 上运行。
      func: (TaskContext, Iterator[_]) => _, //应用于 RDD 上每个分区的函数，
      partitions: Array[Int], //分区数组，指定作业运行的目标分区。
      callSite: CallSite,
      listener: JobListener,
      artifacts: JobArtifactSet,
      properties: Properties): Unit = {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)  //创建阶段，会递归创建所有父阶段
    } catch {
      case e: BarrierJobSlotsNumberCheckFailed =>
        // If jobId doesn't exist in the map, Scala coverts its value null to 0: Int automatically.
        val numCheckFailures = barrierJobIdToNumTasksCheckFailures.compute(jobId,
          (_: Int, value: Int) => value + 1)

        logWarning(s"Barrier stage in job $jobId requires ${e.requiredConcurrentTasks} slots, " +
          s"but only ${e.maxConcurrentTasks} are available. " +
          s"Will retry up to ${maxFailureNumTasksCheck - numCheckFailures + 1} more times")

        if (numCheckFailures <= maxFailureNumTasksCheck) {
          messageScheduler.schedule(
            new Runnable {
              override def run(): Unit = eventProcessLoop.post(JobSubmitted(jobId, finalRDD, func,
                partitions, callSite, listener, artifacts, properties))
            },
            timeIntervalNumTasksCheck,
            TimeUnit.SECONDS
          )
          return
        } else {
          // Job failed, clear internal data.
          barrierJobIdToNumTasksCheckFailures.remove(jobId)
          listener.jobFailed(e)
          return
        }

      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    // Job submitted, clear internal data.
    barrierJobIdToNumTasksCheckFailures.remove(jobId)

    val job = new ActiveJob(jobId, finalStage, callSite, listener, artifacts, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos,
        Utils.cloneProperties(properties)))
    submitStage(finalStage)  //提交stage
  }

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
      dependency: ShuffleDependency[_, _, _],
      callSite: CallSite,
      listener: JobListener,
      artifacts: JobArtifactSet,
      properties: Properties): Unit = {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, artifacts, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos,
        Utils.cloneProperties(properties)))
    submitStage(finalStage)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
  }

  /** Submits stage, but first recursively submits any missing parents. */
  private def submitStage(stage: Stage): Unit = {
    val jobId = activeJobForStage(stage)
    //通过 activeJobForStage(stage) 获取该阶段所属的作业 ID。如果作业 ID 存在，说明该阶段是作业的一部分，继续执行；否则终止阶段提交。
    if (jobId.isDefined) {
      logDebug(s"submitStage($stage (name=${stage.name};" +
        s"jobs=${stage.jobIds.toSeq.sorted.mkString(",")}))")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        if (stage.getNextAttemptId >= maxStageAttempts) { //如果该阶段已经尝试过的次数超过最大尝试次数 maxStageAttempts，则认为该阶段执行失败，并通过 abortStage 中止该阶段，给出原因
          val reason = s"$stage (name=${stage.name}) has been resubmitted for the maximum " +
            s"allowable number of times: ${maxStageAttempts}, which is the max value of " +
            s"config `spark.stage.maxAttempts` and `spark.stage.maxConsecutiveAttempts`."
          abortStage(stage, reason, None)
        } else {
          val missing = getMissingParentStages(stage).sortBy(_.id)
          logDebug("missing: " + missing)
          if (missing.isEmpty) {
            logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
            submitMissingTasks(stage, jobId.get)
          } else {
            //如果有缺失的父阶段，递归提交这些父阶段。父阶段会按照 ID 升序排序，确保按顺序提交。
            for (parent <- missing) {
              submitStage(parent)
            }
            waitingStages += stage
          }
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /**
   * `PythonRunner` needs to know what the pyspark memory and cores settings are for the profile
   * being run. Pass them in the local properties of the task if it's set for the stage profile.
   */
  private def addPySparkConfigsToProperties(stage: Stage, properties: Properties): Unit = {
    val rp = sc.resourceProfileManager.resourceProfileFromId(stage.resourceProfileId)
    val pysparkMem = rp.getPySparkMemory
    // use the getOption on EXECUTOR_CORES.key instead of using the EXECUTOR_CORES config reader
    // because the default for this config isn't correct for standalone mode. Here we want
    // to know if it was explicitly set or not. The default profile always has it set to either
    // what user specified or default so special case it here.
    val execCores = if (rp.id == DEFAULT_RESOURCE_PROFILE_ID) {
      sc.conf.getOption(config.EXECUTOR_CORES.key)
    } else {
      val profCores = rp.getExecutorCores.map(_.toString)
      if (profCores.isEmpty) sc.conf.getOption(config.EXECUTOR_CORES.key) else profCores
    }
    pysparkMem.map(mem => properties.setProperty(PYSPARK_MEMORY_LOCAL_PROPERTY, mem.toString))
    execCores.map(cores => properties.setProperty(EXECUTOR_CORES_LOCAL_PROPERTY, cores))
  }

  /**
   * If push based shuffle is enabled, set the shuffle services to be used for the given
   * shuffle map stage for block push/merge.
   *
   * Even with dynamic resource allocation kicking in and significantly reducing the number
   * of available active executors, we would still be able to get sufficient shuffle service
   * locations for block push/merge by getting the historical locations of past executors.
   */
  private def prepareShuffleServicesForShuffleMapStage(stage: ShuffleMapStage): Unit = {
    assert(stage.shuffleDep.shuffleMergeAllowed && !stage.shuffleDep.isShuffleMergeFinalizedMarked)
    configureShufflePushMergerLocations(stage)

    val shuffleId = stage.shuffleDep.shuffleId
    val shuffleMergeId = stage.shuffleDep.shuffleMergeId
    if (stage.shuffleDep.shuffleMergeEnabled) {
      logInfo(s"Shuffle merge enabled before starting the stage for $stage with shuffle" +
        s" $shuffleId and shuffle merge $shuffleMergeId with" +
        s" ${stage.shuffleDep.getMergerLocs.size} merger locations")
    } else {
      logInfo(s"Shuffle merge disabled for $stage with shuffle $shuffleId" +
        s" and shuffle merge $shuffleMergeId, but can get enabled later adaptively" +
        s" once enough mergers are available")
    }
  }

  private def configureShufflePushMergerLocations(stage: ShuffleMapStage): Unit = {
    if (stage.shuffleDep.getMergerLocs.nonEmpty) return
    val mergerLocs = sc.schedulerBackend.getShufflePushMergerLocations(
      stage.shuffleDep.partitioner.numPartitions, stage.resourceProfileId)
    if (mergerLocs.nonEmpty) {
      stage.shuffleDep.setMergerLocs(mergerLocs)
      mapOutputTracker.registerShufflePushMergerLocations(stage.shuffleDep.shuffleId, mergerLocs)
      logDebug(s"Shuffle merge locations for shuffle ${stage.shuffleDep.shuffleId} with" +
        s" shuffle merge ${stage.shuffleDep.shuffleMergeId} is" +
        s" ${stage.shuffleDep.getMergerLocs.map(_.host).mkString(", ")}")
    }
  }

  /** Called when stage's parents are available and we can now do its task. */
    //在阶段的父阶段完成后，提交当前阶段的任务。它处理了任务的调度、序列化、广播等多个操作，并确保所有任务都能正确执行。
  private def submitMissingTasks(stage: Stage, jobId: Int): Unit = {
    logDebug("submitMissingTasks(" + stage + ")")

    // Before find missing partition, do the intermediate state clean work first.
    // The operation here can make sure for the partially completed intermediate stage,
    // `findMissingPartitions()` returns all partitions every time.
    stage match {
      //如果阶段是 ShuffleMapStage 且当前阶段是不可确定的并且输出不可用，
      // 则调用 mapOutputTracker.unregisterAllMapAndMergeOutput 来清理与该阶段相关的输出。
      case sms: ShuffleMapStage if stage.isIndeterminate && !sms.isAvailable =>
        mapOutputTracker.unregisterAllMapAndMergeOutput(sms.shuffleDep.shuffleId)
        sms.shuffleDep.newShuffleMergeState()
      case _ =>
    }

    // Figure out the indexes of partition ids to compute.
    //查找缺失的分区
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties
    addPySparkConfigsToProperties(stage, properties)

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
        // Only generate merger location for a given shuffle dependency once.
        if (s.shuffleDep.shuffleMergeAllowed) {
          if (!s.shuffleDep.isShuffleMergeFinalizedMarked) {
            prepareShuffleServicesForShuffleMapStage(s)
          } else {
            // Disable Shuffle merge for the retry/reuse of the same shuffle dependency if it has
            // already been merge finalized. If the shuffle dependency was previously assigned
            // merger locations but the corresponding shuffle map stage did not complete
            // successfully, we would still enable push for its retry.
            s.shuffleDep.setShuffleMergeAllowed(false)
            logInfo(s"Push-based shuffle disabled for $stage (${stage.name}) since it" +
              " is already shuffle merge finalized")
          }
        }
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    // 创建一个 Map，该 Map 包含每个任务（分区）的任务位置
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        // 如果创建任务位置时发生异常，标记阶段失败，并记录错误
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo,
          Utils.cloneProperties(properties)))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }
    // 为当前阶段创建新的 Stage 尝试，并将任务位置传递给每个任务
    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)

    // If there are tasks to execute, record the submission time of the stage. Otherwise,
    // post the even without the submission time, which indicates that this stage was
    // skipped.
    // 如果有需要计算的任务，记录阶段提交时间；否则，跳过该阶段
    if (partitionsToCompute.nonEmpty) {
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    }
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo,
      Utils.cloneProperties(properties)))  // 发布 Stage 提交事件

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    var partitions: Array[Partition] = null
    try {

      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      var taskBinaryBytes: Array[Byte] = null
      // taskBinaryBytes and partitions are both effected by the checkpoint status. We need
      // this synchronization in case another concurrent job is checkpointing this RDD, so we get a
      // consistent view of both variables.
      RDDCheckpointData.synchronized {
        // 根据阶段的类型，序列化任务的闭包（如 ShuffleMapStage 或 ResultStage）
        taskBinaryBytes = stage match {
          case stage: ShuffleMapStage =>
            JavaUtils.bufferToArray(
              closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
          case stage: ResultStage =>
            JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
        }

        partitions = stage.rdd.partitions
      }

      if (taskBinaryBytes.length > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024) {
        logWarning(s"Broadcasting large task binary with size " +
          s"${Utils.bytesToString(taskBinaryBytes.length)}")
      }
      taskBinary = sc.broadcast(taskBinaryBytes)  // 广播任务二进制
    } catch {
      // In the case of a failure during serialization, abort the stage.
      // 如果任务闭包不支持序列化，标记阶段失败并中止
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case e: Throwable =>
        // 如果序列化过程中发生其他异常，标记阶段失败并中止
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage

        // Abort execution
        return
    }

    val artifacts = jobIdToActiveJob(jobId).artifacts

    val tasks: Seq[Task[_]] = try {
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      stage match {
        case stage: ShuffleMapStage =>
          stage.pendingPartitions.clear()  //清除待处理的分区
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = partitions(id)
            stage.pendingPartitions += id  //记录待处理的分区
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber, taskBinary,
              part, stage.numPartitions, locs, artifacts, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
              stage.rdd.isBarrier())      //创建 ShuffleMapTask
          }

        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptNumber,
              taskBinary, part, stage.numPartitions, locs, id, artifacts, properties,
              serializedTaskMetrics, Option(jobId), Option(sc.applicationId),
              sc.applicationAttemptId, stage.rdd.isBarrier())  // 创建 ResultTask
          }
      }
    } catch {
      case NonFatal(e) =>
        // 如果创建任务时发生异常，标记阶段失败并中止
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage   // 从正在运行的阶段中移除
        return
    }

    if (tasks.nonEmpty) {
      logInfo(s"Submitting ${tasks.size} missing tasks from $stage (${stage.rdd}) (first 15 " +
        s"tasks are for partitions ${tasks.take(15).map(_.partitionId)})")
      val shuffleId = stage match {
        case s: ShuffleMapStage => Some(s.shuffleDep.shuffleId)
        case _: ResultStage => None
      }
      // 将任务提交给任务调度器
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptNumber, jobId, properties,
        stage.resourceProfileId, shuffleId))
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      stage match {
        case stage: ShuffleMapStage =>
          logDebug(s"Stage ${stage} is actually done; " +
              s"(available: ${stage.isAvailable}," +
              s"available outputs: ${stage.numAvailableOutputs}," +
              s"partitions: ${stage.numPartitions})")
          if (!stage.shuffleDep.isShuffleMergeFinalizedMarked &&
            stage.shuffleDep.getMergerLocs.nonEmpty) {
            checkAndScheduleShuffleMergeFinalize(stage)
          } else {
            processShuffleMapStageCompletion(stage)
          }
        case stage : ResultStage =>
          logDebug(s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})")
          markStageAsFinished(stage)
          submitWaitingChildStages(stage)
      }
    }
  }

  /**
   * Merge local values from a task into the corresponding accumulators previously registered
   * here on the driver.
   *
   * Although accumulators themselves are not thread-safe, this method is called only from one
   * thread, the one that runs the scheduling loop. This means we only handle one task
   * completion event at a time so we don't need to worry about locking the accumulators.
   * This still doesn't stop the caller from updating the accumulator outside the scheduler,
   * but that's not our problem since there's nothing we can do about that.
   */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)

    event.accumUpdates.foreach { updates =>
      val id = updates.id
      try {
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw SparkCoreErrors.accessNonExistentAccumulatorError(id)
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.setAccumulables(
            acc.toInfo(Some(updates.value), Some(acc.value)) +: event.taskInfo.accumulables)
        }
      } catch {
        case NonFatal(e) =>
          // Log the class name to make it easy to find the bad implementation
          val accumClassName = AccumulatorContext.get(id) match {
            case Some(accum) => accum.getClass.getName
            case None => "Unknown class"
          }
          logError(
            s"Failed to update accumulator $id ($accumClassName) for task ${task.partitionId}",
            e)
      }
    }
  }

  private def postTaskEnd(event: CompletionEvent): Unit = {
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            val taskId = event.taskInfo.taskId
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    listenerBus.post(SparkListenerTaskEnd(event.task.stageId, event.task.stageAttemptId,
      Utils.getFormattedClassName(event.task), event.reason, event.taskInfo,
      new ExecutorMetrics(event.metricPeaks), taskMetrics))
  }

  /**
   * Check [[SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL]] in job properties to see if we should
   * interrupt running tasks. Returns `false` if the property value is not a boolean value
   */
  private def shouldInterruptTaskThread(job: ActiveJob): Boolean = {
    if (job.properties == null) {
      false
    } else {
      val shouldInterruptThread =
        job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
      try {
        shouldInterruptThread.toBoolean
      } catch {
        case e: IllegalArgumentException =>
          logWarning(s"${SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL} in Job ${job.jobId} " +
            s"is invalid: $shouldInterruptThread. Using 'false' instead", e)
          false
      }
    }
  }

  private[scheduler] def checkAndScheduleShuffleMergeFinalize(
      shuffleStage: ShuffleMapStage): Unit = {
    // Check if a finalize task has already been scheduled. This is to prevent scenarios
    // where we don't schedule multiple shuffle merge finalization which can happen due to
    // stage retry or shufflePushMinRatio is already hit etc.
    if (shuffleStage.shuffleDep.getFinalizeTask.isEmpty) {
      // 1. Stage indeterminate and some map outputs are not available - finalize
      // immediately without registering shuffle merge results.
      // 2. Stage determinate and some map outputs are not available - decide to
      // register merge results based on map outputs size available and
      // shuffleMergeWaitMinSizeThreshold.
      // 3. All shuffle outputs available - decide to register merge results based
      // on map outputs size available and shuffleMergeWaitMinSizeThreshold.
      val totalSize = {
        lazy val computedTotalSize =
          mapOutputTracker.getStatistics(shuffleStage.shuffleDep).
            bytesByPartitionId.filter(_ > 0).sum
        if (shuffleStage.isAvailable) {
          computedTotalSize
        } else {
          if (shuffleStage.isIndeterminate) {
            0L
          } else {
            computedTotalSize
          }
        }
      }

      if (totalSize < shuffleMergeWaitMinSizeThreshold) {
        scheduleShuffleMergeFinalize(shuffleStage, delay = 0, registerMergeResults = false)
      } else {
        scheduleShuffleMergeFinalize(shuffleStage, shuffleMergeFinalizeWaitSec)
      }
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent): Unit = {
    val task = event.task
    val stageId = task.stageId

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.stageAttemptId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    if (!stageIdToStage.contains(task.stageId)) {
      // The stage may have already finished when we get this event -- e.g. maybe it was a
      // speculative task. It is important that we send the TaskEnd event in any case, so listeners
      // are properly notified and can chose to handle it. For instance, some listeners are
      // doing their own accounting and if they don't get the task end event they think
      // tasks are still running when they really aren't.
      postTaskEnd(event)

      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)

    // Make sure the task's accumulators are updated before any other processing happens, so that
    // we can post a task end event before any jobs or stages are updated. The accumulators are
    // only updated in certain cases.
    event.reason match {
      case Success =>
        task match {
          case rt: ResultTask[_, _] =>
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                // Only update the accumulator once for each result task.
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                }
              case None => // Ignore update if task's job has finished.
            }
          case _ =>
            updateAccumulators(event)
        }
      case _: ExceptionFailure | _: TaskKilled => updateAccumulators(event)
      case _ =>
    }
    if (trackingCacheVisibility) {
      // Update rdd blocks' visibility status.
      blockManagerMaster.updateRDDBlockVisibility(
        event.taskInfo.taskId, visible = event.reason == Success)
    }

    postTaskEnd(event)

    event.reason match {
      case Success =>
        // An earlier attempt of a stage (which is zombie) may still have running tasks. If these
        // tasks complete, they still count and we can mark the corresponding partitions as
        // finished if the stage is determinate. Here we notify the task scheduler to skip running
        // tasks for the same partition to save resource.
        if (!stage.isIndeterminate && task.stageAttemptId < stage.latestInfo.attemptNumber()) {
          taskScheduler.notifyPartitionCompletion(stageId, task.partitionId)
        }

        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cancelRunningIndependentStages(job, s"Job ${job.jobId} is finished.")
                    cleanupStateForJobAndIndependentStages(job)
                    try {
                      // killAllTaskAttempts will fail if a SchedulerBackend does not implement
                      // killTask.
                      logInfo(s"Job ${job.jobId} is finished. Cancelling potential speculative " +
                        "or zombie tasks for this job")
                      // ResultStage is only used by this job. It's safe to kill speculative or
                      // zombie tasks in this stage.
                      taskScheduler.killAllTaskAttempts(
                        stageId,
                        shouldInterruptTaskThread(job),
                        reason = "Stage finished")
                    } catch {
                      case e: UnsupportedOperationException =>
                        logWarning(s"Could not cancel tasks for stage $stageId", e)
                    }
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Throwable if !Utils.isFatalError(e) =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            // Ignore task completion for old attempt of indeterminate stage
            val ignoreIndeterminate = stage.isIndeterminate &&
              task.stageAttemptId < stage.latestInfo.attemptNumber()
            if (!ignoreIndeterminate) {
              shuffleStage.pendingPartitions -= task.partitionId
              val status = event.result.asInstanceOf[MapStatus]
              val execId = status.location.executorId
              logDebug("ShuffleMapTask finished on " + execId)
              if (executorFailureEpoch.contains(execId) &&
                smt.epoch <= executorFailureEpoch(execId)) {
                logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
              } else {
                // The epoch of the task is acceptable (i.e., the task was launched after the most
                // recent failure we're aware of for the executor), so mark the task's output as
                // available.
                mapOutputTracker.registerMapOutput(
                  shuffleStage.shuffleDep.shuffleId, smt.partitionId, status)
              }
            } else {
              logInfo(s"Ignoring $smt completion from an older attempt of indeterminate stage")
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              if (!shuffleStage.shuffleDep.isShuffleMergeFinalizedMarked &&
                shuffleStage.shuffleDep.getMergerLocs.nonEmpty) {
                checkAndScheduleShuffleMergeFinalize(shuffleStage)
              } else {
                processShuffleMapStageCompletion(shuffleStage)
              }
            }
        }

      case FetchFailed(bmAddress, shuffleId, _, mapIndex, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleIdToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptNumber != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ${failedStage.latestInfo.attemptNumber}) running")
        } else {
          val ignoreStageFailure = ignoreDecommissionFetchFailure &&
            isExecutorDecommissioningOrDecommissioned(taskScheduler, bmAddress)
          if (ignoreStageFailure) {
            logInfo(s"Ignoring fetch failure from $task of $failedStage attempt " +
              s"${task.stageAttemptId} when count spark.stage.maxConsecutiveAttempts " +
              s"as executor ${bmAddress.executorId} is decommissioned and " +
              s" ${config.STAGE_IGNORE_DECOMMISSION_FETCH_FAILURE.key}=true")
          } else {
            failedStage.failedAttemptIds.add(task.stageAttemptId)
          }

          val shouldAbortStage =
            failedStage.failedAttemptIds.size >= maxConsecutiveStageAttempts ||
            disallowStageRetryForTest

          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, errorMessage = Some(failureMessage),
              willRetry = !shouldAbortStage)
          } else {
            logDebug(s"Received fetch failure from $task, but it's from $failedStage which is no " +
              "longer running")
          }

          if (mapStage.rdd.isBarrier()) {
            // Mark all the map as broken in the map stage, to ensure retry all the tasks on
            // resubmitted stage attempt.
            // TODO: SPARK-35547: Clean all push-based shuffle metadata like merge enabled and
            // TODO: finalized as we are clearing all the merge results.
            mapOutputTracker.unregisterAllMapAndMergeOutput(shuffleId)
          } else if (mapIndex != -1) {
            // Mark the map whose fetch failed as broken in the map stage
            mapOutputTracker.unregisterMapOutput(shuffleId, mapIndex, bmAddress)
            if (pushBasedShuffleEnabled) {
              // Possibly unregister the merge result <shuffleId, reduceId>, if the FetchFailed
              // mapIndex is part of the merge result of <shuffleId, reduceId>
              mapOutputTracker.
                unregisterMergeResult(shuffleId, reduceId, bmAddress, Option(mapIndex))
            }
          } else {
            // Unregister the merge result of <shuffleId, reduceId> if there is a FetchFailed event
            // and is not a  MetaDataFetchException which is signified by bmAddress being null
            if (bmAddress != null &&
              bmAddress.executorId.equals(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)) {
              assert(pushBasedShuffleEnabled, "Push based shuffle expected to " +
                "be enabled when handling merge block fetch failure.")
              mapOutputTracker.
                unregisterMergeResult(shuffleId, reduceId, bmAddress, None)
            }
          }

          if (failedStage.rdd.isBarrier()) {
            failedStage match {
              case failedMapStage: ShuffleMapStage =>
                // Mark all the map as broken in the map stage, to ensure retry all the tasks on
                // resubmitted stage attempt.
                mapOutputTracker.unregisterAllMapAndMergeOutput(failedMapStage.shuffleDep.shuffleId)

              case failedResultStage: ResultStage =>
                // Abort the failed result stage since we may have committed output for some
                // partitions.
                val reason = "Could not recover from a failed barrier ResultStage. Most recent " +
                  s"failure reason: $failureMessage"
                abortStage(failedResultStage, reason, None)
            }
          }

          if (shouldAbortStage) {
            val abortMessage = if (disallowStageRetryForTest) {
              "Fetch failure will not retry stage due to testing config"
            } else {
              s"$failedStage (${failedStage.name}) has failed the maximum allowable number of " +
                s"times: $maxConsecutiveStageAttempts. Most recent failure reason:\n" +
                failureMessage
            }
            abortStage(failedStage, abortMessage, None)
          } else { // update failedStages and make sure a ResubmitFailedStages event is enqueued
            // TODO: Cancel running tasks in the failed stage -- cf. SPARK-17064
            val noResubmitEnqueued = !failedStages.contains(failedStage)
            failedStages += failedStage
            failedStages += mapStage
            if (noResubmitEnqueued) {
              // If the map stage is INDETERMINATE, which means the map tasks may return
              // different result when re-try, we need to re-try all the tasks of the failed
              // stage and its succeeding stages, because the input data will be changed after the
              // map tasks are re-tried.
              // Note that, if map stage is UNORDERED, we are fine. The shuffle partitioner is
              // guaranteed to be determinate, so the input data of the reducers will not change
              // even if the map tasks are re-tried.
              if (mapStage.isIndeterminate) {
                // It's a little tricky to find all the succeeding stages of `mapStage`, because
                // each stage only know its parents not children. Here we traverse the stages from
                // the leaf nodes (the result stages of active jobs), and rollback all the stages
                // in the stage chains that connect to the `mapStage`. To speed up the stage
                // traversing, we collect the stages to rollback first. If a stage needs to
                // rollback, all its succeeding stages need to rollback to.
                val stagesToRollback = HashSet[Stage](mapStage)

                def collectStagesToRollback(stageChain: List[Stage]): Unit = {
                  if (stagesToRollback.contains(stageChain.head)) {
                    stageChain.drop(1).foreach(s => stagesToRollback += s)
                  } else {
                    stageChain.head.parents.foreach { s =>
                      collectStagesToRollback(s :: stageChain)
                    }
                  }
                }

                def generateErrorMessage(stage: Stage): String = {
                  "A shuffle map stage with indeterminate output was failed and retried. " +
                    s"However, Spark cannot rollback the $stage to re-process the input data, " +
                    "and has to fail this job. Please eliminate the indeterminacy by " +
                    "checkpointing the RDD before repartition and try again."
                }

                activeJobs.foreach(job => collectStagesToRollback(job.finalStage :: Nil))

                // The stages will be rolled back after checking
                val rollingBackStages = HashSet[Stage](mapStage)
                stagesToRollback.foreach {
                  case mapStage: ShuffleMapStage =>
                    val numMissingPartitions = mapStage.findMissingPartitions().length
                    if (numMissingPartitions < mapStage.numTasks) {
                      if (sc.getConf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
                        val reason = "A shuffle map stage with indeterminate output was failed " +
                          "and retried. However, Spark can only do this while using the new " +
                          "shuffle block fetching protocol. Please check the config " +
                          "'spark.shuffle.useOldFetchProtocol', see more detail in " +
                          "SPARK-27665 and SPARK-25341."
                        abortStage(mapStage, reason, None)
                      } else {
                        rollingBackStages += mapStage
                      }
                    }

                  case resultStage: ResultStage if resultStage.activeJob.isDefined =>
                    val numMissingPartitions = resultStage.findMissingPartitions().length
                    if (numMissingPartitions < resultStage.numTasks) {
                      // TODO: support to rollback result tasks.
                      abortStage(resultStage, generateErrorMessage(resultStage), None)
                    }

                  case _ =>
                }
                logInfo(s"The shuffle map stage $mapStage with indeterminate output was failed, " +
                  s"we will roll back and rerun below stages which include itself and all its " +
                  s"indeterminate child stages: $rollingBackStages")
              }

              // We expect one executor failure to trigger many FetchFailures in rapid succession,
              // but all of those task failures can typically be handled by a single resubmission of
              // the failed stage.  We avoid flooding the scheduler's event queue with resubmit
              // messages by checking whether a resubmit is already in the event queue for the
              // failed stage.  If there is already a resubmit enqueued for a different failed
              // stage, that event would also be sufficient to handle the current failed stage, but
              // producing a resubmit for each failed stage makes debugging and logging a little
              // simpler while not producing an overwhelming number of scheduler events.
              logInfo(
                s"Resubmitting $mapStage (${mapStage.name}) and " +
                  s"$failedStage (${failedStage.name}) due to fetch failure"
              )
              messageScheduler.schedule(
                new Runnable {
                  override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
                },
                DAGScheduler.RESUBMIT_TIMEOUT,
                TimeUnit.MILLISECONDS
              )
            }
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            val externalShuffleServiceEnabled = env.blockManager.externalShuffleServiceEnabled
            val isHostDecommissioned = taskScheduler
              .getExecutorDecommissionState(bmAddress.executorId)
              .exists(_.workerHost.isDefined)

            // Shuffle output of all executors on host `bmAddress.host` may be lost if:
            // - External shuffle service is enabled, so we assume that all shuffle data on node is
            //   bad.
            // - Host is decommissioned, thus all executors on that host will die.
            val shuffleOutputOfEntireHostLost = externalShuffleServiceEnabled ||
              isHostDecommissioned
            val hostToUnregisterOutputs = if (shuffleOutputOfEntireHostLost
              && unRegisterOutputOnHostOnFetchFailure) {
              Some(bmAddress.host)
            } else {
              // Unregister shuffle data just for one executor (we don't have any
              // reason to believe shuffle data has been lost for the entire host).
              None
            }
            removeExecutorAndUnregisterOutputs(
              execId = bmAddress.executorId,
              fileLost = true,
              hostToUnregisterOutputs = hostToUnregisterOutputs,
              maybeEpoch = Some(task.epoch),
              // shuffleFileLostEpoch is ignored when a host is decommissioned because some
              // decommissioned executors on that host might have been removed before this fetch
              // failure and might have bumped up the shuffleFileLostEpoch. We ignore that, and
              // proceed with unconditional removal of shuffle outputs from all executors on that
              // host, including from those that we still haven't confirmed as lost due to heartbeat
              // delays.
              ignoreShuffleFileLostEpoch = isHostDecommissioned)
          }
        }

      case failure: TaskFailedReason if task.isBarrier =>
        // Also handle the task failed reasons here.
        failure match {
          case Resubmitted =>
            handleResubmittedFailure(task, stage)

          case _ => // Do nothing.
        }

        // Always fail the current stage and retry all the tasks when a barrier task fail.
        val failedStage = stageIdToStage(task.stageId)
        if (failedStage.latestInfo.attemptNumber != task.stageAttemptId) {
          logInfo(s"Ignoring task failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ${failedStage.latestInfo.attemptNumber}) running")
        } else {
          logInfo(s"Marking $failedStage (${failedStage.name}) as failed due to a barrier task " +
            "failed.")
          val message = s"Stage failed because barrier task $task finished unsuccessfully.\n" +
            failure.toErrorString
          try {
            // killAllTaskAttempts will fail if a SchedulerBackend does not implement killTask.
            val reason = s"Task $task from barrier stage $failedStage (${failedStage.name}) " +
              "failed."
            val job = jobIdToActiveJob.get(failedStage.firstJobId)
            val shouldInterrupt = job.exists(j => shouldInterruptTaskThread(j))
            taskScheduler.killAllTaskAttempts(stageId, shouldInterrupt, reason)
          } catch {
            case e: UnsupportedOperationException =>
              // Cannot continue with barrier stage if failed to cancel zombie barrier tasks.
              // TODO SPARK-24877 leave the zombie tasks and ignore their completion events.
              logWarning(s"Could not kill all tasks for stage $stageId", e)
              abortStage(failedStage, "Could not kill zombie barrier tasks for stage " +
                s"$failedStage (${failedStage.name})", Some(e))
          }
          markStageAsFinished(failedStage, Some(message))

          failedStage.failedAttemptIds.add(task.stageAttemptId)
          // TODO Refactor the failure handling logic to combine similar code with that of
          // FetchFailed.
          val shouldAbortStage =
            failedStage.failedAttemptIds.size >= maxConsecutiveStageAttempts ||
              disallowStageRetryForTest

          if (shouldAbortStage) {
            val abortMessage = if (disallowStageRetryForTest) {
              "Barrier stage will not retry stage due to testing config. Most recent failure " +
                s"reason: $message"
            } else {
              s"$failedStage (${failedStage.name}) has failed the maximum allowable number of " +
                s"times: $maxConsecutiveStageAttempts. Most recent failure reason: $message"
            }
            abortStage(failedStage, abortMessage, None)
          } else {
            failedStage match {
              case failedMapStage: ShuffleMapStage =>
                // Mark all the map as broken in the map stage, to ensure retry all the tasks on
                // resubmitted stage attempt.
                mapOutputTracker.unregisterAllMapAndMergeOutput(failedMapStage.shuffleDep.shuffleId)

              case failedResultStage: ResultStage =>
                // Abort the failed result stage since we may have committed output for some
                // partitions.
                val reason = "Could not recover from a failed barrier ResultStage. Most recent " +
                  s"failure reason: $message"
                abortStage(failedResultStage, reason, None)
            }
            // In case multiple task failures triggered for a single stage attempt, ensure we only
            // resubmit the failed stage once.
            val noResubmitEnqueued = !failedStages.contains(failedStage)
            failedStages += failedStage
            if (noResubmitEnqueued) {
              logInfo(s"Resubmitting $failedStage (${failedStage.name}) due to barrier stage " +
                "failure.")
              messageScheduler.schedule(new Runnable {
                override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
              }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
            }
          }
        }

      case Resubmitted =>
        handleResubmittedFailure(task, stage)

      case _: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case _: ExceptionFailure | _: TaskKilled =>
        // Nothing left to do, already handled above for accumulator updates.

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case _: ExecutorLostFailure | UnknownReason =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
  }

  /**
   * Whether executor is decommissioning or decommissioned.
   * Return true when:
   *  1. Waiting for decommission start
   *  2. Under decommission process
   *  3. Stopped or terminated after finishing decommission
   *  4. Under decommission process, then removed by driver with other reasons
   * Return false in case 3 and 4 when removed executors info are not retained.
   * The max size of removed executors is controlled by
   * spark.scheduler.maxRetainedRemovedExecutors
   */
  private[scheduler] def isExecutorDecommissioningOrDecommissioned(
      taskScheduler: TaskScheduler, bmAddress: BlockManagerId): Boolean = {
    if (bmAddress != null) {
      taskScheduler
        .getExecutorDecommissionState(bmAddress.executorId)
        .nonEmpty
    } else {
      false
    }
  }

  /**
   *
   * Schedules shuffle merge finalization.
   *
   * @param stage the stage to finalize shuffle merge
   * @param delay how long to wait before finalizing shuffle merge
   * @param registerMergeResults indicate whether DAGScheduler would register the received
   *                             MergeStatus with MapOutputTracker and wait to schedule the reduce
   *                             stage until MergeStatus have been received from all mergers or
   *                             reaches timeout. For very small shuffle, this could be set to
   *                             false to avoid impact to job runtime.
   */
  private[scheduler] def scheduleShuffleMergeFinalize(
      stage: ShuffleMapStage,
      delay: Long,
      registerMergeResults: Boolean = true): Unit = {
    val shuffleDep = stage.shuffleDep
    val scheduledTask: Option[ScheduledFuture[_]] = shuffleDep.getFinalizeTask
    scheduledTask match {
      case Some(task) =>
        // If we find an already scheduled task, check if the task has been triggered yet.
        // If it's already triggered, do nothing. Otherwise, cancel it and schedule a new
        // one for immediate execution. Note that we should get here only when
        // handleShufflePushCompleted schedules a finalize task after the shuffle map stage
        // completed earlier and scheduled a task with default delay.
        // The current task should be coming from handleShufflePushCompleted, thus the
        // delay should be 0 and registerMergeResults should be true.
        assert(delay == 0 && registerMergeResults)
        if (task.getDelay(TimeUnit.NANOSECONDS) > 0 && task.cancel(false)) {
          logInfo(s"$stage (${stage.name}) scheduled for finalizing shuffle merge immediately " +
            s"after cancelling previously scheduled task.")
          shuffleDep.setFinalizeTask(
            shuffleMergeFinalizeScheduler.schedule(
              new Runnable {
                override def run(): Unit = finalizeShuffleMerge(stage, registerMergeResults)
              },
              0,
              TimeUnit.SECONDS
            )
          )
        } else {
          logInfo(s"$stage (${stage.name}) existing scheduled task for finalizing shuffle merge" +
            s"would either be in-progress or finished. No need to schedule shuffle merge" +
            s" finalization again.")
        }
      case None =>
        // If no previous finalization task is scheduled, schedule the finalization task.
        logInfo(s"$stage (${stage.name}) scheduled for finalizing shuffle merge in $delay s")
        shuffleDep.setFinalizeTask(
          shuffleMergeFinalizeScheduler.schedule(
            new Runnable {
              override def run(): Unit = finalizeShuffleMerge(stage, registerMergeResults)
            },
            delay,
            TimeUnit.SECONDS
          )
        )
    }
  }

  /**
   * DAGScheduler notifies all the remote shuffle services chosen to serve shuffle merge request for
   * the given shuffle map stage to finalize the shuffle merge process for this shuffle. This is
   * invoked in a separate thread to reduce the impact on the DAGScheduler main thread, as the
   * scheduler might need to talk to 1000s of shuffle services to finalize shuffle merge.
   *
   * @param stage ShuffleMapStage to finalize shuffle merge for
   * @param registerMergeResults indicate whether DAGScheduler would register the received
   *                             MergeStatus with MapOutputTracker and wait to schedule the reduce
   *                             stage until MergeStatus have been received from all mergers or
   *                             reaches timeout. For very small shuffle, this could be set to
   *                             false to avoid impact to job runtime.
   */
  private[scheduler] def finalizeShuffleMerge(
      stage: ShuffleMapStage,
      registerMergeResults: Boolean = true): Unit = {
    logInfo(s"$stage (${stage.name}) finalizing the shuffle merge with registering merge " +
      s"results set to $registerMergeResults")
    val shuffleId = stage.shuffleDep.shuffleId
    val shuffleMergeId = stage.shuffleDep.shuffleMergeId
    val numMergers = stage.shuffleDep.getMergerLocs.length
    val results = (0 until numMergers).map(_ => SettableFuture.create[Boolean]())
    externalShuffleClient.foreach { shuffleClient =>
      val scheduledFutures =
        if (!registerMergeResults) {
          results.foreach(_.set(true))
          // Finalize in separate thread as shuffle merge is a no-op in this case
          stage.shuffleDep.getMergerLocs.map {
            case shuffleServiceLoc =>
              // Sends async request to shuffle service to finalize shuffle merge on that host.
              // Since merge statuses will not be registered in this case,
              // we pass a no-op listener.
              shuffleSendFinalizeRpcExecutor.submit(new Runnable() {
                override def run(): Unit = {
                  shuffleClient.finalizeShuffleMerge(shuffleServiceLoc.host,
                    shuffleServiceLoc.port, shuffleId, shuffleMergeId,
                    new MergeFinalizerListener {
                      override def onShuffleMergeSuccess(statuses: MergeStatuses): Unit = {
                      }

                      override def onShuffleMergeFailure(e: Throwable): Unit = {
                      }
                    })
                }
              })
          }
        } else {
          stage.shuffleDep.getMergerLocs.zipWithIndex.map {
            case (shuffleServiceLoc, index) =>
              // Sends async request to shuffle service to finalize shuffle merge on that host
              // TODO: SPARK-35536: Cancel finalizeShuffleMerge if the stage is cancelled
              // TODO: during shuffleMergeFinalizeWaitSec
              shuffleSendFinalizeRpcExecutor.submit(new Runnable() {
                override def run(): Unit = {
                  shuffleClient.finalizeShuffleMerge(shuffleServiceLoc.host,
                    shuffleServiceLoc.port, shuffleId, shuffleMergeId,
                    new MergeFinalizerListener {
                      override def onShuffleMergeSuccess(statuses: MergeStatuses): Unit = {
                        assert(shuffleId == statuses.shuffleId)
                        eventProcessLoop.post(RegisterMergeStatuses(stage, MergeStatus.
                          convertMergeStatusesToMergeStatusArr(statuses, shuffleServiceLoc)))
                        results(index).set(true)
                      }

                      override def onShuffleMergeFailure(e: Throwable): Unit = {
                        logWarning(s"Exception encountered when trying to finalize shuffle " +
                          s"merge on ${shuffleServiceLoc.host} for shuffle $shuffleId", e)
                        // Do not fail the future as this would cause dag scheduler to prematurely
                        // give up on waiting for merge results from the remaining shuffle services
                        // if one fails
                        results(index).set(false)
                      }
                    })
                }
              })
          }
        }
      // DAGScheduler only waits for a limited amount of time for the merge results.
      // It will attempt to submit the next stage(s) irrespective of whether merge results
      // from all shuffle services are received or not.
      var timedOut = false
      try {
        Futures.allAsList(results: _*).get(shuffleMergeResultsTimeoutSec, TimeUnit.SECONDS)
      } catch {
        case _: TimeoutException =>
          timedOut = true
          logInfo(s"Timed out on waiting for merge results from all " +
            s"$numMergers mergers for shuffle $shuffleId")
      } finally {
        if (timedOut || !registerMergeResults) {
          cancelFinalizeShuffleMergeFutures(scheduledFutures,
            if (timedOut) 0L else shuffleMergeResultsTimeoutSec)
        }
        eventProcessLoop.post(ShuffleMergeFinalized(stage))
      }
    }
  }

  private def cancelFinalizeShuffleMergeFutures(
      futures: Seq[JFutrue[_]],
      delayInSecs: Long): Unit = {

    def cancelFutures(): Unit = futures.foreach(_.cancel(true))

    if (delayInSecs > 0) {
      shuffleMergeFinalizeScheduler.schedule(new Runnable {
        override def run(): Unit = {
          cancelFutures()
        }
      }, delayInSecs, TimeUnit.SECONDS)
    } else {
      cancelFutures()
    }
  }

  private def processShuffleMapStageCompletion(shuffleStage: ShuffleMapStage): Unit = {
    markStageAsFinished(shuffleStage)
    logInfo("looking for newly runnable stages")
    logInfo("running: " + runningStages)
    logInfo("waiting: " + waitingStages)
    logInfo("failed: " + failedStages)

    // This call to increment the epoch may not be strictly necessary, but it is retained
    // for now in order to minimize the changes in behavior from an earlier version of the
    // code. This existing behavior of always incrementing the epoch following any
    // successful shuffle map stage completion may have benefits by causing unneeded
    // cached map outputs to be cleaned up earlier on executors. In the future we can
    // consider removing this call, but this will require some extra investigation.
    // See https://github.com/apache/spark/pull/17955/files#r117385673 for more details.
    mapOutputTracker.incrementEpoch()

    clearCacheLocs()

    if (!shuffleStage.isAvailable) {
      // Some tasks had failed; let's resubmit this shuffleStage.
      // TODO: Lower-level scheduler should also deal with this
      logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
        ") because some of its tasks had failed: " +
        shuffleStage.findMissingPartitions().mkString(", "))
      submitStage(shuffleStage)
    } else {
      markMapStageJobsAsFinished(shuffleStage)
      submitWaitingChildStages(shuffleStage)
    }
  }

  private[scheduler] def handleRegisterMergeStatuses(
      stage: ShuffleMapStage,
      mergeStatuses: Seq[(Int, MergeStatus)]): Unit = {
    // Register merge statuses if the stage is still running and shuffle merge is not finalized yet.
    // TODO: SPARK-35549: Currently merge statuses results which come after shuffle merge
    // TODO: is finalized is not registered.
    if (runningStages.contains(stage) && !stage.shuffleDep.isShuffleMergeFinalizedMarked) {
      mapOutputTracker.registerMergeResults(stage.shuffleDep.shuffleId, mergeStatuses)
    }
  }

  private[scheduler] def handleShuffleMergeFinalized(stage: ShuffleMapStage,
        shuffleMergeId: Int): Unit = {
    // Check if update is for the same merge id - finalization might have completed for an earlier
    // adaptive attempt while the stage might have failed/killed and shuffle id is getting
    // re-executing now.
    if (stage.shuffleDep.shuffleMergeId == shuffleMergeId) {
      // When it reaches here, there is a possibility that the stage will be resubmitted again
      // because of various reasons. Some of these could be:
      // a) Stage results are not available. All the tasks completed once so the
      // pendingPartitions is empty but due to an executor failure some of the map outputs are not
      // available any more, so the stage will be re-submitted.
      // b) Stage failed due to a task failure.
      // We should mark the stage as merged finalized irrespective of what state it is in.
      // This will prevent the push being enabled for the re-attempt.
      // Note: for indeterminate stages, this doesn't matter at all, since the merge finalization
      // related state is reset during the stage submission.
      stage.shuffleDep.markShuffleMergeFinalized()
      if (stage.pendingPartitions.isEmpty)
        if (runningStages.contains(stage)) {
          processShuffleMapStageCompletion(stage)
        } else if (stage.isIndeterminate) {
          // There are 2 possibilities here - stage is either cancelled or it will be resubmitted.
          // If this is an indeterminate stage which is cancelled, we unregister all its merge
          // results here just to free up some memory. If the indeterminate stage is resubmitted,
          // merge results are cleared again when the newer attempt is submitted.
          mapOutputTracker.unregisterAllMergeResult(stage.shuffleDep.shuffleId)
          // For determinate stages, which have completed merge finalization, we don't need to
          // unregister merge results - since the stage retry, or any other stage computing the
          // same shuffle id, can use it.
        }
    }
  }

  private[scheduler] def handleShufflePushCompleted(
      shuffleId: Int, shuffleMergeId: Int, mapIndex: Int): Unit = {
    shuffleIdToMapStage.get(shuffleId) match {
      case Some(mapStage) =>
        val shuffleDep = mapStage.shuffleDep
        // Only update shufflePushCompleted events for the current active stage map tasks.
        // This is required to prevent shuffle merge finalization by dangling tasks of a
        // previous attempt in the case of indeterminate stage.
        if (shuffleDep.shuffleMergeId == shuffleMergeId) {
          if (!shuffleDep.isShuffleMergeFinalizedMarked &&
            shuffleDep.incPushCompleted(mapIndex).toDouble / shuffleDep.rdd.partitions.length
              >= shufflePushMinRatio) {
            scheduleShuffleMergeFinalize(mapStage, delay = 0)
          }
        }
      case None =>
    }
  }

  private def handleResubmittedFailure(task: Task[_], stage: Stage): Unit = {
    logInfo(s"Resubmitted $task, so marking it as still running.")
    stage match {
      case sms: ShuffleMapStage =>
        sms.pendingPartitions += task.partitionId

      case _ =>
        throw SparkCoreErrors.sendResubmittedTaskStatusForShuffleMapStagesOnlyError()
    }
  }

  private[scheduler] def markMapStageJobsAsFinished(shuffleStage: ShuffleMapStage): Unit = {
    // Mark any map-stage jobs waiting on this stage as finished
    if (shuffleStage.isAvailable && shuffleStage.mapStageJobs.nonEmpty) {
      val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
      for (job <- shuffleStage.mapStageJobs) {
        markMapStageJobAsFinished(job, stats)
      }
    }
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using an external shuffle service), or the
   * entire Standalone worker is lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      workerHost: Option[String]): Unit = {
    // if the cluster manager explicitly tells us that the entire worker was lost, then
    // we know to unregister shuffle output.  (Note that "worker" specifically refers to the process
    // from a Standalone cluster, where the shuffle service lives in the Worker.)
    val fileLost = !sc.shuffleDriverComponents.supportsReliableStorage() &&
      (workerHost.isDefined || !env.blockManager.externalShuffleServiceEnabled)
    removeExecutorAndUnregisterOutputs(
      execId = execId,
      fileLost = fileLost,
      hostToUnregisterOutputs = workerHost,
      maybeEpoch = None)
  }

  /**
   * Handles removing an executor from the BlockManagerMaster as well as unregistering shuffle
   * outputs for the executor or optionally its host.
   *
   * @param execId executor to be removed
   * @param fileLost If true, indicates that we assume we've lost all shuffle blocks associated
   *   with the executor; this happens if the executor serves its own blocks (i.e., we're not
   *   using an external shuffle service), the entire Standalone worker is lost, or a FetchFailed
   *   occurred (in which case we presume all shuffle data related to this executor to be lost).
   * @param hostToUnregisterOutputs (optional) executor host if we're unregistering all the
   *   outputs on the host
   * @param maybeEpoch (optional) the epoch during which the failure was caught (this prevents
   *   reprocessing for follow-on fetch failures)
   */
  private def removeExecutorAndUnregisterOutputs(
      execId: String,
      fileLost: Boolean,
      hostToUnregisterOutputs: Option[String],
      maybeEpoch: Option[Long] = None,
      ignoreShuffleFileLostEpoch: Boolean = false): Unit = {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    logDebug(s"Considering removal of executor $execId; " +
      s"fileLost: $fileLost, currentEpoch: $currentEpoch")
    // Check if the execId is a shuffle push merger. We do not remove the executor if it is,
    // and only remove the outputs on the host.
    val isShuffleMerger = execId.equals(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)
    if (isShuffleMerger && pushBasedShuffleEnabled) {
      hostToUnregisterOutputs.foreach(
        host => blockManagerMaster.removeShufflePushMergerLocation(host))
    }
    if (!isShuffleMerger &&
      (!executorFailureEpoch.contains(execId) || executorFailureEpoch(execId) < currentEpoch)) {
      executorFailureEpoch(execId) = currentEpoch
      logInfo(s"Executor lost: $execId (epoch $currentEpoch)")
      if (pushBasedShuffleEnabled) {
        // Remove fetchFailed host in the shuffle push merger list for push based shuffle
        hostToUnregisterOutputs.foreach(
          host => blockManagerMaster.removeShufflePushMergerLocation(host))
      }
      blockManagerMaster.removeExecutor(execId)
      clearCacheLocs()
    }
    if (fileLost) {
      // When the fetch failure is for a merged shuffle chunk, ignoreShuffleFileLostEpoch is true
      // and so all the files will be removed.
      val remove = if (ignoreShuffleFileLostEpoch) {
        true
      } else if (!shuffleFileLostEpoch.contains(execId) ||
        shuffleFileLostEpoch(execId) < currentEpoch) {
        shuffleFileLostEpoch(execId) = currentEpoch
        true
      } else {
        false
      }
      if (remove) {
        hostToUnregisterOutputs match {
          case Some(host) =>
            logInfo(s"Shuffle files lost for host: $host (epoch $currentEpoch)")
            mapOutputTracker.removeOutputsOnHost(host)
          case None =>
            logInfo(s"Shuffle files lost for executor: $execId (epoch $currentEpoch)")
            mapOutputTracker.removeOutputsOnExecutor(execId)
        }
      }
    }
  }

  /**
   * Responds to a worker being removed. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use workerRemoved() to post a loss event from outside.
   *
   * We will assume that we've lost all shuffle blocks associated with the host if a worker is
   * removed, so we will remove them all from MapStatus.
   *
   * @param workerId identifier of the worker that is removed.
   * @param host host of the worker that is removed.
   * @param message the reason why the worker is removed.
   */
  private[scheduler] def handleWorkerRemoved(
      workerId: String,
      host: String,
      message: String): Unit = {
    logInfo("Shuffle files lost for worker %s on host %s".format(workerId, host))
    mapOutputTracker.removeOutputsOnHost(host)
    clearCacheLocs()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String): Unit = {
    // remove from executorFailureEpoch(execId) ?
    if (executorFailureEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      executorFailureEpoch -= execId
    }
    shuffleFileLostEpoch -= execId

    if (pushBasedShuffleEnabled) {
      // Only set merger locations for stages that are not yet finished and have empty mergers
      shuffleIdToMapStage.filter { case (_, stage) =>
        stage.shuffleDep.shuffleMergeAllowed && stage.shuffleDep.getMergerLocs.isEmpty &&
          runningStages.contains(stage)
      }.foreach { case (_, stage: ShuffleMapStage) =>
        configureShufflePushMergerLocations(stage)
        if (stage.shuffleDep.getMergerLocs.nonEmpty) {
          logInfo(s"Shuffle merge enabled adaptively for $stage with shuffle" +
            s" ${stage.shuffleDep.shuffleId} and shuffle merge" +
            s" ${stage.shuffleDep.shuffleMergeId} with ${stage.shuffleDep.getMergerLocs.size}" +
            s" merger locations")
        }
      }
    }
  }
  //取消stage
  private[scheduler] def handleStageCancellation(stageId: Int, reason: Option[String]): Unit = {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          val reasonStr = reason match {
            case Some(originalReason) =>
              s"because $originalReason"
            case None =>
              s"because Stage $stageId was cancelled"
          }
          handleJobCancellation(jobId, Option(reasonStr))
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
  }
  //取消job
  private[scheduler] def handleJobCancellation(jobId: Int, reason: Option[String]): Unit = {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason.getOrElse("")))
    }
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
    //用于标记一个阶段（Stage）为已完成，并将其从正在运行的阶段列表中移除。
  // 该方法根据阶段的执行结果（成功或失败）记录相关信息，并执行一些后续操作，如清除失败计数、更新输出协调器等。
  private def markStageAsFinished(
      stage: Stage,
      errorMessage: Option[String] = None,
      willRetry: Boolean = false): Unit = {
    //通过计算阶段的提交时间与当前时间的差来确定阶段的执行时间，单位为秒
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      //如果没有提供错误消息，表示阶段执行成功，记录成功日志，包括阶段名称和服务时间
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      //如果提供了错误消息，表示阶段执行失败，记录失败日志，包括阶段名称、服务时间和错误信息。
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(s"$stage (${stage.name}) failed in $serviceTime s due to ${errorMessage.get}")
    }
    //更新推送式 Shuffle 的阶段信息
    updateStageInfoForPushBasedShuffle(stage)
    //如果阶段不会被重试（即 willRetry 为 false），则调用 outputCommitCoordinator.stageEnd 方法，通知输出协调器阶段结束。
    if (!willRetry) {
      outputCommitCoordinator.stageEnd(stage.id)
    }
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
   * Called by the OutputCommitCoordinator to cancel stage due to data duplication may happen.
   */
  private[scheduler] def stageFailed(stageId: Int, reason: String): Unit = {
    eventProcessLoop.post(StageFailed(stageId, reason, None))
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(
      failedStage: Stage,
      reason: String,
      exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    updateStageInfoForPushBasedShuffle(failedStage)
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  private def updateStageInfoForPushBasedShuffle(stage: Stage): Unit = {
    // With adaptive shuffle mergers, StageInfo's
    // isShufflePushEnabled and shuffleMergers need to be updated at the end.
    stage match {
      case s: ShuffleMapStage =>
        stage.latestInfo.setPushBasedShuffleEnabled(s.shuffleDep.shuffleMergeEnabled)
        if (s.shuffleDep.shuffleMergeEnabled) {
          stage.latestInfo.setShuffleMergerCount(s.shuffleDep.getMergerLocs.size)
        }
      case _ =>
    }
  }

  /** Cancel all independent, running stages that are only used by this job. */
    //取消所有正在运行的独立阶段，这些阶段仅由当前作业使用。它会遍历作业相关的所有阶段，并根据需要终止正在运行的阶段
  private def cancelRunningIndependentStages(job: ActiveJob, reason: String): Boolean = {
    var ableToCancelStages = true
    //首先，方法会从 jobIdToStageIds 中查找与作业关联的阶段 ID。如果找不到相关阶段，会记录一个错误日志
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError(s"No stages registered for job ${job.jobId}")
    }
    stages.foreach { stageId =>
      //检查 stageIdToStage 中是否包含该阶段，并验证该阶段是否仅由当前作业使用。如果该阶段还被其他作业使用，则不会取消该阶段。
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This stage is only used by the job, so finish the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              //如果阶段正在运行，调用 taskScheduler.cancelTasks 方法来取消该阶段的任务。
              taskScheduler.cancelTasks(stageId, shouldInterruptTaskThread(job), reason)
              markStageAsFinished(stage, Some(reason))  //标记阶段完成
            } catch {
              case e: UnsupportedOperationException =>
                logWarning(s"Could not cancel tasks for stage $stageId", e)
                ableToCancelStages = false
            }
          }
        }
      }
    }
    ableToCancelStages
  }
  //取消job和stage
  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
      job: ActiveJob,
      failureReason: String,
      exception: Option[Throwable] = None): Unit = {
    if (cancelRunningIndependentStages(job, failureReason)) {
      // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
      // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
      cleanupStateForJobAndIndependentStages(job)
      val error = new SparkException(failureReason, exception.orNull)
      job.listener.jobFailed(error)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += stage.rdd
    def visit(rdd: RDD[_]): Unit = {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.prepend(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.prepend(narrowDep.rdd)
          }
        }
      }
    }
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.remove(0))
    }
    visitedRdds.contains(target.rdd)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.filter(_ != null).map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }

    Nil
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener. */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def stop(exitCode: Int = 0): Unit = {
    Utils.tryLogNonFatalError {
      messageScheduler.shutdownNow()
    }
    Utils.tryLogNonFatalError {
      shuffleMergeFinalizeScheduler.shutdownNow()
    }
    Utils.tryLogNonFatalError {
      eventProcessLoop.stop()
    }
    Utils.tryLogNonFatalError {
      taskScheduler.stop(exitCode)
    }
  }

  eventProcessLoop.start()
}
//事件处理循环类
private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, artifacts, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, artifacts,
        properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, artifacts, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, artifacts,
        properties)

    case StageCancelled(stageId, reason) =>
      dagScheduler.handleStageCancellation(stageId, reason)

    case JobCancelled(jobId, reason) =>
      dagScheduler.handleJobCancellation(jobId, reason)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case JobTagCancelled(tag) =>
      dagScheduler.handleJobTagCancelled(tag)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val workerHost = reason match {
        case ExecutorProcessLost(_, workerHost, _) => workerHost
        case ExecutorDecommission(workerHost, _) => workerHost
        case _ => None
      }
      dagScheduler.handleExecutorLost(execId, workerHost)

    case WorkerRemoved(workerId, host, message) =>
      dagScheduler.handleWorkerRemoved(workerId, host, message)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case SpeculativeTaskSubmitted(task, taskIndex) =>
      dagScheduler.handleSpeculativeTaskSubmitted(task, taskIndex)

    case UnschedulableTaskSetAdded(stageId, stageAttemptId) =>
      dagScheduler.handleUnschedulableTaskSetAdded(stageId, stageAttemptId)

    case UnschedulableTaskSetRemoved(stageId, stageAttemptId) =>
      dagScheduler.handleUnschedulableTaskSetRemoved(stageId, stageAttemptId)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case StageFailed(stageId, reason, exception) =>
      dagScheduler.handleStageFailed(stageId, reason, exception)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()

    case RegisterMergeStatuses(stage, mergeStatuses) =>
      dagScheduler.handleRegisterMergeStatuses(stage, mergeStatuses)

    case ShuffleMergeFinalized(stage) =>
      dagScheduler.handleShuffleMergeFinalized(stage, stage.shuffleDep.shuffleMergeId)

    case ShufflePushCompleted(shuffleId, shuffleMergeId, mapIndex) =>
      dagScheduler.handleShufflePushCompleted(shuffleId, shuffleMergeId, mapIndex)
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200

  // Number of consecutive stage attempts allowed before a stage is aborted
  val DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS = 4
}
