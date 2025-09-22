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

import java.util.Properties
import javax.annotation.Nullable

import scala.collection.Map

import com.fasterxml.jackson.annotation.JsonTypeInfo

import org.apache.spark.TaskEndReason
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{BlockManagerId, BlockUpdatedInfo}

//所有事件的基类
//像一个信封，里面封装了关于 Spark 应用程序状态变化的所有信息。这个基类的存在，
// 使得 Spark 能够以统一、可扩展的方式来管理和处理各种运行时事件，从而支持实时监控、性能调优和历史记录等功能
@DeveloperApi
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
trait SparkListenerEvent {
  /* Whether output this event to the event log */
  //控制该事件是否应该被写入事件日志
  protected[spark] def logEvent: Boolean = true
}
//阶段提交的事件
@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null)
  extends SparkListenerEvent
//阶段完成的事件
@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent
//任务开始的事件
@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)
  extends SparkListenerEvent
//用于标记一个任务开始远程获取其结果
//在 Spark 应用程序中，当一个任务在执行器上完成计算后，如果其结果（例如，collect() 操作的结果）需要传回驱动程序，这个过程可能不会立即完成。
// 该事件在任务结果开始从执行器传输到驱动程序时触发，通知监听器任务已经进入“正在获取结果”的状态
// 这个事件对于监控任务的生命周期非常重要，因为它将任务的生命周期细化为三个主要阶段：
// 启动（Start）：SparkListenerTaskStart
// 获取结果（Getting Result）：SparkListenerTaskGettingResult
// 结束（End）：SparkListenerTaskEnd
@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent

//通知监听器一个推测性任务（speculative task）被提交了
@DeveloperApi
case class SparkListenerSpeculativeTaskSubmitted(
    stageId: Int,//表示推测性任务所属的阶段 ID
    stageAttemptId: Int = 0) //表示推测性任务所属的阶段尝试 ID。默认值为 0
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // didn't stored taskIndex
  private var _taskIndex: Int = -1 //用于存储推测性任务在其任务集中的索引
  private var _partitionId: Int = -1 //用于存储推测性任务所处理的 RDD 分区 ID

  def taskIndex: Int = _taskIndex
  def partitionId: Int = _partitionId

  def this(stageId: Int, stageAttemptId: Int, taskIndex: Int, partitionId: Int) = {
    this(stageId, stageAttemptId)
    _partitionId = partitionId
    _taskIndex = taskIndex
  }
}
//任务结束的事件
@DeveloperApi
case class SparkListenerTaskEnd(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    reason: TaskEndReason,
    taskInfo: TaskInfo,
    taskExecutorMetrics: ExecutorMetrics,
    // may be null if the task has failed
    @Nullable taskMetrics: TaskMetrics)
  extends SparkListenerEvent

//Job被提交事件
@DeveloperApi
case class SparkListenerJobStart(
    jobId: Int,
    time: Long,
    stageInfos: Seq[StageInfo],
    properties: Properties = null)
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // only stored stageIds and not StageInfos:
  val stageIds: Seq[Int] = stageInfos.map(_.stageId)
}
//Job结束事件
@DeveloperApi
case class SparkListenerJobEnd(
    jobId: Int,
    time: Long,
    jobResult: JobResult)
  extends SparkListenerEvent

//环境变量更新事件
@DeveloperApi
case class SparkListenerEnvironmentUpdate(
    environmentDetails: Map[String, collection.Seq[(String, String)]])
  extends SparkListenerEvent

//块管理器增加事件
@DeveloperApi
case class SparkListenerBlockManagerAdded(
    time: Long,
    blockManagerId: BlockManagerId,
    maxMem: Long,
    maxOnHeapMem: Option[Long] = None,
    maxOffHeapMem: Option[Long] = None) extends SparkListenerEvent {
}
//块管理器移除事件
@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
  extends SparkListenerEvent
//Rdd缓存移除事件
@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent
//执行器添加事件
@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo)
  extends SparkListenerEvent
//执行器移除事件
@DeveloperApi
case class SparkListenerExecutorRemoved(time: Long, executorId: String, reason: String)
  extends SparkListenerEvent

@DeveloperApi
@deprecated("use SparkListenerExecutorExcluded instead", "3.1.0")
case class SparkListenerExecutorBlacklisted(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcluded(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerExecutorExcludedForStage instead", "3.1.0")
@DeveloperApi
case class SparkListenerExecutorBlacklistedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcludedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeExcludedForStage instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeBlacklistedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcludedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerExecutorUnexcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerExecutorUnblacklisted(time: Long, executorId: String)
  extends SparkListenerEvent


@DeveloperApi
case class SparkListenerExecutorUnexcluded(time: Long, executorId: String)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeExcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeBlacklisted(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcluded(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeUnexcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeUnblacklisted(time: Long, hostId: String)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeUnexcluded(time: Long, hostId: String)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetAdded(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetRemoved(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends SparkListenerEvent

@DeveloperApi
@Since("3.2.0")
case class SparkListenerMiscellaneousProcessAdded(time: Long, processId: String,
    info: MiscellaneousProcessDetails) extends SparkListenerEvent

/**
 * Periodic updates from executors.
 * @param execId executor id
 * @param accumUpdates sequence of (taskId, stageId, stageAttemptId, accumUpdates)
 * @param executorUpdates executor level per-stage metrics updates
 *
 * @since 3.1.0
 */
@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(
    execId: String,
    accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])],
    executorUpdates: Map[(Int, Int), ExecutorMetrics] = Map.empty)
  extends SparkListenerEvent

/**
 * Peak metric values for the executor for the stage, written to the history log at stage
 * completion.
 * @param execId executor id
 * @param stageId stage id
 * @param stageAttemptId stage attempt
 * @param executorMetrics executor level metrics peak values
 */
@DeveloperApi
case class SparkListenerStageExecutorMetrics(
    execId: String,
    stageId: Int,
    stageAttemptId: Int,
    executorMetrics: ExecutorMetrics)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationStart(
    appName: String,
    appId: Option[String],
    time: Long,
    sparkUser: String,
    appAttemptId: Option[String],
    driverLogs: Option[Map[String, String]] = None,
    driverAttributes: Option[Map[String, String]] = None) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationEnd(time: Long) extends SparkListenerEvent

/**
 * An internal class that describes the metadata of an event log.
 */
@DeveloperApi
case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerResourceProfileAdded(resourceProfile: ResourceProfile)
  extends SparkListenerEvent

/**
 * Interface for listening to events from the Spark scheduler. Most applications should probably
 * extend SparkListener or SparkFirehoseListener directly, rather than implementing this class.
 *
 * Note that this is an internal interface which might change in different Spark releases.
 */
//Spark 内部用于监听调度器事件的接口，它定义了 Spark 应用程序生命周期中各种事件的回调方法
//开发者可以实时捕获和处理 Spark 应用程序的运行状态，例如作业（Job）、阶段（Stage）、任务（Task）的启动、完成或失败，以及执行器（Executor）和块管理器（Block Manager）的添加或移除等事件
private[spark] trait SparkListenerInterface {

  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  //当一个阶段完成（成功或失败）时被调用。参数 stageCompleted 包含了阶段的统计信息，如任务的执行时间、输入/输出数据量等
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

  /**
   * Called when a stage is submitted
   */
  //当一个阶段（Stage）被提交时被调用。一个作业由一个或多个阶段组成，该方法提供了阶段 ID、任务数量等信息
  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit

  /**
   * Called when a task starts
   */
  //当一个任务（Task）开始执行时被调用。任务是 Spark 工作的最小单位，该方法提供了任务 ID、执行器 ID 等信息
  def onTaskStart(taskStart: SparkListenerTaskStart): Unit

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  //当一个任务开始远程获取其结果时被调用。这通常发生在任务结果需要从远程执行器传输到驱动程序时
  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit

  /**
   * Called when a task ends
   */
  //当一个任务结束时被调用。参数 taskEnd 包含了任务的详细结果，例如任务的状态（成功、失败、跳过）和性能指标
  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

  /**
   * Called when a job starts
   */
  //当一个 Spark 作业（Job）启动时被调用。一个作业通常对应一个 action（如 collect()、count() 等），该方法提供了作业 ID、关联的阶段列表等信息
  def onJobStart(jobStart: SparkListenerJobStart): Unit

  /**
   * Called when a job ends
   */
  //当一个 Spark 作业结束时被调用，无论成功还是失败。参数 jobEnd 包含作业的结束时间、结果状态等
  def onJobEnd(jobEnd: SparkListenerJobEnd): Unit

  /**
   * Called when environment properties have been updated
   */
  //当环境属性（如 Spark 配置）发生更新时被调用
  def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit

  /**
   * Called when a new block manager has joined
   */
  //当一个新的块管理器（Block Manager）加入时被调用。每个执行器都有一个块管理器
  def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit

  /**
   * Called when an existing block manager has been removed
   */
  //当一个块管理器被移除时被调用
  def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  //当应用程序手动对一个 RDD 调用 unpersist() 方法时被调用
  def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit

  /**
   * Called when the application starts
   */
  //当 Spark 应用程序启动时被调用，参数 applicationStart 包含了应用程序的详细信息，例如应用程序 ID、名称和启动时间等
  def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit

  /**
   * Called when the application ends
   */
  //当 Spark 应用程序结束时被调用，参数 applicationEnd 包含了应用程序的结束时间等信息
  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  //当驱动程序从执行器的心跳中接收到任务指标更新时被调用
  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit

  /**
   * Called with the peak memory metrics for a given (executor, stage) combination. Note that this
   * is only present when reading from the event log (as in the history server), and is never
   * called in a live application.
   */
  //在从事件日志（例如在历史服务器上）读取数据时被调用，用于报告给定执行器在特定阶段的峰值内存指标
  def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit

  /**
   * Called when the driver registers a new executor.
   */
  //当一个新的执行器（Executor）加入 Spark 应用程序时被调用
  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit

  /**
   * Called when the driver removes an executor.
   */
  //当一个执行器被移除时被调用。这可能是由于执行器故障、动态资源分配或应用程序结束
  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit

  /**
   * Called when the driver excludes an executor for a Spark application.
   */
  @deprecated("use onExecutorExcluded instead", "3.1.0")
  def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit

  /**
   * Called when the driver excludes an executor for a Spark application.
   */
  //当驱动程序将某个执行器排除（不再分配任务）时被调用
  def onExecutorExcluded(executorExcluded: SparkListenerExecutorExcluded): Unit

  /**
   * Called when the driver excludes an executor for a stage.
   */
  @deprecated("use onExecutorExcludedForStage instead", "3.1.0")
  def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit

  /**
   * Called when the driver excludes an executor for a stage.
   */
  //当驱动程序将某个执行器仅对当前阶段排除时被调用
  def onExecutorExcludedForStage(
      executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   */
  @deprecated("use onNodeExcludedForStage instead", "3.1.0")
  def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   */
  //当驱动程序将某个节点（node）仅对当前阶段排除时被调用
  def onNodeExcludedForStage(nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   */
  @deprecated("use onExecutorUnexcluded instead", "3.1.0")
  def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   */
  //当一个之前被排除的执行器被重新启用时被调用
  def onExecutorUnexcluded(executorUnexcluded: SparkListenerExecutorUnexcluded): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   */
  @deprecated("use onNodeExcluded instead", "3.1.0")
  def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   */
  //当驱动程序将某个节点排除时被调用
  def onNodeExcluded(nodeExcluded: SparkListenerNodeExcluded): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   */
  @deprecated("use onNodeUnexcluded instead", "3.1.0")
  def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   */
  //当一个之前被排除的节点被重新启用时被调用
  def onNodeUnexcluded(nodeUnexcluded: SparkListenerNodeUnexcluded): Unit

  /**
   * Called when a taskset becomes unschedulable due to exludeOnFailure and dynamic allocation
   * is enabled.
   */
  //当一个任务集因为 excludeOnFailure 策略和动态资源分配而变得不可调度时被调用
  def onUnschedulableTaskSetAdded(
      unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit

  /**
   * Called when an unschedulable taskset becomes schedulable and dynamic allocation
   * is enabled.
   */
  //当一个不可调度的任务集再次变为可调度时被调用
  def onUnschedulableTaskSetRemoved(
      unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit

  /**
   * Called when the driver receives a block update info.
   */
  //当驱动程序接收到块更新信息时被调用，例如一个缓存的 RDD 分区被更新
  def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit

  /**
   * Called when a speculative task is submitted
   */
  //当一个推测执行任务被提交时被调用
  def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit

  /**
   * Called when other events like SQL-specific events are posted.
   */
  //个通用的回调方法，用于处理非标准事件，例如 SQL 相关的事件
  def onOtherEvent(event: SparkListenerEvent): Unit

  /**
   * Called when a Resource Profile is added to the manager.
   */
  //当资源配置文件被添加到管理器时被调用
  def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit
}


/**
 * :: DeveloperApi ::
 * A default implementation for `SparkListenerInterface` that has no-op implementations for
 * all callbacks.
 *
 * Note that this is an internal interface which might change in different Spark releases.
 */
// 默认的空的监听器实现
@DeveloperApi
abstract class SparkListener extends SparkListenerInterface {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = { }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = { }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = { }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = { }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = { }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = { }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = { }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onStageExecutorMetrics(
      executorMetrics: SparkListenerStageExecutorMetrics): Unit = { }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = { }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = { }

  override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }
  override def onExecutorExcluded(
      executorExcluded: SparkListenerExecutorExcluded): Unit = { }

  override def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = { }
  override def onExecutorExcludedForStage(
      executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit = { }

  override def onNodeBlacklistedForStage(
      nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = { }
  override def onNodeExcludedForStage(
      nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit = { }

  override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = { }
  override def onExecutorUnexcluded(
      executorUnexcluded: SparkListenerExecutorUnexcluded): Unit = { }

  override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = { }
  override def onNodeExcluded(
      nodeExcluded: SparkListenerNodeExcluded): Unit = { }

  override def onNodeUnblacklisted(
      nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }
  override def onNodeUnexcluded(
      nodeUnexcluded: SparkListenerNodeUnexcluded): Unit = { }

  override def onUnschedulableTaskSetAdded(
      unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit = { }

  override def onUnschedulableTaskSetRemoved(
      unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit = { }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

  override def onSpeculativeTaskSubmitted(
      speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = { }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = { }
}
