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

import scala.collection.mutable.Map

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

/**
 * Low-level task scheduler interface, currently implemented exclusively by
 * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
 */
//接收来自 DAGScheduler 的任务集合，并负责将任务发送到集群、执行它们、处理任务失败重试以及处理拖延任务。
private[spark] trait TaskScheduler {

  private val appId = "spark-application-" + System.currentTimeMillis  //用于生成应用程序的 ID

  def rootPool: Pool  //任务调度器的根池

  def schedulingMode: SchedulingMode

  def start(): Unit  //启动任务调度器

  // Invoked after system has successfully initialized (typically in spark context).
  // Yarn uses this to bootstrap allocation of resources based on preferred locations,
  // wait for executor registrations, etc.
  def postStartHook(): Unit = { }   //通常用于在系统成功初始化之后做一些额外的工作

  // Disconnect from the cluster.
  def stop(exitCode: Int = 0): Unit  //停止任务调度器。可以选择传入退出码，表示任务调度器停止时的状态

  // Submit a sequence of tasks to run.
  def submitTasks(taskSet: TaskSet): Unit //提交一组任务执行。taskSet 是一组要执行的任务，通常是由 DAGScheduler 传递过来的。

  // Kill all the tasks in a stage and fail the stage and all the jobs that depend on the stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  def cancelTasks(stageId: Int, interruptThread: Boolean, reason: String): Unit  //取消某个 stage 中的所有任务，并使该 stage 和依赖该 stage 的所有作业失败。

  /**
   * Kills a task attempt.
   * Throw UnsupportedOperationException if the backend doesn't support kill a task.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean  //杀死某个任务尝试。

  // Kill all the running task attempts in a stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit  //杀死某个 stage 中所有正在运行的任务尝试

  // Notify the corresponding `TaskSetManager`s of the stage, that a partition has already completed
  // and they can skip running tasks for it.
  // 通知对应的 TaskSetManager 某个分区的任务已经完成，可以跳过对该分区的任务执行。
  def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit

  // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  //获取集群的默认并行度，作为任务分配时的参考
  def defaultParallelism(): Int

  /**
   * Update metrics for in-progress tasks and executor metrics, and let the master know that the
   * BlockManager is still alive. Return true if the driver knows about the given block manager.
   * Otherwise, return false, indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: Map[(Int, Int), ExecutorMetrics]): Boolean

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Process a decommissioning executor.
   */
  def executorDecommission(executorId: String, decommissionInfo: ExecutorDecommissionInfo): Unit

  /**
   * If an executor is decommissioned, return its corresponding decommission info
   */
  def getExecutorDecommissionState(executorId: String): Option[ExecutorDecommissionState]

  /**
   * Process a lost executor
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * Process a removed worker
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit

  /**
   * Get an application's attempt ID associated with the job.
   *
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]

}
