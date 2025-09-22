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

import org.apache.spark.TaskState
import org.apache.spark.TaskState.TaskState
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.errors.SparkCoreErrors

/**
 * :: DeveloperApi ::
 * Information about a running task attempt inside a TaskSet.
 */
// 封装 Spark 任务（Task） 尝试的详细信息。
// 在 Spark 应用程序中，一个阶段（Stage）由多个可以并行执行的任务组成。
// TaskInfo 记录了每个任务的运行时元数据，例如任务 ID、它所属的执行器和主机、启动时间、以及其当前状态等
@DeveloperApi
class TaskInfo(
    //任务的全局唯一标识符，用于在整个 Spark 应用程序中唯一地识别一个任务
    val taskId: Long,
    /**
     * The index of this task within its task set. Not necessarily the same as the ID of the RDD
     * partition that the task is computing.
     */
    //任务在其所属的任务集（TaskSet） 中的索引。这个索引不一定等于它要计算的 RDD 分区 ID
    val index: Int,
    //任务尝试的次数。如果一个任务失败，Spark 会尝试重试，每次重试都会增加这个值
    val attemptNumber: Int,
    /**
     * The actual RDD partition ID in this task.
     * The ID of the RDD partition is always same across task attempts.
     * This will be -1 for historical data, and available for all applications since Spark 3.3.
     */
    //任务要计算的 RDD 分区 ID。从 Spark 3.3 开始引入，用于更好地追溯任务与其处理的数据分区之间的关系
    val partitionId: Int,
    //任务在执行器上启动的时间戳（毫秒）
    val launchTime: Long,
    //执行该任务的执行器（Executor） 的 ID
    val executorId: String,
    //执行该任务的主机名称
    val host: String,
    //任务的数据本地性级别，表示任务数据与计算任务之间的物理距离
    val taskLocality: TaskLocality.TaskLocality,
    //指示该任务是否是推测性执行（speculative execution）的产物
    val speculative: Boolean) {

  /**
   * This api doesn't contains partitionId, please use the new api.
   * Remain it for backward compatibility before Spark 3.3.
   */
  def this(
      taskId: Long,
      index: Int,
      attemptNumber: Int,
      launchTime: Long,
      executorId: String,
      host: String,
      taskLocality: TaskLocality.TaskLocality,
      speculative: Boolean) = {
    this(taskId, index, attemptNumber, -1, launchTime, executorId, host, taskLocality, speculative)
  }

  /**
   * The time when the task started remotely getting the result. Will not be set if the
   * task result was sent immediately when the task finished (as opposed to sending an
   * IndirectTaskResult and later fetching the result from the block manager).
   */
  //任务开始远程获取结果的时间戳。这通常发生在任务完成执行后，需要将结果从执行器发送回驱动程序时
  var gettingResultTime: Long = 0

  /**
   * Intermediate updates to accumulables during this task. Note that it is valid for the same
   * accumulable to be updated multiple times in a single task or for two accumulables with the
   * same name but different IDs to exist in a task.
   */
  def accumulables: Seq[AccumulableInfo] = _accumulables
  //包含任务执行过程中更新的累加器（Accumulable） 信息
  private[this] var _accumulables: Seq[AccumulableInfo] = Nil

  private[spark] def setAccumulables(newAccumulables: Seq[AccumulableInfo]): Unit = {
    _accumulables = newAccumulables
  }

  /**
   * The time when the task has completed successfully (including the time to remotely fetch
   * results, if necessary).
   */
  //任务完成的时间戳（无论是成功、失败还是被杀死）
  var finishTime: Long = 0
  //一个布尔值，表示任务是否失败
  var failed = false
  //一个布尔值，表示任务是否被杀死
  var killed = false
  //表示任务是否正在启动中
  var launching = true
  //用于标记任务开始获取结果，并设置相应的时间戳
  private[spark] def markGettingResult(time: Long): Unit = {
    gettingResultTime = time
  }
  //用于标记任务完成，并设置完成时间和最终状态（成功、失败或被杀死）
  private[spark] def markFinished(state: TaskState, time: Long): Unit = {
    // finishTime should be set larger than 0, otherwise "finished" below will return false.
    assert(time > 0)
    finishTime = time
    failed = state == TaskState.FAILED
    killed = state == TaskState.KILLED
  }
  //用于将 launching 状态设置为 false，表示任务已成功启动
  private[spark] def launchSucceeded(): Unit = {
    launching = false
  }
  //指示任务是否处于“获取结果”状态
  def gettingResult: Boolean = gettingResultTime != 0
  //指示任务是否已完成
  def finished: Boolean = finishTime != 0
  //指示任务是否成功完成
  def successful: Boolean = finished && !failed && !killed
  //指示任务是否正在运行（尚未完成）
  def running: Boolean = !finished

  def status: String = {
    if (running) {
      if (gettingResult) {
        "GET RESULT"
      } else {
        "RUNNING"
      }
    } else if (failed) {
      "FAILED"
    } else if (killed) {
      "KILLED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
  }

  def id: String = s"$index.$attemptNumber"
  //计算并返回任务从启动到完成所花费的总时间（以毫秒为单位）
  def duration: Long = {
    if (!finished) {
      throw SparkCoreErrors.durationCalledOnUnfinishedTaskError()
    } else {
      finishTime - launchTime
    }
  }
  //计算任务从启动到现在已经运行的时间
  private[spark] def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
