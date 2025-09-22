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

import scala.collection.mutable.HashSet

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.util.CallSite

/**
 * A stage is a set of parallel tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
 * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
 * track the nodes that each output partition is on.
 *
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
 * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
 * The latest one will be accessible through latestInfo.
 *
 * @param id Unique stage ID
 * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
 *   on, while for a result stage, it's the target RDD that we ran an action on
 * @param numTasks Total number of tasks in stage; result stages in particular may not need to
 *   compute all partitions, e.g. for first(), lookup(), and take().
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
 * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
 * @param callSite Location in the user program associated with this stage: either where the target
 *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
 */
//Stage 表示一个包含多个并行任务的阶段，这些任务计算相同的功能，并且共享相同的 shuffle 依赖关系
private[scheduler] abstract class Stage(
    val id: Int, //每个阶段的唯一 ID
    val rdd: RDD[_], //该阶段执行的 RDD
    val numTasks: Int, //该阶段总共有多少个任务
    val parents: List[Stage], //当前阶段依赖的父阶段列表（即 shuffle 依赖）
    val firstJobId: Int, //首次提交该阶段的作业 ID
    val callSite: CallSite,
    val resourceProfileId: Int) //该阶段使用的资源配置 ID
  extends Logging {

  val numPartitions = rdd.partitions.length //当前阶段 RDD 的分区数

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int] //该阶段所属的作业 ID 集合。一个阶段可以属于多个作业。

  /** The ID to use for the next new attempt for this stage. */
  private var nextAttemptId: Int = 0 //下一个尝试的阶段 ID
  private[scheduler] def getNextAttemptId: Int = nextAttemptId

  val name: String = callSite.shortForm
  val details: String = callSite.longForm

  /**
   * Pointer to the [[StageInfo]] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   */
  private var _latestInfo: StageInfo =
    StageInfo.fromStage(this, nextAttemptId, resourceProfileId = resourceProfileId)

  /**
   * Set of stage attempt IDs that have failed. We keep track of these failures in order to avoid
   * endless retries if a stage keeps failing.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
   */
  val failedAttemptIds = new HashSet[Int] //存储失败的阶段尝试 ID

  private[scheduler] def clearFailures() : Unit = {
    failedAttemptIds.clear()
  }
  //创建当前阶段的新尝试，并生成新的 StageInfo 对象。每次阶段尝试失败时，会通过此方法创建新的阶段尝试，并为其分配新的 nextAttemptId
  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID. */
  def makeNewStageAttempt(
      numPartitionsToCompute: Int, //指定需要计算的分区数量
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = { //任务的位置偏好，表示任务应该尽量在哪些节点上执行
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences,
      resourceProfileId = resourceProfileId)
    nextAttemptId += 1
  }

  /** Forward the nextAttemptId if skipped and get visited for the first time. */
  def increaseAttemptIdOnFirstSkip(): Unit = {
    if (nextAttemptId == 0) {
      nextAttemptId = 1
    }
  }

  /** Returns the StageInfo for the most recent attempt for this stage. */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }
  //返回需要计算的分区 ID 列表。这些是缺失的分区，表示这些分区的任务尚未执行。
  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  def findMissingPartitions(): Seq[Int]
  //判断当前阶段的 RDD 是否是非确定性的
  def isIndeterminate: Boolean = {
    rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE
  }
}
