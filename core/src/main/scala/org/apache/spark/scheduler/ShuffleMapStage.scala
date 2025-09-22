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

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages are intermediate stages in the execution DAG that produce data for a shuffle.
 * They occur right before each shuffle operation, and might contain multiple pipelined operations
 * before that (e.g. map and filter). When executed, they save map output files that can later be
 * fetched by reduce tasks. The `shuffleDep` field describes the shuffle each stage is part of,
 * and variables like `outputLocs` and `numAvailableOutputs` track how many map outputs are ready.
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
 */
//表示一个 Map 阶段，该阶段进行 Shuffle 操作前的数据处理。
// 它主要负责生成 Shuffle 操作的中间结果，并将其保存为 Map 输出文件，供后续的 Reduce 阶段使用
private[spark] class ShuffleMapStage(
    id: Int, //是 ShuffleMapStage 的唯一标识符。每个阶段都有一个唯一的 ID，用于区分不同的阶段
    rdd: RDD[_], //表示当前 ShuffleMapStage 操作的底层 RDD
    numTasks: Int, //表示该阶段的任务数量
    parents: List[Stage], //当前阶段依赖的前置阶段列表
    firstJobId: Int, //表示提交当前阶段的第一个作业的 ID
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _], //描述了当前阶段的 Shuffle 操作依赖关系
    mapOutputTrackerMaster: MapOutputTrackerMaster, //管理当前阶段的 Shuffle 输出。它跟踪该阶段的 Map 输出是否可用，并协调不同执行器之间的数据获取。
    resourceProfileId: Int)
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite, resourceProfileId) {

  private[this] var _mapStageJobs: List[ActiveJob] = Nil  //表示与当前 ShuffleMapStage 相关的所有活跃作业的列表

  /**
   * Partitions that either haven't yet been computed, or that were computed on an executor
   * that has since been lost, so should be re-computed.  This variable is used by the
   * DAGScheduler to determine when a stage has completed. Task successes in both the active
   * attempt for the stage or in earlier attempts for this stage can cause partition ids to get
   * removed from pendingPartitions. As a result, this variable may be inconsistent with the pending
   * tasks in the TaskSetManager for the active attempt for the stage (the partitions stored here
   * will always be a subset of the partitions that the TaskSetManager thinks are pending).
   */
  val pendingPartitions = new HashSet[Int] //录的是当前 ShuffleMapStage 中尚未计算的分区 ID，或者曾经计算过但由于执行器丢失等原因需要重新计算的分区

  override def toString: String = "ShuffleMapStage " + id

  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /** Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /** Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /** 已经输出结果的分区数
   * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   */
  def numAvailableOutputs: Int = mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId)

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   */
  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  override def findMissingPartitions(): Seq[Int] = {
    mapOutputTrackerMaster
      .findMissingPartitions(shuffleDep.shuffleId)
      .getOrElse(0 until numPartitions)
  }
}
