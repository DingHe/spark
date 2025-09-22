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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ResultStages apply a function on some partitions of an RDD to compute the result of an action.
 * The ResultStage object captures the function to execute, `func`, which will be applied to each
 * partition, and the set of partition IDs, `partitions`. Some stages may not run on all partitions
 * of the RDD, for actions like first() and lookup().
 */
//用于执行与 Spark 动作（如 count()、save() 等）相关的计算。
private[spark] class ResultStage(
    id: Int, //ResultStage 的唯一标识符，表示该阶段的 ID
    rdd: RDD[_], //与当前阶段关联的 RDD，代表当前阶段的计算基础
    val func: (TaskContext, Iterator[_]) => _, //表示要在每个分区上执行的计算，Iterator[_] 是该分区数据的迭代器
    val partitions: Array[Int],//当前阶段需要计算的分区 ID 数组
    parents: List[Stage], //当前阶段的父阶段列表，
    firstJobId: Int, //首次提交该阶段的作业 ID
    callSite: CallSite,
    resourceProfileId: Int)
  extends Stage(id, rdd, partitions.length, parents, firstJobId, callSite, resourceProfileId) {

  /**
   * The active job for this result stage. Will be empty if the job has already finished
   * (e.g., because the job was cancelled).
   */
  private[this] var _activeJob: Option[ActiveJob] = None  //表示与当前阶段相关的活跃作业

  def activeJob: Option[ActiveJob] = _activeJob

  def setActiveJob(job: ActiveJob): Unit = {
    _activeJob = Option(job)
  }

  def removeActiveJob(): Unit = {
    _activeJob = None
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   * 它通过检查作业中的每个分区是否已经完成来判断哪些分区仍然需要计算。
   * This can only be called when there is an active job.
   */
  override def findMissingPartitions(): Seq[Int] = {
    val job = activeJob.get
    (0 until job.numPartitions).filter(id => !job.finished(id))
  }

  override def toString: String = "ResultStage " + id
}
