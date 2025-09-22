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

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.RDDInfo

/**
 * :: DeveloperApi ::
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 */
//封装 Spark 阶段（Stage） 的详细信息，并将其从调度器传递给监听器（SparkListeners）
@DeveloperApi
class StageInfo(
    val stageId: Int,//阶段的唯一标识符
    private val attemptId: Int,//当前阶段尝试的唯一标识符
    val name: String,//阶段的名称，通常由用户代码中的转换操作（如 map、reduce）生成，用于在 UI 上标识
    val numTasks: Int,//该阶段总共包含的任务（Task）数量
    val rddInfos: Seq[RDDInfo],//与该阶段相关联的 RDD 列表。这包括该阶段的最终 RDD 以及其所有通过窄依赖连接的祖先 RDD
    val parentIds: Seq[Int],//当前阶段的父阶段 ID 列表。一个阶段可能依赖于一个或多个父阶段的输出（通常是 Shuffle）
    val details: String,//提供关于该阶段的额外详细描述，通常是代码调用栈的字符串表示，便于调试
    val taskMetrics: TaskMetrics = null,
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
    private[spark] val shuffleDepId: Option[Int] = None,
    val resourceProfileId: Int, //与此阶段关联的资源配置文件 ID，用于支持 Spark 中的资源细粒度管理（如 GPU 资源）
    private[spark] var isShufflePushEnabled: Boolean = false,//表示该阶段是否启用了基于推送的 Shuffle（push-based shuffle）。这是一种高级的 Shuffle 机制，可提高性能
    private[spark] var shuffleMergerCount: Int = 0) {//表示该阶段的 Shuffle 合并器（merger）数量，与基于推送的 Shuffle 相关
  /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
  var submissionTime: Option[Long] = None  //阶段被提交到 TaskScheduler 的时间戳（毫秒）
  /** Time when the stage completed or when the stage was cancelled. */
  var completionTime: Option[Long] = None  //阶段完成或被取消的时间戳（毫秒）
  /** If the stage failed, the reason why. */
  var failureReason: Option[String] = None  //如果阶段失败，这里存储失败的原因字符串

  /**
   * Terminal values of accumulables updated during this stage, including all the user-defined
   * accumulators.
   */
  //一个哈希表，存储该阶段中所有累加器（Accumulable）的最终值。键是累加器 ID，值是 AccumulableInfo 对象，包含累加器的名称、类型和值
  val accumulables = HashMap[Long, AccumulableInfo]()
  //，用于标记阶段失败。它会设置 failureReason 和 completionTime
  def stageFailed(reason: String): Unit = {
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
  }

  // This would just be the second constructor arg, except we need to maintain this method
  // with parentheses for compatibility
  //返回当前阶段尝试的 ID。这个方法是为了兼容性而保留的，与直接访问 attemptId 属性等效
  def attemptNumber(): Int = attemptId

  //返回阶段的状态字符串（"succeeded"、"failed" 或 "running"）。这方便了 Spark UI 或日志输出
  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }
  //用于设置 Shuffle 合并器的数量
  private[spark] def setShuffleMergerCount(mergers: Int): Unit = {
    shuffleMergerCount = mergers
  }
  //用于设置是否启用了基于推送的 Shuffle
  private[spark] def setPushBasedShuffleEnabled(pushBasedShuffleEnabled: Boolean): Unit = {
    isShufflePushEnabled = pushBasedShuffleEnabled
  }
}

private[spark] object StageInfo {
  /**
   * Construct a StageInfo from a Stage.
   *
   * Each Stage is associated with one or many RDDs, with the boundary of a Stage marked by
   * shuffle dependencies. Therefore, all ancestor RDDs related to this Stage's RDD through a
   * sequence of narrow dependencies should also be associated with this Stage.
   */
    //用于从一个内部 Stage 对象（org.apache.spark.scheduler.Stage）创建一个 StageInfo 实例
  def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskMetrics: TaskMetrics = null,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
      resourceProfileId: Int
    ): StageInfo = {
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
    val shuffleDepId = stage match {
      case sms: ShuffleMapStage => Option(sms.shuffleDep).map(_.shuffleId)
      case _ => None
    }
    new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskMetrics,
      taskLocalityPreferences,
      shuffleDepId,
      resourceProfileId,
      false,
      0)
  }
}
