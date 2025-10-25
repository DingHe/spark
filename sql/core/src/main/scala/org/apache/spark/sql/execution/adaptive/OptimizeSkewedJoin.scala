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

package org.apache.spark.sql.execution.adaptive

import scala.collection.mutable

import org.apache.commons.io.FileUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements, ValidateRequirements}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A rule to optimize skewed joins to avoid straggler tasks whose share of data are significantly
 * larger than those of the rest of the tasks.
 *
 * The general idea is to divide each skew partition into smaller partitions and replicate its
 * matching partition on the other side of the join so that they can run in parallel tasks.
 * Note that when matching partitions from the left side and the right side both have skew,
 * it will become a cartesian product of splits from left and right joining together.
 *
 * For example, assume the Sort-Merge join has 4 partitions:
 * left:  [L1, L2, L3, L4]
 * right: [R1, R2, R3, R4]
 *
 * Let's say L2, L4 and R3, R4 are skewed, and each of them get split into 2 sub-partitions. This
 * is scheduled to run 4 tasks at the beginning: (L1, R1), (L2, R2), (L3, R3), (L4, R4).
 * This rule expands it to 9 tasks to increase parallelism:
 * (L1, R1),
 * (L2-1, R2), (L2-2, R2),
 * (L3, R3-1), (L3, R3-2),
 * (L4-1, R4-1), (L4-2, R4-1), (L4-1, R4-2), (L4-2, R4-2)
 */
// 检测并优化“数据倾斜”的 join（主要针对 SortMergeJoin / ShuffledHashJoin），通过将倾斜的 shuffle partition 切分并对端复制这些切分，扩展并行度，从而避免单个 task 成为 straggler（过慢）
// 找到“倾斜的 partition”；
// 把倾斜 partition 切成若干子范围（split）；
case class OptimizeSkewedJoin(ensureRequirements: EnsureRequirements)
  extends Rule[SparkPlan] {

  /**
   * A partition is considered as a skewed partition if its size is larger than the median
   * partition size * SKEW_JOIN_SKEWED_PARTITION_FACTOR and also larger than
   * SKEW_JOIN_SKEWED_PARTITION_THRESHOLD. Thus we pick the larger one as the skew threshold.
   */
  // 计算把 partition 判定为“倾斜（skewed）”的阈值（bytes）
  // medianSize：给定分区大小数组的中位数（来自 MapOutputStatistics）
  // SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR：乘数因子（例如 3 表示比中位数大 3 倍就可能倾斜）
  def getSkewThreshold(medianSize: Long): Long = {
    conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD).max(
      (medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR)).toLong)
  }

  /**
   * The goal of skew join optimization is to make the data distribution more even. The target size
   * to split skewed partitions is the average size of non-skewed partition, or the
   * advisory partition size if avg size is smaller than it.
   */
  // 确定用于切分倾斜 partition 的“目标子分区大小” —— 即把倾斜 partition 切到每个子分区大约这个大小。
  // 如果没有非倾斜分区，退回到 advisorySize；否则取 advisorySize 与非倾斜分区平均值的最大值。
  private def targetSize(sizes: Array[Long], skewThreshold: Long): Long = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val nonSkewSizes = sizes.filter(_ <= skewThreshold)
    if (nonSkewSizes.isEmpty) {
      advisorySize
    } else {
      math.max(advisorySize, nonSkewSizes.sum / nonSkewSizes.length)
    }
  }
  // 判断根据 join 类型，是否允许对左侧/右侧分区做 split 优化
  private def canSplitLeftSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def canSplitRightSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }
  // 格式化输出当前分区大小的统计信息（中位数 / 最大 / 最小 / 平均），用于 logDebug 打印，帮助调试与监控。
  private def getSizeInfo(medianSize: Long, sizes: Array[Long]): String = {
    s"median size: $medianSize, max size: ${sizes.max}, min size: ${sizes.min}, avg size: " +
      sizes.sum / sizes.length
  }

  /*
   * This method aim to optimize the skewed join with the following steps:
   * 1. Check whether the shuffle partition is skewed based on the median size
   *    and the skewed partition threshold in origin shuffled join (smj and shj).
   * 2. Assuming partition0 is skewed in left side, and it has 5 mappers (Map0, Map1...Map4).
   *    And we may split the 5 Mappers into 3 mapper ranges [(Map0, Map1), (Map2, Map3), (Map4)]
   *    based on the map size and the max split number.
   * 3. Wrap the join left child with a special shuffle read that loads each mapper range with one
   *    task, so total 3 tasks.
   * 4. Wrap the join right child with a special shuffle read that loads partition0 3 times by
   *    3 tasks separately.
   */
  // 目标是检测并优化发生数据倾斜的 Shuffle Join（例如 SortMergeJoin 或 ShuffledHashJoin）
  // 通过分析左右两侧 Shuffle 阶段的统计信息，确定哪些分区是倾斜的，然后为这些倾斜分区生成新的、更细粒度的 Shuffle 读取规范（ShufflePartitionSpec），从而将一个大的倾斜任务拆分成多个小任务并行执行
  // 接受左右两侧的 Shuffle 阶段（ShuffleQueryStageExec）和 JoinType 作为输入
  private def tryOptimizeJoinChildren(
      left: ShuffleQueryStageExec,
      right: ShuffleQueryStageExec,
      joinType: JoinType): Option[(SparkPlan, SparkPlan)] = {
    // 如果根据 Join 类型，左右两侧都不允许进行倾斜优化拆分（例如，某些外部 Join 类型），则直接返回 None，不进行后续处理
    val canSplitLeft = canSplitLeftSide(joinType)
    val canSplitRight = canSplitRightSide(joinType)
    if (!canSplitLeft && !canSplitRight) return None

    val leftSizes = left.mapStats.get.bytesByPartitionId
    val rightSizes = right.mapStats.get.bytesByPartitionId
    // 检查并确保左右两侧 Shuffle 阶段的分区数量必须相等，这是基于 Join 的前提。
    assert(leftSizes.length == rightSizes.length)
    val numPartitions = leftSizes.length
    // We use the median size of the original shuffle partitions to detect skewed partitions.
    val leftMedSize = Utils.median(leftSizes, false)
    val rightMedSize = Utils.median(rightSizes, false)
    logDebug(
      s"""
         |Optimizing skewed join.
         |Left side partitions size info:
         |${getSizeInfo(leftMedSize, leftSizes)}
         |Right side partitions size info:
         |${getSizeInfo(rightMedSize, rightSizes)}
      """.stripMargin)

    // 根据左侧分区大小的中位数计算出倾斜分区大小的阈值。大于此阈值的分区被视为倾斜。
    val leftSkewThreshold = getSkewThreshold(leftMedSize)
    val rightSkewThreshold = getSkewThreshold(rightMedSize)
    // 计算理想的分区目标大小。这个目标大小通常是非倾斜分区的平均大小，或者是一个合理的上限
    val leftTargetSize = targetSize(leftSizes, leftSkewThreshold)
    val rightTargetSize = targetSize(rightSizes, rightSkewThreshold)

    // 可变的数组缓冲区，用于存储优化后左侧 RDD 的所有 ShufflePartitionSpec
    val leftSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    val rightSidePartitions = mutable.ArrayBuffer.empty[ShufflePartitionSpec]
    var numSkewedLeft = 0
    var numSkewedRight = 0
    for (partitionIndex <- 0 until numPartitions) {

      val leftSize = leftSizes(partitionIndex)
      // 判断左侧是否可拆分 且 当前分区大小大于左侧的倾斜阈值
      val isLeftSkew = canSplitLeft && leftSize > leftSkewThreshold
      val rightSize = rightSizes(partitionIndex)
      val isRightSkew = canSplitRight && rightSize > rightSkewThreshold
      // 如果左侧不倾斜，则创建一个标准的 CoalescedPartitionSpec，表示该分区作为一个整体（从 partitionIndex 到 partitionIndex + 1）被读取
      val leftNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, leftSize))
      val rightNoSkewPartitionSpec =
        Seq(CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, rightSize))
      // 处理左侧分区（倾斜/非倾斜）
      val leftParts = if (isLeftSkew) {
        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          left.mapStats.get.shuffleId, partitionIndex, leftTargetSize)
        if (skewSpecs.isDefined) {
          logDebug(s"Left side partition $partitionIndex " +
            s"(${FileUtils.byteCountToDisplaySize(leftSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedLeft += 1
        }
        skewSpecs.getOrElse(leftNoSkewPartitionSpec)
      } else {
        leftNoSkewPartitionSpec
      }
      // 处理右侧分区（倾斜/非倾斜）
      val rightParts = if (isRightSkew) {
        val skewSpecs = ShufflePartitionsUtil.createSkewPartitionSpecs(
          right.mapStats.get.shuffleId, partitionIndex, rightTargetSize)
        if (skewSpecs.isDefined) {
          logDebug(s"Right side partition $partitionIndex " +
            s"(${FileUtils.byteCountToDisplaySize(rightSize)}) is skewed, " +
            s"split it into ${skewSpecs.get.length} parts.")
          numSkewedRight += 1
        }
        skewSpecs.getOrElse(rightNoSkewPartitionSpec)
      } else {
        rightNoSkewPartitionSpec
      }
      // 生成新的分区对（笛卡尔积）
      for {
        leftSidePartition <- leftParts
        rightSidePartition <- rightParts
      } {
        leftSidePartitions += leftSidePartition
        rightSidePartitions += rightSidePartition
      }
    }
    logDebug(s"number of skewed partitions: left $numSkewedLeft, right $numSkewedRight")
    // 构建并返回优化后的 Join 子节点。
    if (numSkewedLeft > 0 || numSkewedRight > 0) {
      Some((
        SkewJoinChildWrapper(AQEShuffleReadExec(left, leftSidePartitions.toSeq)),
        SkewJoinChildWrapper(AQEShuffleReadExec(right, rightSidePartitions.toSeq))
      ))
    } else {
      None
    }
  }
  // 作用是在物理执行计划中找到符合条件的 Join 节点（SortMergeJoinExec 或 ShuffledHashJoinExec），然后尝试对其子节点进行数据倾斜优化
  // 对计划树进行自底向上（bottom-up）的遍历和模式匹配转换
  def optimizeSkewJoin(plan: SparkPlan): SparkPlan = plan.transformUp {
    case smj @ SortMergeJoinExec(_, _, joinType, _,
        s1 @ SortExec(_, _, ShuffleStage(left: ShuffleQueryStageExec), _),
        s2 @ SortExec(_, _, ShuffleStage(right: ShuffleQueryStageExec), _), false) =>
      tryOptimizeJoinChildren(left, right, joinType).map {
        case (newLeft, newRight) =>
          smj.copy(
            left = s1.copy(child = newLeft), right = s2.copy(child = newRight), isSkewJoin = true)
      }.getOrElse(smj)

    case shj @ ShuffledHashJoinExec(_, _, joinType, _, _,
        ShuffleStage(left: ShuffleQueryStageExec),
        ShuffleStage(right: ShuffleQueryStageExec), false) =>
      tryOptimizeJoinChildren(left, right, joinType).map {
        case (newLeft, newRight) =>
          shj.copy(left = newLeft, right = newRight, isSkewJoin = true)
      }.getOrElse(shj)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SKEW_JOIN_ENABLED)) {
      return plan
    }

    // We try to optimize every skewed sort-merge/shuffle-hash joins in the query plan. If this
    // introduces extra shuffles, we give up the optimization and return the original query plan, or
    // accept the extra shuffles if the force-apply config is true.
    // TODO: It's possible that only one skewed join in the query plan leads to extra shuffles and
    //       we only need to skip optimizing that join. We should make the strategy smarter here.
    val optimized = optimizeSkewJoin(plan)
    val requirementSatisfied = if (ensureRequirements.requiredDistribution.isDefined) {
      ValidateRequirements.validate(optimized, ensureRequirements.requiredDistribution.get)
    } else {
      ValidateRequirements.validate(optimized)
    }
    if (requirementSatisfied) {
      optimized.transform {
        case SkewJoinChildWrapper(child) => child
      }
    } else if (conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN)) {
      ensureRequirements.apply(optimized).transform {
        case SkewJoinChildWrapper(child) => child
      }
    } else {
      plan
    }
  }

  object ShuffleStage {
    def unapply(plan: SparkPlan): Option[ShuffleQueryStageExec] = plan match {
      case s: ShuffleQueryStageExec if s.isMaterialized && s.mapStats.isDefined &&
        s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS =>
        Some(s)
      case _ => None
    }
  }
}

// After optimizing skew joins, we need to run EnsureRequirements again to add necessary shuffles
// caused by skew join optimization. However, this shouldn't apply to the sub-plan under skew join,
// as it's guaranteed to satisfy distribution requirement.
case class SkewJoinChildWrapper(plan: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering
}
