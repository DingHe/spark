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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE, REPARTITION_BY_COL, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A rule to coalesce the shuffle partitions based on the map output statistics, which can
 * avoid many small reduce tasks that hurt performance.
 */
// 核心目标： 避免产生大量小的 Reducer 任务。在 Shuffle 阶段完成后，它会利用 Map 端输出的统计信息（即每个 Reducer 分区的大小），将多个相邻的小分区动态地合并成数量更少、大小更合适的大分区
// 性能提升： 通过减少最终任务的数量，可以显著降低任务调度和管理的开销（Overhead），从而提高那些具有许多小分区的工作负载的性能。
case class CoalesceShufflePartitions(session: SparkSession) extends AQEShuffleReadRule {
  // 支持的 Shuffle 来源。
  // 定义了该规则支持优化的 Shuffle 交换类型，包括由于满足要求、按列重分区或平衡分区等原因产生的 Shuffle。
  // 排除了用户指定的分区数（REPARTITION_BY_NUM）和广播 Join 等场景
  override val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REPARTITION_BY_COL, REBALANCE_PARTITIONS_BY_NONE,
      REBALANCE_PARTITIONS_BY_COL)

  override def isSupported(shuffle: ShuffleExchangeLike): Boolean = {
    //等于单分区就没必要往下了
    shuffle.outputPartitioning != SinglePartition && super.isSupported(shuffle)
  }

  // 负责根据运行时收集到的 Shuffle 统计信息，计算新的分区合并方案，并更新查询计划（SparkPlan）
  override def apply(plan: SparkPlan): SparkPlan = {
    // 检查 Spark 配置中是否启用了 Shuffle 分区合并 (coalesceShufflePartitionsEnabled)
    if (!conf.coalesceShufflePartitionsEnabled) {
      return plan
    }

    // Ideally, this rule should simply coalesce partitions w.r.t. the target size specified by
    // ADVISORY_PARTITION_SIZE_IN_BYTES (default 64MB). To avoid perf regression in AQE, this
    // rule by default tries to maximize the parallelism and set the target size to
    // `total shuffle size / Spark default parallelism`. In case the `Spark default parallelism`
    // is too big, this rule also respect the minimum partition size specified by
    // COALESCE_PARTITIONS_MIN_PARTITION_SIZE (default 1MB).
    // For history reason, this rule also need to support the config
    // COALESCE_PARTITIONS_MIN_PARTITION_NUM. We should remove this config in the future.
    val minNumPartitions = conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM).getOrElse {
      // 检查是否启用了“并行度优先”策略
      if (conf.getConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST)) {
        // We fall back to Spark default parallelism if the minimum number of coalesced partitions
        // is not set, so to avoid perf regressions compared to no coalescing.
        session.sparkContext.defaultParallelism
      } else {
        // If we don't need to maximize the parallelism, we set `minPartitionNum` to 1, so that
        // the specified advisory partition size will be respected.
        1
      }
    }

    // Sub-plans under the Union/CartesianProduct/BroadcastHashJoin/BroadcastNestedLoopJoin
    // operator can be coalesced independently, so we can divide them into independent
    // "coalesce groups", and all shuffle stages within each group have to be coalesced together.
    // 收集查询计划中相互独立的 Shuffle Stage 组
    val coalesceGroups = collectCoalesceGroups(plan)

    // Divide minimum task parallelism among coalesce groups according to their data sizes.
    // 计算每个合并组应该分配的最小分区数量列表
    val minNumPartitionsByGroup = if (coalesceGroups.length == 1) {
      Seq(math.max(minNumPartitions, 1))
    } else {
      // 计算所有合并组的总数据大小
      val sizes =
        coalesceGroups.map(_.flatMap(_.shuffleStage.mapStats.map(_.bytesByPartitionId.sum)).sum)
      val totalSize = sizes.sum
      sizes.map { size =>
        val num = if (totalSize > 0) {
          math.round(minNumPartitions * 1.0 * size / totalSize)
        } else {
          minNumPartitions
        }
        math.max(num.toInt, 1)
      }
    }

    val specsMap = mutable.HashMap.empty[Int, Seq[ShufflePartitionSpec]]
    // Coalesce partitions for each coalesce group independently.
    coalesceGroups.zip(minNumPartitionsByGroup).foreach { case (shuffleStages, minNumPartitions) =>
      val advisoryTargetSize = advisoryPartitionSize(shuffleStages)
      val minPartitionSize = if (Utils.isTesting) {
        // In the tests, we usually set the target size to a very small value that is even smaller
        // than the default value of the min partition size. Here we also adjust the min partition
        // size to be not larger than 20% of the target size, so that the tests don't need to set
        // both configs all the time to check the coalescing behavior.
        conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE).min(advisoryTargetSize / 5)
      } else {
        conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE)
      }

      val newPartitionSpecs = ShufflePartitionsUtil.coalescePartitions(
        shuffleStages.map(_.shuffleStage.mapStats),
        shuffleStages.map(_.partitionSpecs),
        advisoryTargetSize = advisoryTargetSize,
        minNumPartitions = minNumPartitions,
        minPartitionSize = minPartitionSize)

      if (newPartitionSpecs.nonEmpty) {
        shuffleStages.zip(newPartitionSpecs).map { case (stageInfo, partSpecs) =>
          specsMap.put(stageInfo.shuffleStage.id, partSpecs)
        }
      }
    }

    if (specsMap.nonEmpty) {
      updateShuffleReads(plan, specsMap.toMap)
    } else {
      plan
    }
  }

  // data sources may request a particular advisory partition size for the final write stage
  // if it happens, the advisory partition size will be set in ShuffleQueryStageExec
  // only one shuffle stage is expected in such cases
  // 计算建议目标分区大小
  // 用于确定 Shuffle 分区合并的理想目标大小
  private def advisoryPartitionSize(shuffleStages: Seq[ShuffleStageInfo]): Long = {
    val defaultAdvisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    shuffleStages match {
      case Seq(stage) =>
        stage.shuffleStage.advisoryPartitionSize.getOrElse(defaultAdvisorySize)
      case _ =>
        defaultAdvisorySize
    }
  }

  /**
   * Gather all coalesce-able groups such that the shuffle stages in each child of a
   * Union/CartesianProduct/BroadcastHashJoin/BroadcastNestedLoopJoin operator are in their
   * independent groups if:
   * 1) all leaf nodes of this child are exchange stages; and
   * 2) all these shuffle stages support coalescing.
   */
  // 核心作用是在 Spark 物理执行计划中递归地寻找并组织可以进行分区合并（Coalescing）的 Shuffle 阶段集合（即合并组）
  // 一个“合并组”内的所有 Shuffle 阶段必须被合并成相同数量的最终分区，以确保下游算子（如 Join 或 Union）能正常工作，因为它们通常要求所有输入子节点具有相同的分区数。
  // Seq[Seq[ShuffleStageInfo]]。外层 Seq 是独立的合并组，内层 Seq 包含属于该组的所有 Shuffle 阶段信息
  private def collectCoalesceGroups(plan: SparkPlan): Seq[Seq[ShuffleStageInfo]] = plan match {
    // 情况 1：匹配已优化的 Shuffle 读取节点。
    // 如果匹配成功，则调用 collectShuffleStageInfos 收集该节点下的 Shuffle 信息（通常就是它自己），并将其作为一个独立的合并组返回
    case r @ AQEShuffleReadExec(q: ShuffleQueryStageExec, _) if isSupported(q.shuffle) =>
      Seq(collectShuffleStageInfos(r))
    // 情况 2：匹配一元（单子）操作符。
    case unary: UnaryExecNode => collectCoalesceGroups(unary.child)
    // 由于 Union 的每个子分支的分区数量是相互独立的，所以对每个子节点递归调用
    case union: UnionExec => union.children.flatMap(collectCoalesceGroups)
    // 情况 4：匹配笛卡尔积 Join。
    case join: CartesianProductExec => join.children.flatMap(collectCoalesceGroups)
    // Note that, `BroadcastQueryStageExec` is a valid case:
    // If a join has been optimized from shuffled join to broadcast join, then the one side is
    // `BroadcastQueryStageExec` and other side is `ShuffleQueryStageExec`. It can coalesce the
    // shuffle side as we do not expect broadcast exchange has same partition number.
    // 情况 5：匹配广播哈希 Join。
    case join: BroadcastHashJoinExec => join.children.flatMap(collectCoalesceGroups)
    // 情况 6：匹配广播嵌套循环 Join。
    case join: BroadcastNestedLoopJoinExec => join.children.flatMap(collectCoalesceGroups)
    // If not all leaf nodes are exchange query stages, it's not safe to reduce the number of
    // shuffle partitions, because we may break the assumption that all children of a spark plan
    // have same number of output partitions.
    // 情况 7：匹配一个完整的、由 Exchange 构成的子树。
    // 只有当一个子树完全由 Shuffle 阶段终止时，才能安全地减少其分区数，因为下游操作符可能依赖于分区数的一致性。
    case p if p.collectLeaves().forall(_.isInstanceOf[ExchangeQueryStageExec]) =>
      val shuffleStages = collectShuffleStageInfos(p)
      // ShuffleExchanges introduced by repartition do not support partition number change.
      // We change the number of partitions only if all the ShuffleExchanges support it.
      if (shuffleStages.forall(s => isSupported(s.shuffleStage.shuffle))) {
        Seq(shuffleStages)
      } else {
        Seq.empty
      }
    case _ => Seq.empty
  }
  // 常用于递归地遍历一个Spark查询计划SparkPlan，找出其中所有与Shuffle相关的阶段信息（ShuffleStageInfo）。
  // 它是 AQE中进行分区合并优化Coalesce的基础
  private def collectShuffleStageInfos(plan: SparkPlan): Seq[ShuffleStageInfo] = plan match {
    case ShuffleStageInfo(stage, specs) => Seq(new ShuffleStageInfo(stage, specs))
    // flatMap 会将每个递归调用返回的 Seq【ShuffleStageInfo】列表连接（flatten）成一个单一的结果Seq返回
    case _ => plan.children.flatMap(collectShuffleStageInfos)
  }

  // 作用是递归地遍历一个逻辑或物理查询计划（SparkPlan），并根据预先计算好的分区规范（specsMap）来更新或替换计划中的Shuffle读取节点
  // specsMap 键（Int）是 Shuffle 阶段的 ID（stage.id），值（Seq[ShufflePartitionSpec]）是该 Shuffle 阶段对应的新的分区规范列表
  private def updateShuffleReads(
      plan: SparkPlan, specsMap: Map[Int, Seq[ShufflePartitionSpec]]): SparkPlan = plan match {
    // Even for shuffle exchange whose input RDD has 0 partition, we should still update its
    // `partitionStartIndices`, so that all the leaf shuffles in a stage have the same
    // number of output partitions.
    case ShuffleStageInfo(stage, _) =>
      specsMap.get(stage.id).map { specs =>
        AQEShuffleReadExec(stage, specs)
      }.getOrElse(plan)
    case other => other.mapChildren(updateShuffleReads(_, specsMap))
  }
}
// 统一表示 两种不同的Shuffle读取场景
// 常规 Shuffle阶段：只包含ShuffleQueryStageExec 本身
// AQE优化后的Shuffle读取：包含ShuffleQueryStageExec以及 经过AQE优化后生成的新的分区规范（ShufflePartitionSpec列表），这些规范可能指示了分区合并（Coalesce）或拆分（Skew处理）
private class ShuffleStageInfo(
    val shuffleStage: ShuffleQueryStageExec,
    val partitionSpecs: Option[Seq[ShufflePartitionSpec]])

private object ShuffleStageInfo {
  def unapply(plan: SparkPlan)
  : Option[(ShuffleQueryStageExec, Option[Seq[ShufflePartitionSpec]])] = plan match {
    case stage: ShuffleQueryStageExec =>
      Some((stage, None))
    case AQEShuffleReadExec(s: ShuffleQueryStageExec, partitionSpecs) =>
      Some((s, Some(partitionSpecs)))
    case _ => None
  }
}
