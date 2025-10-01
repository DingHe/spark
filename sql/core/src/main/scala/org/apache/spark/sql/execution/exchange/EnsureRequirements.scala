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

package org.apache.spark.sql.execution.exchange

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[ShuffleExchangeExec]] Operators where required.  Also ensure that
 * the input partition ordering requirements are met.
 *
 * @param optimizeOutRepartition A flag to indicate that if this rule should optimize out
 *                               user-specified repartition shuffles or not. This is mostly true,
 *                               but can be false in AQE when AQE optimization may change the plan
 *                               output partitioning and need to retain the user-specified
 *                               repartition shuffles in the plan.
 * @param requiredDistribution The root required distribution we should ensure. This value is used
 *                             in AQE in case we change final stage output partitioning.
 */
// EnsureRequirements 是 Spark SQL 优化器中的一条物理规则（Rule[SparkPlan]），它在将逻辑计划转换为最终的物理执行计划的过程中起着核心的桥梁作用
//核心职责：
//满足数据分布（Distribution）要求： 确保每个物理操作符（如 Join、Aggregate）的输入数据分区方式（outputPartitioning）满足其所需的分布要求（requiredChildDistribution）。
// 如果要求不满足，它会插入 ShuffleExchangeExec（进行数据重分区）或 BroadcastExchangeExec（进行数据广播）
//满足排序（Ordering）要求： 确保输入数据满足所需的排序顺序（requiredChildOrdering）。如果要求不满足，它会插入 SortExec 节点
//优化 Join 键顺序： 调整 Join 操作（如 SortMergeJoinExec 或 ShuffledHashJoinExec）的连接键顺序，使其与子节点的现有分区顺序（如果兼容）相匹配，以避免不必要的 Shuffle 或 Sort
//优化 Shuffle 重复性： 移除由用户显式指定但实际上并不必要的重复 Shuffle 操作（例如，数据已经按相同的键分区）
case class EnsureRequirements(
    optimizeOutRepartition: Boolean = true, //优化移除 Repartition,默认为 true。如果为 true，Spark 会尝试移除由用户（如 df.repartition(...)）显式添加的、但其分区方式与子节点输出分区方式语义相等的 ShuffleExchangeExec 节点，以减少不必要的 Shuffle
    requiredDistribution: Option[Distribution] = None) //根节点分布要求,默认为 None。主要用于 AQE (Adaptive Query Execution) 场景。它可以在整个计划转换完成后，为最终的根节点添加一个额外的分布要求（通常是 SinglePartition 或特定的哈希分区），确保最终输出满足特定的分布要求
  extends Rule[SparkPlan] {
  //负责检查并强制其父操作符的所有子节点满足其所需的数据分布（Distribution） 和排序（Ordering） 要求，并在必要时插入 Exchange 或 Sort 物理操作符
  private def ensureDistributionAndOrdering(
      parent: Option[SparkPlan], // 当前物理节点
      originalChildren: Seq[SparkPlan], //当前节点的所有子节点，表示需要处理的物理执行计划
      requiredChildDistributions: Seq[Distribution], //每个子节点需要满足的分布要求，如 ClusteredDistribution、BroadcastDistribution、UnspecifiedDistribution 等
      requiredChildOrderings: Seq[Seq[SortOrder]],//每个子节点需要满足的排序要求，主要用于确保输出数据的排序符合需求
      shuffleOrigin: ShuffleOrigin): Seq[SparkPlan] = { //表示 Shuffle 操作的来源，可能来自于显式的 Repartition 操作或隐式的 EnsureRequirements
    assert(requiredChildDistributions.length == originalChildren.length)
    assert(requiredChildOrderings.length == originalChildren.length)
    // Ensure that the operator's children satisfy their output distribution requirements.
    //将子节点和它们的分布要求配对，并映射生成一组新的 children 序列
    var children = originalChildren.zip(requiredChildDistributions).map {
      //如果子节点的当前输出分区方案 (outputPartitioning) 已经满足所需分布 (distribution.satisfies(distribution))，则不做处理
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      //如果所需的分布是 BroadcastDistribution（广播），无论当前分区状态如何，插入一个 BroadcastExchangeExec 节点，将子节点的数据广播出去
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
      //如果不满足分布要求，且不是广播，则需要进行 Shuffle
      case (child, distribution) =>
        val numPartitions = distribution.requiredNumPartitions
          .getOrElse(conf.numShufflePartitions)
        ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child, shuffleOrigin)
    }

    // Get the indexes of children which have specified distribution requirements and need to be
    // co-partitioned.
    //获取需协同分区索引
    //找出所有需要 ClusteredDistribution（基于键的分区）的子节点的索引。
    val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
      case (_: ClusteredDistribution, _) => true
      case _ => false
    }.map(_._2)

    // Special case: if all sides of the join are single partition and it's physical size less than
    // or equal spark.sql.maxSinglePartitionBytes.
    //检查所有需要协同分区的子节点是否都已经是 SinglePartition，且其逻辑大小小于配置的最大单分区大小。如果满足，则无需进行复杂 Shuffle，优先保持单分区
    val preferSinglePartition = childrenIndexes.forall { i =>
      children(i).outputPartitioning == SinglePartition &&
        children(i).logicalLink
          .forall(_.stats.sizeInBytes <= conf.getConf(SQLConf.MAX_SINGLE_PARTITION_BYTES))
    }

    // If there are more than one children, we'll need to check partitioning & distribution of them
    // and see if extra shuffles are necessary.
    // 如果有多于一个子节点需要协同分区，且不能应用单分区优化，则开始执行复杂的协同分区逻辑。
    if (childrenIndexes.length > 1 && !preferSinglePartition) {
      //对每个需要 ClusteredDistribution 的子节点，生成其对应的 ShuffleSpec，用于描述如何进行 Shuffle
      val specs = childrenIndexes.map(i => {
        val requiredDist = requiredChildDistributions(i)
        assert(requiredDist.isInstanceOf[ClusteredDistribution],
          s"Expected ClusteredDistribution but found ${requiredDist.getClass.getSimpleName}")
        i -> children(i).outputPartitioning.createShuffleSpec(
          requiredDist.asInstanceOf[ClusteredDistribution])
      }).toMap

      // Find out the shuffle spec that gives better parallelism. Currently this is done by
      // picking the spec with the largest number of partitions.
      //
      // NOTE: this is not optimal for the case when there are more than 2 children. Consider:
      //   (10, 10, 11)
      // where the number represent the number of partitions for each child, it's better to pick 10
      // here since we only need to shuffle one side - we'd need to shuffle two sides if we pick 11.
      //
      // However this should be sufficient for now since in Spark nodes with multiple children
      // always have exactly 2 children.

      // Whether we should consider `spark.sql.shuffle.partitions` and ensure enough parallelism
      // during shuffle. To achieve a good trade-off between parallelism and shuffle cost, we only
      // consider the minimum parallelism iff ALL children need to be re-shuffled.
      //
      // A child needs to be re-shuffled iff either one of below is true:
      //   1. It can't create partitioning by itself, i.e., `canCreatePartitioning` returns false
      //      (as for the case of `RangePartitioning`), therefore it needs to be re-shuffled
      //      according to other shuffle spec.
      //   2. It already has `ShuffleExchangeLike`, so we can re-use existing shuffle without
      //      introducing extra shuffle.
      //
      // On the other hand, in scenarios such as:
      //   HashPartitioning(5) <-> HashPartitioning(6)
      // while `spark.sql.shuffle.partitions` is 10, we'll only re-shuffle the left side and make it
      // HashPartitioning(6).
      //检查是否应考虑最小并行度
      val shouldConsiderMinParallelism = specs.forall(p =>
        !p._2.canCreatePartitioning || children(p._1).isInstanceOf[ShuffleExchangeLike]
      )
      // Choose all the specs that can be used to shuffle other children
      //筛选出所有可以用于指导其他子节点 Shuffle 的 ShuffleSpec（即 canCreatePartitioning 为真），并根据 shouldConsiderMinParallelism 的结果，排除并行度不足的方案
      val candidateSpecs = specs
          .filter(_._2.canCreatePartitioning)
          .filter(p => !shouldConsiderMinParallelism ||
              children(p._1).outputPartitioning.numPartitions >= conf.defaultNumShufflePartitions)

      // 在所有候选方案中选择并行度最高的方案 (maxBy(_.numPartitions)) 作为 bestSpec。
      // 为了减少二次 Shuffle，优先考虑选择非 ShuffleExchangeLike 节点（即尚未 Shuffle 过）的方案作为基准
      val bestSpecOpt = if (candidateSpecs.isEmpty) {
        None
      } else {
        // When choosing specs, we should consider those children with no `ShuffleExchangeLike` node
        // first. For instance, if we have:
        //   A: (No_Exchange, 100) <---> B: (Exchange, 120)
        // it's better to pick A and change B to (Exchange, 100) instead of picking B and insert a
        // new shuffle for A.
        val candidateSpecsWithoutShuffle = candidateSpecs.filter { case (k, _) =>
          !children(k).isInstanceOf[ShuffleExchangeLike]
        }
        //在所有候选 ShuffleSpec 中，选择并行度最高的方案，尽可能提高计算效率
        val finalCandidateSpecs = if (candidateSpecsWithoutShuffle.nonEmpty) {
          candidateSpecsWithoutShuffle
        } else {
          candidateSpecs
        }
        // Pick the spec with the best parallelism
        Some(finalCandidateSpecs.values.maxBy(_.numPartitions))
      }

      // Check if the following conditions are satisfied:
      //   1. There are exactly two children (e.g., join). Note that Spark doesn't support
      //      multi-way join at the moment, so this check should be sufficient.
      //   2. All children are of `KeyGroupedPartitioning`, and they are compatible with each other
      // If both are true, skip shuffle.
      //Key-Grouped 兼容性检查
      //检查是否满足 存储分区连接 (Storage-Partitioned Join) 的条件：父节点存在，恰好有两个子节点，且它们的分区是 KeyGroupedPartitioning 并且兼容。
      val isKeyGroupCompatible = parent.isDefined &&
          children.length == 2 && childrenIndexes.length == 2 && {
        val left = children.head
        val right = children(1)
        val newChildren = checkKeyGroupCompatible(
          parent.get, left, right, requiredChildDistributions)
        if (newChildren.isDefined) {
          children = newChildren.get
        }
        newChildren.isDefined
      }
      //应用最佳 Shuffle 方案
      //再次遍历所有子节点，应用协同分区的最终决定。
      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        //如果子节点需要协同分区，但其当前的分区方案与最佳方案不兼容，则需要强制进行 Shuffle。
        case ((child, _), idx) if isKeyGroupCompatible || !childrenIndexes.contains(idx) =>
          child
        case ((child, dist), idx) =>
          //如果子节点的 ShuffleSpec 已经与选定的 bestSpec 兼容，则保持不变。
          if (bestSpecOpt.isDefined && bestSpecOpt.get.isCompatibleWith(specs(idx))) {
            child
          } else {
            //根据 bestSpec（如果存在），创建新的分区方案来指导 Shuffle；如果 bestSpec 不存在，则根据原始的 requiredChildDistributions 创建默认分区方案
            val newPartitioning = bestSpecOpt.map { bestSpec =>
              // Use the best spec to create a new partitioning to re-shuffle this child
              val clustering = dist.asInstanceOf[ClusteredDistribution].clustering
              bestSpec.createPartitioning(clustering)
            }.getOrElse {
              // No best spec available, so we create default partitioning from the required
              // distribution
              val numPartitions = dist.requiredNumPartitions
                  .getOrElse(conf.numShufflePartitions)
              dist.createPartitioning(numPartitions)
            }

            child match {
              case ShuffleExchangeExec(_, c, so, ps) =>
                ShuffleExchangeExec(newPartitioning, c, so, ps)
              case _ => ShuffleExchangeExec(newPartitioning, child)
            }
          }
      }
    }

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    //Shuffle 结束后，开始处理排序要求。
    //这部分处理满足每个子节点的排序要求。
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      //检查子节点当前的输出排序 (outputOrdering) 是否已经满足所需的排序要求 (requiredOrdering)
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        SortExec(requiredOrdering, global = false, child = child)
      }
    }

    children
  }
  //用于根据目标分区键的顺序来调整 Join 操作的连接键顺序。其核心目的是判断是否可以通过简单地重排连接键，使其与子节点已有的分区方案兼容，从而避免昂贵的数据重洗牌（Shuffle）操作
  private def reorder(
      leftKeys: IndexedSeq[Expression],//左侧子节点的连接键列表，类型为 IndexedSeq，方便通过索引访问
      rightKeys: IndexedSeq[Expression],//右侧子节点的连接键列表，类型为 IndexedSeq
      expectedOrderOfKeys: Seq[Expression],//目标分区方案（如 HashPartitioning）所要求的分区键表达式的顺序。
      currentOrderOfKeys: Seq[Expression]): //当前连接键（通常是 leftKeys 或 rightKeys 之一）的原始顺序
  Option[(Seq[Expression], Seq[Expression])] = {
    //检查期望的键序列和当前的键序列长度是否一致
    //如果长度不一致，则无法匹配或重排，立即返回 None
    if (expectedOrderOfKeys.size != currentOrderOfKeys.size) {
      return None
    }

    // Check if the current order already satisfies the expected order.
    //将期望顺序和当前顺序的键按位置配对 (zip)，然后检查每一对键是否语义上相等（semanticEquals）
    if (expectedOrderOfKeys.zip(currentOrderOfKeys).forall(p => p._1.semanticEquals(p._2))) {
      return Some(leftKeys, rightKeys)
    }

    // Build a lookup between an expression and the positions its holds in the current key seq.
    //初始化一个可变映射表：键是规范化后的表达式，值是该表达式在 currentOrderOfKeys 中出现的所有索引位置（使用 BitSet 存储）
    val keyToIndexMap = mutable.Map.empty[Expression, mutable.BitSet]
    currentOrderOfKeys.zipWithIndex.foreach {
      case (key, index) =>
        keyToIndexMap.getOrElseUpdate(key.canonicalized, mutable.BitSet.empty).add(index)
    }

    // Reorder the keys.
    val leftKeysBuffer = new ArrayBuffer[Expression](leftKeys.size)
    val rightKeysBuffer = new ArrayBuffer[Expression](rightKeys.size)
    val iterator = expectedOrderOfKeys.iterator
    while (iterator.hasNext) {
      // Lookup the current index of this key.
      keyToIndexMap.get(iterator.next().canonicalized) match {
        //如果找到了对应的索引集合（Some(indices)），且集合非空
        case Some(indices) if indices.nonEmpty =>
          // Take the first available index from the map.
          //从 BitSet 中取出最小的索引值，即该键在原始序列中首次出现的位置
          val index = indices.firstKey
          indices.remove(index)

          // Add the keys for that index to the reordered keys.
          //使用该索引 index，从原始的 leftKeys 中取出表达式，并添加到左侧重排缓冲区
          leftKeysBuffer += leftKeys(index)
          rightKeysBuffer += rightKeys(index)
        case _ =>
          // The expression cannot be found, or we have exhausted all indices for that expression.
          // 如果没找到，直接返回空
          return None
      }
    }
    Some(leftKeysBuffer.toSeq, rightKeysBuffer.toSeq)
  }
  // 负责优化 Join 键的顺序，使其与子节点现有的数据分区方案相匹配，从而避免不必要的 Shuffle 或 Sort 操作
  private def reorderJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Partitioning,
      rightPartitioning: Partitioning): (Seq[Expression], Seq[Expression]) = {
    //检查所有连接键表达式是否都是确定性（Deterministic）的。只有确定性的表达式才能被安全地重排
    if (leftKeys.forall(_.deterministic) && rightKeys.forall(_.deterministic)) {
      reorderJoinKeysRecursively(
        leftKeys,
        rightKeys,
        Some(leftPartitioning),
        Some(rightPartitioning))
        .getOrElse((leftKeys, rightKeys))
    } else {
      (leftKeys, rightKeys)
    }
  }

  /**
   * Recursively reorders the join keys based on partitioning. It starts reordering the
   * join keys to match HashPartitioning on either side, followed by PartitioningCollection.
   */
  // 此方法是实际的递归重排逻辑，它基于左右子节点的现有分区类型来指导键的重排。目标是使连接键的顺序与已有的分区键顺序一致
  private def reorderJoinKeysRecursively(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Option[Partitioning],
      rightPartitioning: Option[Partitioning]): Option[(Seq[Expression], Seq[Expression])] = {
    (leftPartitioning, rightPartitioning) match {
      //如果左侧的分区方案是 HashPartitioning，且分区表达式为 leftExpressions
      case (Some(HashPartitioning(leftExpressions, _)), _) =>
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leftExpressions, leftKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, None, rightPartitioning))
      //如果右侧的分区方案是 HashPartitioning，且分区表达式为 rightExpressions
      case (_, Some(HashPartitioning(rightExpressions, _))) =>
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, rightExpressions, rightKeys)
          .orElse(reorderJoinKeysRecursively(
            leftKeys, rightKeys, leftPartitioning, None))
      //如果左侧是 KeyGroupedPartitioning（通常用于数据源级的分区/桶），提取其聚簇（Clustering）表达式
      case (Some(KeyGroupedPartitioning(clustering, _, _)), _) =>
        //从聚簇表达式中提取最底层的表达式（即实际用于分区的键）
        val leafExprs = clustering.flatMap(_.collectLeaves())
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, leftKeys)
            .orElse(reorderJoinKeysRecursively(
              leftKeys, rightKeys, None, rightPartitioning))
      // 如果右侧是 KeyGroupedPartitioning，逻辑与情况 3 类似，尝试匹配右侧键组。
      case (_, Some(KeyGroupedPartitioning(clustering, _, _))) =>
        val leafExprs = clustering.flatMap(_.collectLeaves())
        reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leafExprs, rightKeys)
            .orElse(reorderJoinKeysRecursively(
              leftKeys, rightKeys, leftPartitioning, None))
      //如果左侧是 PartitioningCollection（即有多种分区方式），需要递归检查集合中的每个分区方案。
      case (Some(PartitioningCollection(partitionings)), _) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, Some(p), rightPartitioning))
        }.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, None, rightPartitioning))
      // 如果右侧是 PartitioningCollection，逻辑与情况 5 类似，迭代检查集合中的每个分区方案
      case (_, Some(PartitioningCollection(partitionings))) =>
        partitionings.foldLeft(Option.empty[(Seq[Expression], Seq[Expression])]) { (res, p) =>
          res.orElse(reorderJoinKeysRecursively(leftKeys, rightKeys, leftPartitioning, Some(p)))
        }.orElse(None)
      case _ =>
        None
    }
  }

  /**
   * When the physical operators are created for JOIN, the ordering of join keys is based on order
   * in which the join keys appear in the user query. That might not match with the output
   * partitioning of the join node's children (thus leading to extra sort / shuffle being
   * introduced). This rule will change the ordering of the join keys to match with the
   * partitioning of the join nodes' children.
   */
  //作用是优化物理连接（Join）操作的连接键顺序，使其与输入子节点已有的数据分区或排序要求相匹配，以避免插入不必要的 ShuffleExchangeExec 或 SortExec
  private def reorderJoinPredicates(plan: SparkPlan): SparkPlan = {
    plan match {
      case ShuffledHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, condition, left, right, isSkew) =>
        //调用 reorderJoinKeys 辅助方法。传入当前的连接键 (leftKeys, rightKeys) 以及左右子计划当前的输出分区方案 (left.outputPartitioning, right.outputPartitioning)。
        // 该方法会尝试将 leftKeys/rightKeys 的顺序调整为与某个子节点的现有分区键顺序一致
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        ShuffledHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
          left, right, isSkew)

      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, isSkew) =>
        //调用 reorderJoinKeys 辅助方法，传入当前连接键和左右子计划的输出分区方案。对于 SortMergeJoin 而言，这个重排可以帮助匹配子节点可能已有的排序顺序。
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        SortMergeJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, condition,
          left, right, isSkew)

      case other => other
    }
  }

  /**
   * Checks whether two children, `left` and `right`, of a join operator have compatible
   * `KeyGroupedPartitioning`, and can benefit from storage-partitioned join.
   *
   * Returns the updated new children if the check is successful, otherwise `None`.
   */
  private def checkKeyGroupCompatible(
      parent: SparkPlan,
      left: SparkPlan,
      right: SparkPlan,
      requiredChildDistribution: Seq[Distribution]): Option[Seq[SparkPlan]] = {
    parent match {
      case smj: SortMergeJoinExec =>
        checkKeyGroupCompatible(left, right, smj.joinType, requiredChildDistribution)
      case sj: ShuffledHashJoinExec =>
        checkKeyGroupCompatible(left, right, sj.joinType, requiredChildDistribution)
      case _ =>
        None
    }
  }

  private def checkKeyGroupCompatible(
      left: SparkPlan,
      right: SparkPlan,
      joinType: JoinType,
      requiredChildDistribution: Seq[Distribution]): Option[Seq[SparkPlan]] = {
    assert(requiredChildDistribution.length == 2)

    var newLeft = left
    var newRight = right

    val specs = Seq(left, right).zip(requiredChildDistribution).map { case (p, d) =>
      if (!d.isInstanceOf[ClusteredDistribution]) return None
      val cd = d.asInstanceOf[ClusteredDistribution]
      val specOpt = createKeyGroupedShuffleSpec(p.outputPartitioning, cd)
      if (specOpt.isEmpty) return None
      specOpt.get
    }

    val leftSpec = specs.head
    val rightSpec = specs(1)

    var isCompatible = false
    if (!conf.v2BucketingPushPartValuesEnabled) {
      isCompatible = leftSpec.isCompatibleWith(rightSpec)
    } else {
      logInfo("Pushing common partition values for storage-partitioned join")
      isCompatible = leftSpec.areKeysCompatible(rightSpec)

      // Partition expressions are compatible. Regardless of whether partition values
      // match from both sides of children, we can calculate a superset of partition values and
      // push-down to respective data sources so they can adjust their output partitioning by
      // filling missing partition keys with empty partitions. As result, we can still avoid
      // shuffle.
      //
      // For instance, if two sides of a join have partition expressions
      // `day(a)` and `day(b)` respectively
      // (the join query could be `SELECT ... FROM t1 JOIN t2 on t1.a = t2.b`), but
      // with different partition values:
      //   `day(a)`: [0, 1]
      //   `day(b)`: [1, 2, 3]
      // Following the case 2 above, we don't have to shuffle both sides, but instead can
      // just push the common set of partition values: `[0, 1, 2, 3]` down to the two data
      // sources.
      if (isCompatible) {
        val leftPartValues = leftSpec.partitioning.partitionValues
        val rightPartValues = rightSpec.partitioning.partitionValues

        logInfo(
          s"""
             |Left side # of partitions: ${leftPartValues.size}
             |Right side # of partitions: ${rightPartValues.size}
             |""".stripMargin)

        // As partition keys are compatible, we can pick either left or right as partition
        // expressions
        val partitionExprs = leftSpec.partitioning.expressions

        var mergedPartValues = InternalRowComparableWrapper
            .mergePartitions(leftSpec.partitioning, rightSpec.partitioning, partitionExprs)
            .map(v => (v, 1))

        logInfo(s"After merging, there are ${mergedPartValues.size} partitions")

        var replicateLeftSide = false
        var replicateRightSide = false
        var applyPartialClustering = false

        // This means we allow partitions that are not clustered on their values,
        // that is, multiple partitions with the same partition value. In the
        // following, we calculate how many partitions that each distinct partition
        // value has, and pushdown the information to scans, so they can adjust their
        // final input partitions respectively.
        if (conf.v2BucketingPartiallyClusteredDistributionEnabled) {
          logInfo("Calculating partially clustered distribution for " +
              "storage-partitioned join")

          // Similar to `OptimizeSkewedJoin`, we need to check join type and decide
          // whether partially clustered distribution can be applied. For instance, the
          // optimization cannot be applied to a left outer join, where the left hand
          // side is chosen as the side to replicate partitions according to stats.
          // Otherwise, query result could be incorrect.
          val canReplicateLeft = canReplicateLeftSide(joinType)
          val canReplicateRight = canReplicateRightSide(joinType)

          if (!canReplicateLeft && !canReplicateRight) {
            logInfo("Skipping partially clustered distribution as it cannot be applied for " +
                s"join type '$joinType'")
          } else {
            val leftLink = left.logicalLink
            val rightLink = right.logicalLink

            replicateLeftSide = if (
              leftLink.isDefined && rightLink.isDefined &&
                  leftLink.get.stats.sizeInBytes > 1 &&
                  rightLink.get.stats.sizeInBytes > 1) {
              logInfo(
                s"""
                   |Using plan statistics to determine which side of join to fully
                   |cluster partition values:
                   |Left side size (in bytes): ${leftLink.get.stats.sizeInBytes}
                   |Right side size (in bytes): ${rightLink.get.stats.sizeInBytes}
                   |""".stripMargin)
              leftLink.get.stats.sizeInBytes < rightLink.get.stats.sizeInBytes
            } else {
              // As a simple heuristic, we pick the side with fewer number of partitions
              // to apply the grouping & replication of partitions
              logInfo("Using number of partitions to determine which side of join " +
                  "to fully cluster partition values")
              leftPartValues.size < rightPartValues.size
            }

            replicateRightSide = !replicateLeftSide

            // Similar to skewed join, we need to check the join type to see whether replication
            // of partitions can be applied. For instance, replication should not be allowed for
            // the left-hand side of a right outer join.
            if (replicateLeftSide && !canReplicateLeft) {
              logInfo("Left-hand side is picked but cannot be applied to join type " +
                  s"'$joinType'. Skipping partially clustered distribution.")
              replicateLeftSide = false
            } else if (replicateRightSide && !canReplicateRight) {
              logInfo("Right-hand side is picked but cannot be applied to join type " +
                  s"'$joinType'. Skipping partially clustered distribution.")
              replicateRightSide = false
            } else {
              val partValues = if (replicateLeftSide) rightPartValues else leftPartValues
              val numExpectedPartitions = partValues
                .map(InternalRowComparableWrapper(_, partitionExprs))
                .groupBy(identity)
                .mapValues(_.size)

              mergedPartValues = mergedPartValues.map { case (partVal, numParts) =>
                (partVal, numExpectedPartitions.getOrElse(
                  InternalRowComparableWrapper(partVal, partitionExprs), numParts))
              }

              logInfo("After applying partially clustered distribution, there are " +
                  s"${mergedPartValues.map(_._2).sum} partitions.")
              applyPartialClustering = true
            }
          }
        }

        // Now we need to push-down the common partition key to the scan in each child
        newLeft = populatePartitionValues(
          left, mergedPartValues, applyPartialClustering, replicateLeftSide)
        newRight = populatePartitionValues(
          right, mergedPartValues, applyPartialClustering, replicateRightSide)
      }
    }

    if (isCompatible) Some(Seq(newLeft, newRight)) else None
  }

  // Similar to `OptimizeSkewedJoin.canSplitRightSide`
  private def canReplicateLeftSide(joinType: JoinType): Boolean = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }

  // Similar to `OptimizeSkewedJoin.canSplitLeftSide`
  private def canReplicateRightSide(joinType: JoinType): Boolean = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
        joinType == LeftAnti || joinType == LeftOuter
  }

  // Populate the common partition values down to the scan nodes
  private def populatePartitionValues(
      plan: SparkPlan,
      values: Seq[(InternalRow, Int)],
      applyPartialClustering: Boolean,
      replicatePartitions: Boolean): SparkPlan = plan match {
    case scan: BatchScanExec =>
      scan.copy(
        spjParams = scan.spjParams.copy(
          commonPartitionValues = Some(values),
          applyPartialClustering = applyPartialClustering,
          replicatePartitions = replicatePartitions
        )
      )
    case node =>
      node.mapChildren(child => populatePartitionValues(
        child, values, applyPartialClustering, replicatePartitions))
  }

  /**
   * Tries to create a [[KeyGroupedShuffleSpec]] from the input partitioning and distribution, if
   * the partitioning is a [[KeyGroupedPartitioning]] (either directly or indirectly), and
   * satisfies the given distribution.
   */
  private def createKeyGroupedShuffleSpec(
      partitioning: Partitioning,
      distribution: ClusteredDistribution): Option[KeyGroupedShuffleSpec] = {
    def tryCreate(partitioning: KeyGroupedPartitioning): Option[KeyGroupedShuffleSpec] = {
      val attributes = partitioning.expressions.flatMap(_.collectLeaves())
      val clustering = distribution.clustering

      val satisfies = if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
        attributes.length == clustering.length && attributes.zip(clustering).forall {
          case (l, r) => l.semanticEquals(r)
        }
      } else {
        partitioning.satisfies(distribution)
      }

      if (satisfies) {
        Some(partitioning.createShuffleSpec(distribution).asInstanceOf[KeyGroupedShuffleSpec])
      } else {
        None
      }
    }

    partitioning match {
      case p: KeyGroupedPartitioning => tryCreate(p)
      case PartitioningCollection(partitionings) =>
        val specs = partitionings.map(p => createKeyGroupedShuffleSpec(p, distribution))
        specs.filter(_.isDefined).map(_.get).headOption
      case _ => None
    }
  }
  //Rule 的入口方法，负责对整个物理执行计划树进行自底向上（transformUp）的遍历和转换
  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = plan.transformUp {
      //operator @ 的意思是 如果 ShuffleExchangeExec(...) 模式匹配成功，把整个对象绑定到变量 operator
      case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, shuffleOrigin, _)
          if optimizeOutRepartition &&
            (shuffleOrigin == REPARTITION_BY_COL || shuffleOrigin == REPARTITION_BY_NUM) => //REPARTITION_BY_COL和REPARTITION_BY_NUM代表由用户 Repartition 触发
        def hasSemanticEqualPartitioning(partitioning: Partitioning): Boolean = {
          partitioning match {
            case lower: HashPartitioning if upper.semanticEquals(lower) => true
            case lower: PartitioningCollection =>
              lower.partitionings.exists(hasSemanticEqualPartitioning)
            case _ => false
          }
        }
        // 如果语意相等，直接返回子节点
        if (hasSemanticEqualPartitioning(child.outputPartitioning)) {
          child
        } else {
          operator
        }
      //对所有非 Shuffle 节点：首先调用 reorderJoinPredicates 优化 Join 键，然后调用 ensureDistributionAndOrdering 满足子节点的分区和排序要求，
      // 最后用新的子节点替换旧的子节点
      case operator: SparkPlan =>
        val reordered = reorderJoinPredicates(operator)
        // 满足子节点的分区和排序要求，最后用新的子节点替换旧的子节点
        val newChildren = ensureDistributionAndOrdering(
          Some(reordered),
          reordered.children,
          reordered.requiredChildDistribution,
          reordered.requiredChildOrdering,
          ENSURE_REQUIREMENTS)
        reordered.withNewChildren(newChildren)
    }
    //处理根节点分布要求
    //如果构造函数传入了 requiredDistribution（常见于 AQE），则在整个计划转换完成后，对最顶层节点再应用一次 ensureDistributionAndOrdering，确保最终输出满足该要求
    if (requiredDistribution.isDefined) {
      val shuffleOrigin = if (requiredDistribution.get.requiredNumPartitions.isDefined) {
        REPARTITION_BY_NUM
      } else {
        REPARTITION_BY_COL
      }
      val finalPlan = ensureDistributionAndOrdering(
        None,
        newPlan :: Nil,
        requiredDistribution.get :: Nil,
        Seq(Nil),
        shuffleOrigin)
      assert(finalPlan.size == 1)
      finalPlan.head
    } else {
      newPlan
    }
  }
}
