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

package org.apache.spark.sql.execution.bucketing

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, ShuffledJoin, SortMergeJoinExec}

/**
 * This rule coalesces one side of the `SortMergeJoin` and `ShuffledHashJoin`
 * if the following conditions are met:
 *   - Two bucketed tables are joined.
 *   - Join keys match with output partition expressions on their respective sides.
 *   - The larger bucket number is divisible by the smaller bucket number.
 *   - COALESCE_BUCKETS_IN_JOIN_ENABLED is set to true.
 *   - The ratio of the number of buckets is less than the value set in
 *     COALESCE_BUCKETS_IN_JOIN_MAX_BUCKET_RATIO.
 */
object CoalesceBucketsInJoin extends Rule[SparkPlan] {
  //在 Spark 执行计划中，更新使用了分桶数据源（FileSourceScanExec）的扫描操作，使其使用合并后的桶数
  private def updateNumCoalescedBucketsInScan(
      plan: SparkPlan,
      numCoalescedBuckets: Int): SparkPlan = {
    plan transformUp {
      case f: FileSourceScanExec if f.relation.bucketSpec.nonEmpty =>
        //case class会自动生成一个名为copy的方法。这个方法允许你在不修改原对象的情况下创建一个新对象，并且可以选择性地修改部分字段的值
        //copy方法接受与case class定义的构造函数相同的字段参数
        f.copy(optionalNumCoalescedBuckets = Some(numCoalescedBuckets))
    }
  }
  //用于更新连接操作（ShuffledJoin）的左右子树的桶数。具体来说，它根据合并后的桶数来调整连接操作的左右子操作，确保连接操作中使用了合适的桶数
  //这在优化 Spark 中的 SortMergeJoinExec 和 ShuffledHashJoinExec 类型的连接操作时非常重要，可以通过调整桶数来提高性能并减少内存消耗
  private def updateNumCoalescedBuckets(
      join: ShuffledJoin, //表示连接操作。ShuffledJoin 是 SortMergeJoinExec 或 ShuffledHashJoinExec 的父类，表示基于 shuffle 的连接操作
      numLeftBuckets: Int, //表示连接操作左侧（左表）桶的数量
      numCoalescedBuckets: Int): ShuffledJoin = { //表示合并后的桶数，即两个表中较小的桶数
    //首先判断 numCoalescedBuckets（合并后的桶数）是否与左侧表的桶数 numLeftBuckets 相等。如果不相等，意味着需要更新左侧的桶数
    if (numCoalescedBuckets != numLeftBuckets) {
      val leftCoalescedChild =
        updateNumCoalescedBucketsInScan(join.left, numCoalescedBuckets)
      join match {
        case j: SortMergeJoinExec => j.copy(left = leftCoalescedChild)
        case j: ShuffledHashJoinExec => j.copy(left = leftCoalescedChild)
      }
    } else {
      val rightCoalescedChild =
        updateNumCoalescedBucketsInScan(join.right, numCoalescedBuckets)
      join match {
        case j: SortMergeJoinExec => j.copy(right = rightCoalescedChild)
        case j: ShuffledHashJoinExec => j.copy(right = rightCoalescedChild)
      }
    }
  }
  //判断在 ShuffledHashJoinExec 连接操作中，是否应该合并流侧（即合并桶数时，哪个侧的桶应该进行合并）。
  // 函数的核心逻辑是根据左右表的桶数以及连接操作的构建侧（buildSide）来确定是否合并流侧
  private def isCoalesceSHJStreamSide(
      join: ShuffledHashJoinExec, //表示一个 ShuffledHashJoinExec 类型的连接操作。该操作是 Spark 中一种基于 Shuffle 的哈希连接，通常用于处理大规模数据的连接操作
      numLeftBuckets: Int, //表示连接操作中左侧表（左边的子计划）的桶数
      numCoalescedBuckets: Int): Boolean = { //表示合并后的桶数，通常为左右表中桶数较小的那个桶数
    //如果 numCoalescedBuckets（合并后的桶数）与左侧表的桶数 numLeftBuckets 相等，意味着左侧表的桶数已经合并为合适的桶数，不需要进一步合并
    if (numCoalescedBuckets == numLeftBuckets) {
      //如果 buildSide 是 BuildRight，表示右侧是构建侧，则返回 true，表示左侧应该进行流侧的合并（不合并右侧）
      join.buildSide != BuildRight
    } else {
      //如果左侧的桶数不等于合并后的桶数（即左侧表的桶数需要合并），则检查连接操作的构建侧（buildSide）。
      // 如果 buildSide 是 BuildLeft，表示左侧是构建侧，那么应该合并右侧的流侧。因此返回 true，表示右侧是流侧。
      join.buildSide != BuildLeft
    }
  }
  // 用于对 SparkPlan（执行计划）进行转换，并根据特定条件优化连接操作中的桶数。
  // 这个方法的主要目的是在满足一定条件的情况下合并连接操作中的桶数，以提升性能
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.coalesceBucketsInJoinEnabled) { //是否开始bucket合并
      return plan
    }

    plan transform {
      case ExtractJoinWithBuckets(join, numLeftBuckets, numRightBuckets)
        if math.max(numLeftBuckets, numRightBuckets) / math.min(numLeftBuckets, numRightBuckets) <=
          conf.coalesceBucketsInJoinMaxBucketRatio =>   //最大桶数 / 最小桶数的比例要小于等于这个参数
        val numCoalescedBuckets = math.min(numLeftBuckets, numRightBuckets)
        join match {
          case j: SortMergeJoinExec =>
            updateNumCoalescedBuckets(j, numLeftBuckets, numCoalescedBuckets)
          case j: ShuffledHashJoinExec
            // Only coalesce the buckets for shuffled hash join stream side,
            // to avoid OOM for build side.
            if isCoalesceSHJStreamSide(j, numLeftBuckets, numCoalescedBuckets) =>
            updateNumCoalescedBuckets(j, numLeftBuckets, numCoalescedBuckets)
          case other => other
        }
      case other => other
    }
  }
}

/**
 * An extractor that extracts `SortMergeJoinExec` and `ShuffledHashJoin`,
 * where both sides of the join have the bucketed tables,
 * are consisted of only the scan operation,
 * and numbers of buckets are not equal but divisible.
 */
//主要用于提取符合特定条件的 ShuffledJoin（带有分桶的连接）操作。
// 它的核心是处理与桶化（bucketed）相关的连接操作，且要求连接的两边表必须都是分桶表，且桶的数量不相等但可以整除
object ExtractJoinWithBuckets {
  //递归检查给定的 SparkPlan 是否包含扫描操作（FileSourceScanExec）。
  // 如果计划中包含 FilterExec 或 ProjectExec 等操作，继续递归检查其子节点。
  // 最终，只有 FileSourceScanExec 会返回一个布尔值，表示是否包含分桶（bucketSpec 不为空）
  @tailrec
  private def hasScanOperation(plan: SparkPlan): Boolean = plan match {
    case f: FilterExec => hasScanOperation(f.child)
    case p: ProjectExec => hasScanOperation(p.child)
    case j: BroadcastHashJoinExec =>
      if (j.buildSide == BuildLeft) hasScanOperation(j.right) else hasScanOperation(j.left)
    case j: BroadcastNestedLoopJoinExec =>
      if (j.buildSide == BuildLeft) hasScanOperation(j.right) else hasScanOperation(j.left)
    case f: FileSourceScanExec => f.relation.bucketSpec.nonEmpty
    case _ => false
  }
  //提取扫描操作中的分桶信息，前提是扫描操作包含非空的 bucketSpec，并且没有指定可选的合并桶数
  private def getBucketSpec(plan: SparkPlan): Option[BucketSpec] = {
    plan.collectFirst {
      case f: FileSourceScanExec if f.relation.bucketSpec.nonEmpty &&
          f.optionalNumCoalescedBuckets.isEmpty =>
        f.relation.bucketSpec.get
    }
  }

  /**
   * The join keys should match with expressions for output partitioning. Note that
   * the ordering does not matter because it will be handled in `EnsureRequirements`.
   */
  //判断连接操作的连接键（keys）是否符合输出分区的要求（partitioning）
  private def satisfiesOutputPartitioning(
      keys: Seq[Expression], //表示连接操作使用的连接键（连接条件中使用的字段）。这些连接键将用于检查是否符合分区要求
      partitioning: Partitioning): Boolean = { //表示输出分区的方式。分区方式一般是根据连接键进行的哈希分区（HashPartitioning）
    partitioning match {
      case HashPartitioning(exprs, _) if exprs.length == keys.length =>
        exprs.forall(e => keys.exists(_.semanticEquals(e)))
      case _ => false
    }
  }
  //检查连接的左侧和右侧操作是否都包含扫描操作，且它们的输出分区符合连接键的分区要求
  private def isApplicable(j: ShuffledJoin): Boolean = {
    hasScanOperation(j.left) &&
      hasScanOperation(j.right) &&
      satisfiesOutputPartitioning(j.leftKeys, j.left.outputPartitioning) &&
      satisfiesOutputPartitioning(j.rightKeys, j.right.outputPartitioning)
  }
  //判断两个分桶的数量是否可以整除。要求两个表的分桶数量不相等，但大桶数能被小桶数整除
  private def isDivisible(numBuckets1: Int, numBuckets2: Int): Boolean = {
    val (small, large) = (math.min(numBuckets1, numBuckets2), math.max(numBuckets1, numBuckets2))
    // A bucket can be coalesced only if the bigger number of buckets is divisible by the smaller
    // number of buckets because bucket id is calculated by modding the total number of buckets.
    numBuckets1 != numBuckets2 && large % small == 0
  }
  //实现 Scala 的模式匹配，提取符合条件的 ShuffledJoin 操作，并返回桶数
  def unapply(plan: SparkPlan): Option[(ShuffledJoin, Int, Int)] = {
    plan match {
      case j: ShuffledJoin if isApplicable(j) =>
        val leftBucket = getBucketSpec(j.left)
        val rightBucket = getBucketSpec(j.right)
        if (leftBucket.isDefined && rightBucket.isDefined &&
            isDivisible(leftBucket.get.numBuckets, rightBucket.get.numBuckets)) {
          Some(j, leftBucket.get.numBuckets, rightBucket.get.numBuckets)
        } else {
          None
        }
      case _ => None
    }
  }
}
