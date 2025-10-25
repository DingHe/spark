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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{CoalescedBoundary, CoalescedHashPartitioning, HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A wrapper of shuffle query stage, which follows the given partition arrangement.
 *
 * @param child           It is usually `ShuffleQueryStageExec`, but can be the shuffle exchange
 *                        node during canonicalization.
 * @param partitionSpecs  The partition specs that defines the arrangement, requires at least one
 *                        partition.
 */
// AQEShuffleReadExec 是一个 物理执行计划节点（SparkPlan），它的主要作用是封装一个已完成的 Shuffle 阶段（Query Stage），
// 并根据 AQE 优化器在运行时（即 Shuffle 阶段完成后）动态决定的分区安排来读取 Shuffle 输出数据。
// 它是一个智能的 Shuffle 读取器，负责在 AQE 流程中实现以下运行时优化：
// 分区合并 (Coalescing Partitions): 将小的 Shuffle 分区合并成更大的分区，以减少任务数量，降低调度开销。
// 处理数据倾斜 (Skew Handling): 将过大的、产生数据倾斜的 Reducer 块拆分成多个小的子任务（Split），并行读取和处理，以缓解倾斜问题。
// 本地 Shuffle 读取 (Local Shuffle Read): 在特定条件下，可以直接从上游 Mapper 任务所在的节点读取 Shuffle 数据，避免跨节点传输，提高效率。
case class AQEShuffleReadExec private(
    child: SparkPlan, // 子执行计划。 通常是 ShuffleQueryStageExec（一个已完成的 Shuffle 阶段），但在**规范化（canonicalization）**过程中也可以是原始的 ShuffleExchangeExec 节点。
    partitionSpecs: Seq[ShufflePartitionSpec]) extends UnaryExecNode { // 分区规范序列。 一个序列，包含了由 AQE 动态计算出的 ShufflePartitionSpec 实例
  assert(partitionSpecs.nonEmpty, s"${getClass.getSimpleName} requires at least one partition")

  // If this is to read shuffle files locally, then all partition specs should be
  // `PartialMapperPartitionSpec`.
  if (partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec])) {
    assert(partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]))
  }

  override def supportsColumnar: Boolean = child.supportsColumnar
  // 输出列的属性
  override def output: Seq[Attribute] = child.output
  // 用来在 AQE（自适应执行）场景下推断当前 Shuffle Read 节点在执行时的输出分区信息（Partitioning）
  override lazy val outputPartitioning: Partitioning = {
    // If it is a local shuffle read with one mapper per task, then the output partitioning is
    // the same as the plan before shuffle.
    // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
    if (partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]) &&
      //  意味着每个 partitionSpec 的 mapIndex 都互不相同
      // 即“每个输出分区对应不同的 mapper”，也就是“one mapper per task”的情况
        partitionSpecs.map(_.asInstanceOf[PartialMapperPartitionSpec].mapIndex).toSet.size ==
          partitionSpecs.length) {
      // 合在一起判断：所有都是 PartialMapper 且 mapIndex 无重复 → 这是 local read 且每个 mapper 对应一个输出分区 → 可以保留 shuffle 之前 child 的 partitioning
      child match {
        case ShuffleQueryStageExec(_, s: ShuffleExchangeLike, _) =>
          s.child.outputPartitioning
        case ShuffleQueryStageExec(_, r @ ReusedExchangeExec(_, s: ShuffleExchangeLike), _) =>
          s.child.outputPartitioning match {
            case e: Expression => r.updateAttr(e).asInstanceOf[Partitioning]
            case other => other
          }
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
      // 是否为 coalesced read，即合并分区读取场景
    } else if (isCoalescedRead) {
      // For coalesced shuffle read, the data distribution is not changed, only the number of
      // partitions is changed.
      child.outputPartitioning match {
        case h: HashPartitioning =>
          val partitions = partitionSpecs.map {
            case CoalescedPartitionSpec(start, end, _) => CoalescedBoundary(start, end)
            // Can not happend due to isCoalescedRead
            case unexpected =>
              throw SparkException.internalError(s"Unexpected ShufflePartitionSpec: $unexpected")
          }
          CurrentOrigin.withOrigin(h.origin)(CoalescedHashPartitioning(h, partitions))
        case r: RangePartitioning =>
          CurrentOrigin.withOrigin(r.origin)(r.copy(numPartitions = partitionSpecs.length))
        // This can only happen for `REBALANCE_PARTITIONS_BY_NONE`, which uses
        // `RoundRobinPartitioning` but we don't need to retain the number of partitions.
        case r: RoundRobinPartitioning =>
          r.copy(numPartitions = partitionSpecs.length)
        case other @ SinglePartition =>
          throw new IllegalStateException(
            "Unexpected partitioning for coalesced shuffle read: " + other)
        case _ =>
          // Spark plugins may have custom partitioning and may replace this operator
          // during the postStageOptimization phase, so return UnknownPartitioning here
          // rather than throw an exception
          UnknownPartitioning(partitionSpecs.length)
      }
    } else {
      UnknownPartitioning(partitionSpecs.length)
    }
  }

  override def stringArgs: Iterator[Any] = {
    val desc = if (isLocalRead) {
      "local"
    } else if (hasCoalescedPartition && hasSkewedPartition) {
      "coalesced and skewed"
    } else if (hasCoalescedPartition) {
      "coalesced"
    } else if (hasSkewedPartition) {
      "skewed"
    } else {
      ""
    }
    Iterator(desc)
  }

  /**
   * Returns true iff some partitions were actually combined
   */
  private def isCoalescedSpec(spec: ShufflePartitionSpec) = spec match {
    case CoalescedPartitionSpec(0, 0, _) => true
    case s: CoalescedPartitionSpec => s.endReducerIndex - s.startReducerIndex > 1
    case _ => false
  }

  /**
   * Returns true iff some non-empty partitions were combined
   */
  def hasCoalescedPartition: Boolean = {
    partitionSpecs.exists(isCoalescedSpec)
  }

  def hasSkewedPartition: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialReducerPartitionSpec])

  def isLocalRead: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec]) ||
      partitionSpecs.exists(_.isInstanceOf[CoalescedMapperPartitionSpec])

  def isCoalescedRead: Boolean = {
    partitionSpecs.sliding(2).forall {
      // A single partition spec which is `CoalescedPartitionSpec` also means coalesced read.
      case Seq(_: CoalescedPartitionSpec) => true
      case Seq(l: CoalescedPartitionSpec, r: CoalescedPartitionSpec) =>
        l.endReducerIndex <= r.startReducerIndex
      case _ => false
    }
  }

  private def shuffleStage = child match {
    case stage: ShuffleQueryStageExec => Some(stage)
    case _ => None
  }

  @transient private lazy val partitionDataSizes: Option[Seq[Long]] = {
    if (!isLocalRead && shuffleStage.get.mapStats.isDefined) {
      Some(partitionSpecs.map {
        case p: CoalescedPartitionSpec =>
          assert(p.dataSize.isDefined)
          p.dataSize.get
        case p: PartialReducerPartitionSpec => p.dataSize
        case p => throw new IllegalStateException(s"unexpected $p")
      })
    } else {
      None
    }
  }

  private def sendDriverMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val driverAccumUpdates = ArrayBuffer.empty[(Long, Long)]

    val numPartitionsMetric = metrics("numPartitions")
    numPartitionsMetric.set(partitionSpecs.length)
    driverAccumUpdates += (numPartitionsMetric.id -> partitionSpecs.length.toLong)

    if (hasSkewedPartition) {
      val skewedSpecs = partitionSpecs.collect {
        case p: PartialReducerPartitionSpec => p
      }

      val skewedPartitions = metrics("numSkewedPartitions")
      val skewedSplits = metrics("numSkewedSplits")

      val numSkewedPartitions = skewedSpecs.map(_.reducerIndex).distinct.length
      val numSplits = skewedSpecs.length

      skewedPartitions.set(numSkewedPartitions)
      driverAccumUpdates += (skewedPartitions.id -> numSkewedPartitions)

      skewedSplits.set(numSplits)
      driverAccumUpdates += (skewedSplits.id -> numSplits)
    }

    if (hasCoalescedPartition) {
      val numCoalescedPartitionsMetric = metrics("numCoalescedPartitions")
      val x = partitionSpecs.count(isCoalescedSpec)
      numCoalescedPartitionsMetric.set(x)
      driverAccumUpdates += numCoalescedPartitionsMetric.id -> x
    }

    partitionDataSizes.foreach { dataSizes =>
      val partitionDataSizeMetrics = metrics("partitionDataSize")
      driverAccumUpdates ++= dataSizes.map(partitionDataSizeMetrics.id -> _)
      // Set sum value to "partitionDataSize" metric.
      partitionDataSizeMetrics.set(dataSizes.sum)
    }

    SQLMetrics.postDriverMetricsUpdatedByValue(sparkContext, executionId, driverAccumUpdates.toSeq)
  }

  @transient override lazy val metrics: Map[String, SQLMetric] = {
    if (shuffleStage.isDefined) {
      Map("numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")) ++ {
        if (isLocalRead) {
          // We split the mapper partition evenly when creating local shuffle read, so no
          // data size info is available.
          Map.empty
        } else {
          Map("partitionDataSize" ->
            SQLMetrics.createSizeMetric(sparkContext, "partition data size"))
        }
      } ++ {
        if (hasSkewedPartition) {
          Map("numSkewedPartitions" ->
            SQLMetrics.createMetric(sparkContext, "number of skewed partitions"),
            "numSkewedSplits" ->
              SQLMetrics.createMetric(sparkContext, "number of skewed partition splits"))
        } else {
          Map.empty
        }
      } ++ {
        if (hasCoalescedPartition) {
          Map("numCoalescedPartitions" ->
            SQLMetrics.createMetric(sparkContext, "number of coalesced partitions"))
        } else {
          Map.empty
        }
      }
    } else {
      // It's a canonicalized plan, no need to report metrics.
      Map.empty
    }
  }

  private lazy val shuffleRDD: RDD[_] = {
    shuffleStage match {
      case Some(stage) =>
        sendDriverMetrics()
        stage.shuffle.getShuffleRDD(partitionSpecs.toArray)
      case _ =>
        throw new IllegalStateException("operating on canonicalized plan")
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    shuffleRDD.asInstanceOf[RDD[InternalRow]]
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    shuffleRDD.asInstanceOf[RDD[ColumnarBatch]]
  }

  override protected def withNewChildInternal(newChild: SparkPlan): AQEShuffleReadExec =
    copy(child = newChild)
}
