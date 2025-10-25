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

package org.apache.spark.sql.execution

import java.util.Arrays

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.internal.SQLConf

// 作用是统一规范地描述下游（Reducer 端）任务在执行 Shuffle 读取操作时，应该从上游（Mapper 端）读取哪些具体的数据块
// 在传统的 Spark Shuffle 中，每个 Reducer 任务只读取其对应的 Reducer 索引下的所有数据。但在 Spark SQL 的 自适应查询执行 (AQE) 优化中，为了实现以下目标，需要更灵活的分区规范：
// 分区合并（Shuffle Coalesce）： 将多个相邻的小 Shuffle 分区合并成一个大的分区，由一个 Reducer 任务读取，减少任务开销。
// 动态任务划分： 实现更细粒度或更灵活的数据读取模式。
sealed trait ShufflePartitionSpec

// A partition that reads data of one or more reducers, from `startReducerIndex` (inclusive) to
// `endReducerIndex` (exclusive).
// 合并分区规范
// 这是 AQE Shuffle 分区合并（Coalesce Shuffle Partitions）优化的主要产物。
case class CoalescedPartitionSpec(
    startReducerIndex: Int, // 起始 Reducer 索引（包含）。 指示该任务将要读取的 Shuffle 分区的起始索引
    endReducerIndex: Int,  // 结束 Reducer 索引（不包含）。 指示读取的 Shuffle 分区的结束索引（即不读取该索引）。一个任务将读取范围 [startReducerIndex, endReducerIndex) 内的所有分区数据
    @transient dataSize: Option[Long] = None)  // 数据总大小（可选/瞬态）。 可选地包含该合并分区所代表的所有数据块的近似总字节数。
  extends ShufflePartitionSpec

object CoalescedPartitionSpec {
  def apply(startReducerIndex: Int,
            endReducerIndex: Int,
            dataSize: Long): CoalescedPartitionSpec = {
    CoalescedPartitionSpec(startReducerIndex, endReducerIndex, Some(dataSize))
  }
}

// A partition that reads partial data of one reducer, from `startMapIndex` (inclusive) to
// `endMapIndex` (exclusive).
// 部分 Reducer 分区规范
// 用于指定一个任务只读取一个特定 Reducer 分区中的部分 Mapper 输出
case class PartialReducerPartitionSpec(
    reducerIndex: Int, // 目标 Reducer 索引。 指示要读取哪个 Reducer 分区的数据。
    startMapIndex: Int, // 起始 Mapper 索引（包含）。 指示只读取数据来源于 startMapIndex 开始的 Mapper 任务。
    endMapIndex: Int, // 结束 Mapper 索引（不包含）。 指示只读取数据来源到 endMapIndex 结束的 Mapper 任务。
    @transient dataSize: Long) extends ShufflePartitionSpec // 数据大小（瞬态）。 该部分数据对应的总字节数。

// A partition that reads partial data of one mapper, from `startReducerIndex` (inclusive) to
// `endReducerIndex` (exclusive).
// 部分 Mapper 分区规范
// 描述一个任务只读取一个特定 Mapper 输出中的部分 Reducer 数据
case class PartialMapperPartitionSpec(
    mapIndex: Int, // 目标 Mapper 索引。 指示要读取哪个 Mapper 的输出数据。
    startReducerIndex: Int, // 起始 Reducer 索引（包含）。 指示只读取目标 Reducer 分区范围 [startReducerIndex, endReducerIndex) 的数据。
    endReducerIndex: Int) extends ShufflePartitionSpec

// TODO(SPARK-36234): Consider mapper location and shuffle block size when coalescing mappers
// 合并 Mapper 分区规范
// 用于描述一个任务读取多个 Mapper 的输出，但它们的目标 Reducer 分区数是固定的。
case class CoalescedMapperPartitionSpec(
    startMapIndex: Int, // 起始 Mapper 索引（包含）。
    endMapIndex: Int, // 结束 Mapper 索引（不包含）。
    numReducers: Int) extends ShufflePartitionSpec // 目标 Reducer 数量。 指示所合并的 Mapper 集合的目标 Reducer 总数。

/**
 * The [[Partition]] used by [[ShuffledRowRDD]].
 */
private final case class ShuffledRowRDDPartition(
  index: Int, spec: ShufflePartitionSpec) extends Partition

/**
 * A Partitioner that might group together one or more partitions from the parent.
 *
 * @param parent a parent partitioner
 * @param partitionStartIndices indices of partitions in parent that should create new partitions
 *   in child (this should be an array of increasing partition IDs). For example, if we have a
 *   parent with 5 partitions, and partitionStartIndices is [0, 2, 4], we get three output
 *   partitions, corresponding to partition ranges [0, 1], [2, 3] and [4] of the parent partitioner.
 */
 // 主要作用是将一个“父分区器”（parent）产生的多个分区合并（coalesce）成数量更少的新分区
 // 分区器通常用于 减少分区数量 的操作，例如 coalesce 转换操作。它通过一个预先定义的起始索引数组（partitionStartIndices）来决定如何将父分区器的连续分区范围映射到新的、更少的分区上
// partitionStartIndices 定义了新分区的边界。数组中的每个元素都是父分区器中的一个分区 ID 如果父分区有 5 个分区，partitionStartIndices 为 [0, 2, 4]，则：<ul><li>新分区 0 包含父分区的 [0, 1] 范围。</li><li>新分区 1 包含父分区的 [2, 3] 范围。</li><li>新分区 2 包含父分区的 [4] 范围。
class CoalescedPartitioner(val parent: Partitioner, val partitionStartIndices: Array[Int])
  extends Partitioner {
  // 转换映射表
  // 将父分区 ID 映射到新的合并分区 ID
  @transient private lazy val parentPartitionMapping: Array[Int] = {
    val n = parent.numPartitions
    val result = new Array[Int](n)
    //  i: 新分区 ID (New Partition ID), 从 0 到 numPartitions - 1
    for (i <- partitionStartIndices.indices) {
      val start = partitionStartIndices(i) // 确定当前新分区的起始旧分区 ID
      //  如果不是最后一个新分区，结束点是下一个新分区的起始点
      val end = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      for (j <- start until end) {
        result(j) = i
      }
    }
    result
  }

  override def numPartitions: Int = partitionStartIndices.length

  override def getPartition(key: Any): Int = {
    parentPartitionMapping(parent.getPartition(key))
  }

  override def equals(other: Any): Boolean = other match {
    case c: CoalescedPartitioner =>
      c.parent == parent && Arrays.equals(c.partitionStartIndices, partitionStartIndices)
    case _ =>
      false
  }

  override def hashCode(): Int = 31 * parent.hashCode() + Arrays.hashCode(partitionStartIndices)
}

/**
 * This is a specialized version of [[org.apache.spark.rdd.ShuffledRDD]] that is optimized for
 * shuffling rows instead of Java key-value pairs. Note that something like this should eventually
 * be implemented in Spark core, but that is blocked by some more general refactorings to shuffle
 * interfaces / internals.
 *
 * This RDD takes a [[ShuffleDependency]] (`dependency`),
 * and an array of [[ShufflePartitionSpec]] as input arguments.
 *
 * The `dependency` has the parent RDD of this RDD, which represents the dataset before shuffle
 * (i.e. map output). Elements of this RDD are (partitionId, Row) pairs.
 * Partition ids should be in the range [0, numPartitions - 1].
 * `dependency.partitioner` is the original partitioner used to partition
 * map output, and `dependency.partitioner.numPartitions` is the number of pre-shuffle partitions
 * (i.e. the number of partitions of the map output).
 */
// ShuffledRowRDD 是 Spark SQL 内部使用的一种 RDD（弹性分布式数据集），它是一个专门为行（InternalRow）设计和优化的 ShuffledRDD 版本。
// 它的主要作用是从 Shuffle 的输出中读取数据
// 优化目的： 传统的 ShuffledRDD 处理的是 Java 键值对（Key-Value Pairs），而 ShuffledRowRDD 直接处理 Spark SQL 内部的紧凑行格式 InternalRow，从而减少了不必要的序列化/反序列化和对象创建，提高了 Spark SQL 交换（Exchange）操作（例如 Join 或 Group By 之后的 Shuffle 阶段）的性能
// 输入： 它接受一个 ShuffleDependency，这个依赖包含了上游 RDD（即 Shuffle 的 Map 阶段的输出）的信息，以及一个 ShufflePartitionSpec 数组，用于定义如何读取这些 Shuffle 输出块。
// 输出： 它产生的元素是 InternalRow，即 Shuffle 后的行数据。
// 分区规范： 它支持不同的分区读取策略（由 ShufflePartitionSpec 定义），如合并分区（CoalescedPartitionSpec）、部分 Reducer 读取（PartialReducerPartitionSpec）或部分 Mapper 读取（PartialMapperPartitionSpec），这使得它在执行计划中能够灵活地处理数据倾斜或进行更细粒度的控制。
class ShuffledRowRDD(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow], // Shuffle 依赖。 包含了上游 RDD 的信息、Shuffle Id、以及用于将 Map 输出数据分发到 Reducer 的 Partitioner
    metrics: Map[String, SQLMetric], // SQL 指标。 一个映射表，用于存储和报告与 Shuffle 读取操作相关的 SQL 执行指标（如 Shuffle 读取的字节数、记录数等）
    partitionSpecs: Array[ShufflePartitionSpec]) // 分区规范。 一个数组，其中的每个元素定义了当前 ShuffledRowRDD 的一个分区应该从 Shuffle 输出中读取哪些 Map 任务（Mapper）的哪些 Reducer 块（Reduce Blocks）
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  // 即每个原始 Reducer 块对应一个分区（使用 CoalescedPartitionSpec）
  def this(
      dependency: ShuffleDependency[Int, InternalRow, InternalRow],
      metrics: Map[String, SQLMetric]) = {
    this(dependency, metrics,
      Array.tabulate(dependency.partitioner.numPartitions)(i => CoalescedPartitionSpec(i, i + 1)))
  }

  dependency.rdd.context.setLocalProperty(
    SortShuffleManager.FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY,
    SQLConf.get.fetchShuffleBlocksInBatch.toString)
  // 获取依赖
  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner: Option[Partitioner] = {
    // 判断所有用于定义当前 RDD 分区的 ShufflePartitionSpec（即 partitionSpecs 数组中的元素）是否都是 CoalescedPartitionSpec 的实例
    // CoalescedPartitionSpec 表示将多个原始 Reducer 块合并为一个分区。如果不是全部是这种类型，则跳到最后的 else { None }
    if (partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec])) {
      val indices = partitionSpecs.map(_.asInstanceOf[CoalescedPartitionSpec].startReducerIndex)
      // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
      if (indices.toSet.size == partitionSpecs.length) {
        // 基于上游 Shuffle 的分区器（dependency.partitioner）和当前 RDD 的分区索引（indices）来定义其分区逻辑
        Some(new CoalescedPartitioner(dependency.partitioner, indices))
      } else {
        None
      }
    } else {
      None
    }
  }
  // 获取分区
  // 根据 partitionSpecs 数组，为 ShuffledRowRDD 的每个分区规范创建一个 ShuffledRowRDDPartition 实例数组
  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](partitionSpecs.length) { i =>
      ShuffledRowRDDPartition(i, partitionSpecs(i))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    partition.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        // TODO order by partition size.
        startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
          tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }

      case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

      case PartialMapperPartitionSpec(mapIndex, _, _) =>
        tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
    }
  }
  // 负责在一个 Executor 上执行任务，从 Shuffle 输出中读取所需的数据块，并将它们作为 InternalRow 迭代器返回
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)
    // 用于根据当前分区 (split) 的分区规范（spec）类型来确定如何获取 ShuffleReader
    val reader = split.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)

      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter)

      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          mapIndex,
          mapIndex + 1,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          0,
          numReducers,
          context,
          sqlMetricsReporter)
    }
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dependency = null
  }
}
