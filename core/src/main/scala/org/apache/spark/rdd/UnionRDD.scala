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

package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.reflect.ClassTag

import org.apache.spark.{Dependency, Partition, RangeDependency, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.config.RDD_PARALLEL_LISTING_THRESHOLD
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Partition for UnionRDD.
 *
 * @param idx index of the partition
 * @param rdd the parent RDD this partition refers to
 * @param parentRddIndex index of the parent RDD this partition refers to
 * @param parentRddPartitionIndex index of the partition within the parent RDD
 *                                this partition refers to
 */
// UnionPartition 是 UnionRDD 的分区实现，它封装了对父 RDD 分区的引用。
private[spark] class UnionPartition[T: ClassTag](
    idx: Int, // 当前 UnionRDD 分区在整个 UnionRDD 分区数组中的全局索引（0 到 总分区数−1）
    @transient private val rdd: RDD[T], // 父 RDD 引用。 当前分区所属的那个父 RDD。由于使用了 @transient 关键字，它在序列化到 Executor 时不会被传输，而是在 writeObject 中重建
    val parentRddIndex: Int, // 父 RDD 索引。 当前分区所属的父 RDD 在 UnionRDD 构造函数传入的 rdds 序列中的索引。
    @transient private val parentRddPartitionIndex: Int) // 父分区索引。 当前分区所引用的父 RDD 内部的分区索引。同样使用 @transient。
  extends Partition {
  // 父分区对象。 实际引用的父 RDD 的 Partition 对象。它通过 rdd.partitions(parentRddPartitionIndex) 在 Driver 端和 Executor 端初始化
  var parentPartition: Partition = rdd.partitions(parentRddPartitionIndex)

  def preferredLocations(): Seq[String] = rdd.preferredLocations(parentPartition)

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    parentPartition = rdd.partitions(parentRddPartitionIndex)
    oos.defaultWriteObject()
  }
}

object UnionRDD {
  private[spark] lazy val partitionEvalTaskSupport =
    new ForkJoinTaskSupport(ThreadUtils.newForkJoinPool("partition-eval-task-support", 8))
}
// UnionRDD 的作用是**逻辑上联合（Union）**多个 RDD，将它们视为一个单一的 RDD。
// 实现了 Spark 的 union 操作（例如 rdd1.union(rdd2) 或 sc.union(Seq(rdd1, rdd2))）
// 分区联合： UnionRDD 的分区是其所有父 RDD 分区的简单串联。如果 RDD A 有 3 个分区，RDD B 有 5 个分区，那么 UnionRDD(A, B) 将会有 8 个分区。
// 不发生数据移动： UnionRDD 是一种窄依赖（Narrow Dependency）。它本身不包含任何数据，也不执行任何数据 shuffle 操作。它的每个分区都直接映射并指向其一个父 RDD 的一个分区，实现了高效的逻辑合并。

@DeveloperApi
class UnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]]) // 父 RDD 序列 rdds。注意 rdds 被声明为 var，因为它在 clearDependencies 中会被置为 null
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies

  // visible for testing
  // 判断是否以并行方式计算分区总数。
  // 如果父 RDD 的数量超过配置的阈值 (RDD_PARALLEL_LISTING_THRESHOLD)，则设置为 true，以避免在 Driver 上花费过多时间来获取所有父 RDD 的分区信息
  private[spark] val isPartitionListingParallel: Boolean =
    rdds.length > conf.get(RDD_PARALLEL_LISTING_THRESHOLD)
  // 计算 RDD 分区数组
  override def getPartitions: Array[Partition] = {
    val parRDDs = if (isPartitionListingParallel) {
      // scalastyle:off parvector
      // ParVector 是 Scala 标准库里 并行集合（Parallel Collections） 的一个重要实现，它对应的是普通 Vector 的并行版本
      // 多线程并行执行（ForkJoinPool）
      val parArray = new ParVector(rdds.toVector)
      parArray.tasksupport = UnionRDD.partitionEvalTaskSupport
      // scalastyle:on parvector
      parArray
    } else {
      rdds
    }
    val array = new Array[Partition](parRDDs.map(_.partitions.length).sum)
    var pos = 0
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      pos += 1
    }
    array
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
      pos += rdd.partitions.length
    }
    deps.toSeq
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val part = s.asInstanceOf[UnionPartition[T]]
    parent[T](part.parentRddIndex).iterator(part.parentPartition, context)
  }

  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
