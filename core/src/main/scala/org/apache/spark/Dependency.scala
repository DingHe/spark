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

package org.apache.spark

import java.util.concurrent.ScheduledFuture

import scala.reflect.ClassTag

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteProcessor}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T] //rdd 是依赖的 RDD
}


/**
 * :: DeveloperApi ::  窄依赖指的是子 RDD 的每个分区只依赖于父 RDD 的少数几个分区
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int] //返回子 RDD 中某个分区依赖的父 RDD 中的分区列表

  override def rdd: RDD[T] = _rdd //使得 _rdd 成为该依赖的父 RDD
}


/**
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 * @param _rdd the parent RDD
 * @param partitioner partitioner used to partition the shuffle output
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If not set
 *                   explicitly then the default serializer, as specified by `spark.serializer`
 *                   config option, will be used.
 * @param keyOrdering key ordering for RDD's shuffles
 * @param aggregator map/reduce-side aggregator for RDD's shuffle
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine)
 * @param shuffleWriterProcessor the processor to control the write behavior in ShuffleMapTask
 */
// Spark 中用于表示宽依赖（Wide Dependency） 的核心类
// 在 Spark 的 DAG（有向无环图）中，一个 RDD 到另一个 RDD 的转换关系可以分为两种
// 窄依赖（Narrow Dependency）：父 RDD 的一个分区最多只被子 RDD 的一个分区所依赖。例如，map, filter 等转换
// 宽依赖（Wide Dependency）：父 RDD 的一个分区可能被子 RDD 的多个分区所依赖。例如，groupByKey, reduceByKey 等聚合和重分区操作
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]], // 表示此 ShuffleDependency 所依赖的父 RDD
    val partitioner: Partitioner,  //分区器，决定如何将数据分区
    val serializer: Serializer = SparkEnv.get.serializer, //用于序列化 shuffle 输出的序列化器
    val keyOrdering: Option[Ordering[K]] = None,  //键的排序方式
    val aggregator: Option[Aggregator[K, V, C]] = None, //可选的聚合器。用于在 Shuffle 过程中的Map 端进行部分聚合（map-side combine），以减少写入磁盘和通过网络传输的数据量
    val mapSideCombine: Boolean = false, //是否启用 map-side 聚合
    val shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor) //用于控制 shuffle 写操作的处理器
  extends Dependency[Product2[K, V]] with Logging {

  if (mapSideCombine) {
    require(aggregator.isDefined, "Map-side combine without Aggregator specified!")
  }
  // 返回父 RDD。由于 _rdd 是私有的，这个方法提供了对父 RDD 的公共访问
  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName  //key类的名称
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName  //value类的名称
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)  //combiner类的名称

  val shuffleId: Int = _rdd.context.newShuffleId()  //分配一个shuffle ID
  //用于在 ShuffleManager 中注册此 Shuffle。它是一个抽象类，提供了对 Shuffle 内部实现的统一访问
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, this)

  private[this] val numPartitions = rdd.partitions.length

  // By default, shuffle merge is allowed for ShuffleDependency if push based shuffle
  // is enabled
  // 表示是否允许Shuffle 合并（merge）。这是一个在推送式 Shuffle（Push-based shuffle） 下的新特性，用于优化 Shuffle 过程
  private[this] var _shuffleMergeAllowed = canShuffleMergeBeEnabled()
  // 用于设置是否允许 Shuffle 合并
  private[spark] def setShuffleMergeAllowed(shuffleMergeAllowed: Boolean): Unit = {
    _shuffleMergeAllowed = shuffleMergeAllowed
  }

  def shuffleMergeEnabled : Boolean = shuffleMergeAllowed && mergerLocs.nonEmpty

  def shuffleMergeAllowed : Boolean = _shuffleMergeAllowed

  /**
   * Stores the location of the list of chosen external shuffle services for handling the
   * shuffle merge requests from mappers in this shuffle map stage.
   */
  //存储用于处理此 Shuffle 合并请求的外部 Shuffle 服务位置列表
  private[spark] var mergerLocs: Seq[BlockManagerId] = Nil

  /**
   * Stores the information about whether the shuffle merge is finalized for the shuffle map stage
   * associated with this shuffle dependency
   */

  // 指示此 Shuffle Map 阶段的合并是否已完成
  private[this] var _shuffleMergeFinalized: Boolean = false

  /**
   * shuffleMergeId is used to uniquely identify merging process of shuffle
   * by an indeterminate stage attempt.
   */
  // 用于唯一标识一个不确定阶段尝试的合并过程
  private[this] var _shuffleMergeId: Int = 0

  def shuffleMergeId: Int = _shuffleMergeId

  def setMergerLocs(mergerLocs: Seq[BlockManagerId]): Unit = {
    assert(shuffleMergeAllowed)
    this.mergerLocs = mergerLocs
  }

  def getMergerLocs: Seq[BlockManagerId] = mergerLocs

  private[spark] def markShuffleMergeFinalized(): Unit = {
    _shuffleMergeFinalized = true
  }

  private[spark] def isShuffleMergeFinalizedMarked: Boolean = {
    _shuffleMergeFinalized
  }

  /**
   * Returns true if push-based shuffle is disabled or if the shuffle merge for
   * this shuffle is finalized.
   */
  def shuffleMergeFinalized: Boolean = {
    if (shuffleMergeEnabled) {
      isShuffleMergeFinalizedMarked
    } else {
      true
    }
  }

  def newShuffleMergeState(): Unit = {
    _shuffleMergeFinalized = false
    mergerLocs = Nil
    _shuffleMergeId += 1
    finalizeTask = None
    shufflePushCompleted.clear()
  }
  // 用于检查是否可以启用 Shuffle 合并。这取决于 spark.shuffle.push.enabled 配置、分区数和 RDD 是否为屏障 RDD
  private def canShuffleMergeBeEnabled(): Boolean = {
    val isPushShuffleEnabled = Utils.isPushBasedShuffleEnabled(rdd.sparkContext.getConf,
      // invoked at driver
      isDriver = true)
    if (isPushShuffleEnabled && rdd.isBarrier()) {
      logWarning("Push-based shuffle is currently not supported for barrier stages")
    }
    isPushShuffleEnabled && numPartitions > 0 &&
      // TODO: SPARK-35547: Push based shuffle is currently unsupported for Barrier stages
      !rdd.isBarrier()
  }

  @transient private[this] val shufflePushCompleted = new RoaringBitmap()

  /**
   * Mark a given map task as push completed in the tracking bitmap.
   * Using the bitmap ensures that the same map task launched multiple times due to
   * either speculation or stage retry is only counted once.
   * @param mapIndex Map task index
   * @return number of map tasks with block push completed
   */
  private[spark] def incPushCompleted(mapIndex: Int): Int = {
    shufflePushCompleted.add(mapIndex)
    shufflePushCompleted.getCardinality
  }

  // Only used by DAGScheduler to coordinate shuffle merge finalization
  @transient private[this] var finalizeTask: Option[ScheduledFuture[_]] = None

  private[spark] def getFinalizeTask: Option[ScheduledFuture[_]] = finalizeTask

  private[spark] def setFinalizeTask(task: ScheduledFuture[_]): Unit = {
    finalizeTask = Option(task)
  }

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
  _rdd.sparkContext.shuffleDriverComponents.registerShuffle(shuffleId)
}


/** 窄依赖，并且仅依赖一个父rdd分区
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
