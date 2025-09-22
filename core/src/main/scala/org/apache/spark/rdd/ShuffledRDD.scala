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

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer

private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
}

/**
 * :: DeveloperApi ::
 * 进行数据shuffle操作的RDD（弹性分布式数据集）。此类用于表示一个经过shuffle处理的数据集，通常用于数据重分区等操作。
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param prev the parent RDD.  父RDD
 * @param part the partitioner used to partition the RDD  分区器（part）
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class.
 */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends RDD[(K, C)](prev.context, Nil) {
  //prev: RDD[_ <: Product2[K, V]]：表示父RDD，它的数据类型为键值对(K, V)
  //part: Partitioner：用于将数据划分为多个分区的分区器

  private var userSpecifiedSerializer: Option[Serializer] = None  //用户指定的序列化器，用于定制shuffle时的序列化方式

  private var keyOrdering: Option[Ordering[K]] = None //键的排序方式，决定数据在shuffle过程中是否需要排序

  private var aggregator: Option[Aggregator[K, V, C]] = None  //聚合器，用于定义如何合并键值对中的值

  private var mapSideCombine: Boolean = false  //是否在map端进行合并，减少数据传输量

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, C] = {
    this.userSpecifiedSerializer = Option(serializer)
    this
  }

  /** Set key ordering for RDD's shuffle. */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  /** Set aggregator for RDD's shuffle. */
  def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  def setMapSideCombine(mapSideCombine: Boolean): ShuffledRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }
  //返回当前RDD的依赖关系
  override def getDependencies: Seq[Dependency[_]] = {
    val serializer = userSpecifiedSerializer.getOrElse { //如果用户指定了序列化器，就使用该序列化器；如果没有指定，就会使用默认的序列化器
      val serializerManager = SparkEnv.get.serializerManager
      if (mapSideCombine) {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[C]])
      } else {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
      }
    }
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
    //prev：当前RDD的父RDD，表示数据源
    //part：分区器，用于指定如何将数据划分为多个分区
    //serializer：序列化器，用于在shuffle过程中序列化和反序列化数据
    //aggregator：聚合器，用于定义如何对数据进行合并操作
    //mapSideCombine：表示是否在map端进行合并操作的标志
  }

  override val partitioner = Some(part)
  //Array.tabulate是一个非常有用的Scala集合操作方法，它可以创建一个数组，并且在创建过程中对每个元素进行初始化。
  // 此处，Array.tabulate会创建一个Partition类型的数组，数组的大小为part.numPartitions，即当前RDD的分区数
  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    //MapOutputTrackerMaster负责管理Shuffle操作的元数据
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    //通过tracker获取shuffle数据的位置信息
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }
  //负责实际计算某个分区的数据并返回一个Iterator[(K, C)]，即返回一个键值对的迭代器。
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    //由于ShuffledRDD通常只有一个父RDD依赖，所以取第一个依赖项（dependencies.head）
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    val metrics = context.taskMetrics().createTempShuffleReadMetrics()
    SparkEnv.get.shuffleManager.getReader(
      dep.shuffleHandle, split.index, split.index + 1, context, metrics)
      .read()
      .asInstanceOf[Iterator[(K, C)]]

    //split.index：当前分区的索引，表示要读取哪个分区的数据。
    //split.index + 1：表示读取的是一个单一的分区，通常在Shuffle操作中，数据是按照分区索引进行划分的
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  private[spark] override def isBarrier(): Boolean = false
}
