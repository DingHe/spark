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
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.{CompactBuffer, ExternalAppendOnlyMap}

/**
 * The references to rdd and splitIndex are transient because redundant information is stored
 * in the CoGroupedRDD object.  Because CoGroupedRDD is serialized separately from
 * CoGroupPartition, if rdd and splitIndex aren't transient, they'll be included twice in the
 * task closure.
 */
//主要用于表示和保存一个父 RDD 中某个分区的信息，并且支持序列化
//rdd 和 splitIndex 使用 @transient 修饰符，意味着这两个字段不会被序列化
private[spark] case class NarrowCoGroupSplitDep(
    @transient rdd: RDD[_],  //父 RDD 的引用
    @transient splitIndex: Int, //父 RDD 中当前分区的索引
    var split: Partition  //表示当前父 RDD 分区的 Partition 对象，表示特定分区的数据
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    //序列化时，更新 split为实际引用的父rdd分区
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()  //调用默认的序列化方法，将除 transient 字段之外的对象状态序列化到输出流中
  }
}

/**
 * Stores information about the narrow dependencies used by a CoGroupedRdd.
 *
 * @param narrowDeps maps to the dependencies variable in the parent RDD: for each one to one
 *                   dependency in dependencies, narrowDeps has a NarrowCoGroupSplitDep (describing
 *                   the partition for that dependency) at the corresponding index. The size of
 *                   narrowDeps should always be equal to the number of parents.
 */
private[spark] class CoGroupPartition(
    override val index: Int, val narrowDeps: Array[Option[NarrowCoGroupSplitDep]])
  extends Partition with Serializable {
  override def hashCode(): Int = index
  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * :: DeveloperApi ::
 * An RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * @param rdds parent RDDs.
 * @param part partitioner used to partition the shuffle output
 *
 * @note This is an internal API. We recommend users use RDD.cogroup(...) instead of
 * instantiating this directly.
 */
@DeveloperApi
class CoGroupedRDD[K: ClassTag](
    @transient var rdds: Seq[RDD[_ <: Product2[K, _]]],
    part: Partitioner)
  extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {
  //rdds: 该属性表示输入的多个父 RDD，这些 RDD 都是按键值对的形式组织的，K 为键类型，_ 为值类型
  //part: 用于指定分区操作的 Partitioner

  // For example, `(k, a) cogroup (k, b)` produces k -> Array(ArrayBuffer as, ArrayBuffer bs).
  // Each ArrayBuffer is represented as a CoGroup, and the resulting Array as a CoGroupCombiner.
  // CoGroupValue is the intermediate state of each value before being merged in compute.
  private type CoGroup = CompactBuffer[Any] //表示一个键对应的所有值集合，每个值来自不同的父 RDD。
  private type CoGroupValue = (Any, Int)  // Int is dependency number 第一个元素是实际的数据（如某个父 RDD 的数据），第二个元素是依赖编号，表示该数据来源于第几个父 RDD
  private type CoGroupCombiner = Array[CoGroup] //合并所有父 RDD 中相同键的值的容器

  private var serializer: Serializer = SparkEnv.get.serializer

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): CoGroupedRDD[K] = {
    this.serializer = serializer
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_] =>
      if (rdd.partitioner == Some(part)) {  //如果父 RDD 使用了和当前 CoGroupedRDD 相同的分区器（part），则为该 RDD 创建一个 "一对一" 依赖
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else { //如果父 RDD 没有使用与当前 RDD 相同的分区器，则创建一个 ShuffleDependency
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[K, Any, CoGroupCombiner](
          rdd.asInstanceOf[RDD[_ <: Product2[K, _]]], part, serializer)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- array.indices) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>  //如果当前依赖是 ShuffleDependency（即需要通过 shuffle 操作来重新分配数据），则返回 None
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i))) //NarrowCoGroupSplitDep 描述的是当前父 RDD 的某个分区与当前分区之间的窄依赖关系
        }
      }.toArray)
    }
    array
  }

  override val partitioner: Some[Partitioner] = Some(part)

  //主要负责对每个分区的数据进行计算，并返回一个包含键值对 (K, Array[Iterable[_]]) 的迭代器
  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = dependencies.length

    // A list of (rdd iterator, dependency number) pairs
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      case oneToOneDependency: OneToOneDependency[Product2[K, Any]] @unchecked =>
        //表示当前父RDD与子RDD之间存在一对一依赖关系。如果父RDD和子RDD使用相同的分区器，则此类型适用。此时，我们从父 RDD 中获取对应分区的迭代器
        val dependencyPartition = split.narrowDeps(depNum).get.split
        // Read them from the parent
        val it = oneToOneDependency.rdd.iterator(dependencyPartition, context)
        rddIterators += ((it, depNum))

      case shuffleDependency: ShuffleDependency[_, _, _] =>
        // Read map outputs of shuffle
        //表示当前父RDD和子RDD 之间存在 shuffle 依赖关系。shuffle 操作涉及跨分区的数据传输，因此我们需要从 shuffle 文件中读取数据
        val metrics = context.taskMetrics().createTempShuffleReadMetrics()
        val it = SparkEnv.get.shuffleManager
          .getReader(
            shuffleDependency.shuffleHandle, split.index, split.index + 1, context, metrics)
          .read()
        rddIterators += ((it, depNum))
    }
    //这个数据结构会在后面用来存储并合并来自多个父 RDD 的数据
    val map = createExternalMap(numRdds)
    for ((it, depNum) <- rddIterators) {
      map.insertAll(it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum))))
    }
    context.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    new InterruptibleIterator(context,
      map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
  }



  //numRdds 有多少个父 RDD 参与到当前的 cogroup 操作
  private def createExternalMap(numRdds: Int)
    : ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner] = {
     //CoGroupValue 是一个二元组 (Any, Int)，其中：
    //第一个元素 value._1 是实际的数据（如某个父 RDD 的数据）。
    //第二个元素 value._2 是依赖编号，表示该数据来源于第几个父 RDD。
    val createCombiner: (CoGroupValue => CoGroupCombiner) = value => {
      //newCombiner 是一个 CoGroupCombiner，它是一个数组，数组大小为 numRdds（即参与的父 RDD 数量）。每个元素是一个 CoGroup（CompactBuffer[Any]）
      val newCombiner = Array.fill(numRdds)(new CoGroup)
      newCombiner(value._2) += value._1    //将CoGroupValue中的第一个元素（即数据）追加到对应父 RDD 的 CoGroup 中
      newCombiner
    }
    //它将CoGroupValue中的数据（value._1）追加到已有的CoGroupCombiner 中，value._2 表示数据所属的父 RDD 的编号
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
      combiner(value._2) += value._1
      combiner
    }

    val mergeCombiners: (CoGroupCombiner, CoGroupCombiner) => CoGroupCombiner =
      (combiner1, combiner2) => {  //它遍历两个 CoGroupCombiner，并将第二个 combiner 中的每个 CoGroup 的内容追加到第一个 combiner 中
        var depNum = 0
        while (depNum < numRdds) {
          combiner1(depNum) ++= combiner2(depNum)
          depNum += 1
        }
        combiner1
      }
    new ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner](
      createCombiner, mergeValue, mergeCombiners)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
