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

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.util.Utils

private[spark]
class CartesianPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

// 计算两个 RDD 之间的笛卡尔积
// 笛卡尔积是一种 Join 操作，它将第一个 RDD (rdd1) 中的每一条记录与第二个 RDD (rdd2) 中的所有记录进行组合。
// 输入： 两个 RDD，rdd1: RDD[T] 和 rdd2: RDD[U]。
// 输出： 一个新的 RDD，其中包含所有可能的配对 (T, U) 记录，类型为 RDD[(T, U)]
// 实现原理： 它通过组合 rdd1 的每个分区和 rdd2 的每个分区来创建一个新的分区。如果 rdd1 有 $N_1$ 个分区，rdd2 有 $N_2$ 个分区，则 CartesianRDD 将有 $N_1 \times N_2$ 个分区。
private[spark]
class CartesianRDD[T: ClassTag, U: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[T], // 第一个源 RDD
    var rdd2 : RDD[U]) // 第二个源 RDD。
  extends RDD[(T, U)](sc, Nil)
  with Serializable {

  val numPartitionsInRdd2 = rdd2.partitions.length

  // 获取新 RDD 的分区列表。
  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }
  // 计算给定分区的元素。
  override def compute(split: Partition, context: TaskContext): Iterator[(T, U)] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    for (x <- rdd1.iterator(currSplit.s1, context);
         y <- rdd2.iterator(currSplit.s2, context)) yield (x, y)
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
