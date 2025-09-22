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

import org.apache.spark.{Partition, PartitionEvaluatorFactory, TaskContext}
//用于在每个分区上使用一个 PartitionEvaluator 来处理数据
private[spark] class MapPartitionsWithEvaluatorRDD[T : ClassTag, U : ClassTag](
    var prev: RDD[T], //prev：表示当前 RDD 的父 RDD，即前一个计算结果（类型为 RDD[T]）。它是该 RDD 的输入
    evaluatorFactory: PartitionEvaluatorFactory[T, U])  //根据传入的输入数据类型 T 和输出数据类型 U，为每个分区创建一个合适的 PartitionEvaluator 实例
  extends RDD[U](prev) {
  //返回当前 RDD 的所有分区。由于 MapPartitionsWithEvaluatorRDD 是对父 RDD 的一个转换操作，因此它的分区就是父 RDD 的分区
  override def getPartitions: Array[Partition] = firstParent[T].partitions
  //split：当前计算的分区
  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    val evaluator = evaluatorFactory.createEvaluator()
    val input = firstParent[T].iterator(split, context)
    evaluator.eval(split.index, input)
  }
  //用于清除当前 RDD 对父 RDD 的依赖，在计算完成后，prev 被置为 null，表示不再需要父 RDD 的引用，以帮助垃圾回收
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}
