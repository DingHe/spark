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

package org.apache.spark.util.collection

import java.util.Comparator


/**
 * A common interface for size-tracking collections of key-value pairs that
 *  - Have an associated partition for each key-value pair.
 *  - Support a memory-efficient sorted iterator
 *  - Support a WritablePartitionedIterator for writing the contents directly as bytes.
 */
// 定义了可写出的、带分区的键值对集合所应具备的公共接口。这个特质是 Spark 内部用于在内存中缓冲 Shuffle 数据的核心抽象

private[spark] trait WritablePartitionedPairCollection[K, V] {
  /**
   * Insert a key-value pair with a partition into the collection
   */
  // 向集合中插入一个键值对。除了键 key 和值 value 外，还需要指定这个键值对所属的分区ID。这是因为 Shuffle 数据是按分区进行组织的
  def insert(partition: Int, key: K, value: V): Unit

  /**
   * Iterate through the data in order of partition ID and then the given comparator. This may
   * destroy the underlying collection.
   */
  //返回一个按分区 ID 和可选的键比较器排序后的迭代器
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)]

  /**
   * Iterate through the data and write out the elements instead of returning them. Records are
   * returned in order of their partition ID and then the given comparator.
   * This may destroy the underlying collection.
   */
  // 返回一个可写出的、带分区的迭代器
  // 过调用 partitionedDestructiveSortedIterator 获得一个排序后的迭代器，
  // 然后用这个迭代器构造并返回一个 WritablePartitionedIterator。这种设计将排序逻辑和写入逻辑解耦
  def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]])
    : WritablePartitionedIterator[K, V] = {
    val it = partitionedDestructiveSortedIterator(keyComparator)
    new WritablePartitionedIterator[K, V](it)
  }
}

private[spark] object WritablePartitionedPairCollection {
  /**  返回一个只按分区 ID 进行比较的比较器
   * A comparator for (Int, K) pairs that orders them by only their partition ID.
   */
  def partitionComparator[K]: Comparator[(Int, K)] = (a: (Int, K), b: (Int, K)) => a._1 - b._1

  /** 返回一个同时按分区 ID 和键进行比较的比较器
   * A comparator for (Int, K) pairs that orders them both by their partition ID and a key ordering.
   */
  def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] =
    (a: (Int, K), b: (Int, K)) => {
      val partitionDiff = a._1 - b._1
      if (partitionDiff != 0) {
        partitionDiff
      } else {
        keyComparator.compare(a._2, b._2)
      }
    }
}

/**
 * Iterator that writes elements to a DiskBlockObjectWriter instead of returning them. Each element
 * has an associated partition.
 */
// 封装了一个已排序的迭代器，并提供了专门用于写入数据的方法
private[spark] class WritablePartitionedIterator[K, V](it: Iterator[((Int, K), V)]) {
  private[this] var cur = if (it.hasNext) it.next() else null
   //通过PairsWriter把数据写出
  def writeNext(writer: PairsWriter): Unit = {
    writer.write(cur._1._2, cur._2)
    cur = if (it.hasNext) it.next() else null
  }

  def hasNext: Boolean = cur != null

  def nextPartition(): Int = cur._1._1
}
