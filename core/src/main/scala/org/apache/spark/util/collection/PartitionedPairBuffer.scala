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

import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * Append-only buffer of key-value pairs, each with a corresponding partition ID, that keeps track
 * of its estimated size in bytes.
 * The buffer can support up to 1073741819 elements.
 */
//  Spark 内部用于在内存中暂存带有分区 ID 的键值对的缓冲区。
//  它以可增长的数组形式实现，专门用于没有Map端聚合（map-side combine） 的 Shuffle 场景，例如 groupByKey、repartition 等操作
// 与 AppendOnlyMap 不同，PartitionedPairBuffer 不会进行键值对的聚合，它只是简单地将每个 (key, value) 对和其分区 ID 一起追加到内存数组中
// 实现了 WritablePartitionedPairCollection 特质，使得它的内容可以被排序，并通过迭代器直接高效地写入到磁盘或网络中
// 只是一个 缓冲区（buffer），它内部用的是 数组，存储按插入顺序追加的键值对
// 它不会做 key 的合并（merge），即使 key 相同也会保留多份
// 主要用于 shuffle map 端，先存储所有结果，之后再写磁盘文件
private[spark] class PartitionedPairBuffer[K, V](initialCapacity: Int = 64)
  extends WritablePartitionedPairCollection[K, V] with SizeTracker
{
  import PartitionedPairBuffer._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  // Basic growable array data structure. We use a single array of AnyRef to hold both the keys
  // and the values, so that we can sort them efficiently with KVArraySortDataFormat.
  //缓冲区的当前容量
  private var capacity = initialCapacity
  //缓冲区中当前存储的元素对数量
  private var curSize = 0
  //将每个键值对及其分区 ID 作为一个元组 (partition, key)，然后与值 value 一起存储
  private var data = new Array[AnyRef](2 * initialCapacity)

  /** Add an element into the buffer */
  // 向缓冲区中插入一个带有分区 ID 的键值对
  def insert(partition: Int, key: K, value: V): Unit = {
    if (curSize == capacity) {
      growArray()
    }
    data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
    data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
    curSize += 1
    afterUpdate()
  }

  /** Double the size of the array because we've reached capacity */
  // 当缓冲区达到容量时，用于扩展底层数组
  private def growArray(): Unit = {
    if (capacity >= MAXIMUM_CAPACITY) {
      throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
    }
    val newCapacity =
      if (capacity * 2 > MAXIMUM_CAPACITY) { // Overflow
        MAXIMUM_CAPACITY
      } else {
        capacity * 2
      }
    //创建新的数组，然后内存拷贝全部的元素
    val newArray = new Array[AnyRef](2 * newCapacity)
    System.arraycopy(data, 0, newArray, 0, 2 * capacity)
    data = newArray
    capacity = newCapacity
    resetSamples()
  }

  /** Iterate through the data in a given order. For this class this is not really destructive. */
  // 返回一个排序后的迭代器
  override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
    iterator
  }
  //返回一个用于遍历已排序数组的迭代器
  private def iterator(): Iterator[((Int, K), V)] = new Iterator[((Int, K), V)] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): ((Int, K), V) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val pair = (data(2 * pos).asInstanceOf[(Int, K)], data(2 * pos + 1).asInstanceOf[V])
      pos += 1
      pair
    }
  }
}

private object PartitionedPairBuffer {
  val MAXIMUM_CAPACITY: Int = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 2
}
