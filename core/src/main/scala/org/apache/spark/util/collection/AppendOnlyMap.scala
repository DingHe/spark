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

import com.google.common.hash.Hashing

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements.
 * 简化的哈希表实现，特别优化了仅追加数据的场景，其中键不可删除，值可能会发生改变
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 */
// 这个只是内存中的Map存储，没有溢出操作
@DeveloperApi
class AppendOnlyMap[K, V](initialCapacity: Int = 64)
  extends Iterable[(K, V)] with Serializable {

  import AppendOnlyMap._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")
  //负载因子，默认为 0.7。它决定了哈希表何时需要扩容（即当元素数量超过当前容量的 70% 时）
  private val LOAD_FACTOR = 0.7
  //当前哈希表的容量，初始化时根据 initialCapacity 参数计算成一个 2 的幂
  private var capacity = nextPowerOf2(initialCapacity)
  private var mask = capacity - 1 //一个掩码值，用于计算哈希值的有效位置，等于 capacity - 1，保证哈希值可以按位与操作后得到有效的位置
  private var curSize = 0  //当前哈希表中已存储的键值对数量
  private var growThreshold = (LOAD_FACTOR * capacity).toInt  //触发扩容的阈值，当 curSize 达到该值时，哈希表会进行扩容

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.
  private var data = new Array[AnyRef](2 * capacity) //用于存储键值对。数组的每个偶数位置存储键，奇数位置存储值。数组大小是 2 * capacity，因为每个位置存储一个键和一个值

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  private var haveNullValue = false  //标识哈希表是否存储了空键对应的值
  private var nullValue: V = null.asInstanceOf[V]  //用于存储空键对应的值

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  private var destroyed = false //行破坏性排序时，标识哈希表的状态是否已经被破坏
  private val destructionMessage = "Map state is invalid from destructive sorting!"
  // 根据给定的键（key），在 Map 中查找对应的值（value）。
  // 这个方法的实现使用了开放寻址（Open Addressing） 的哈希表，并采用了二次探测（quadratic probing） 的冲突解决策略
  /** Get the value for a given key */
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    //检查键 k 是否为 null。在 AppendOnlyMap 的设计中，null 键被特殊处理。如果键为 null，它会直接返回一个专门为 null 键存储的 nullValue
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      return nullValue
    }
    //求出key的位置
    var pos = rehash(k.hashCode) & mask //mask 是一个等于 容量 - 1 的值，用于将哈希码映射到哈希表数组的有效索引范围内
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        return data(2 * pos + 1).asInstanceOf[V]
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[V]
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V]
  }
  //根据给定的键更新或插入键值对
  /** Set the value for a key */
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]

    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = value
      haveNullValue = true
      return
    }
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        data(2 * pos) = k
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        incrementSize()  // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) {
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }
  //通过一个更新函数 updateFunc 来更新键的值。updateFunc 接受两个参数：Boolean 表示该键是否已经存在，V 为旧的值或 null
  //可以更新Key的值，也就是支持合并元素
  /**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else if (k.eq(curKey) || k.equals(curKey)) {
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }
  //返回一个迭代器，用于遍历哈希表中的所有键值对
  /** Iterator method from Iterable */
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    new Iterator[(K, V)] {
      var pos = -1

      /** Get the next value we should return from next(), or null if we're finished iterating */
      def nextValue(): (K, V) = {
        if (pos == -1) {    // Treat position -1 as looking at the null value
          if (haveNullValue) {
            return (null.asInstanceOf[K], nullValue)
          }
          pos += 1
        }
        while (pos < capacity) {
          if (!data(2 * pos).eq(null)) {
            return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      override def next(): (K, V) = {
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }
  //返回当前哈希表中的元素数量
  override def size: Int = curSize

  /** Increase table size by 1, rehashing if necessary */
  private def incrementSize(): Unit = {
    curSize += 1
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   */
  //求出Key的Hash值
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()
  //扩容哈希表，将当前容量翻倍，并重新哈希所有键值对到新的哈希表中
  /** Double the table's size and re-hash everything */
  protected def growTable(): Unit = {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
    val newCapacity = capacity * 2
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
    val newData = new Array[AnyRef](2 * newCapacity)
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) {
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }
    data = newData
    capacity = newCapacity
    mask = newMask
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  /**
   * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
   */
  // 主要作用是返回一个按键排序的迭代器。这个过程是“破坏性的”，因为它会改变 AppendOnlyMap 底层的数据结构，使得该 Map 在操作完成后变得无效。
  // 这样做的好处是不需要额外的内存来执行排序，因为它直接在原地对数据进行处理
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    //将 AppendOnlyMap 的内部状态标记为已销毁。这确保了在方法执行完毕后，任何对该 Map 的其他操作（如 update 或 apply）都会触发断言失败
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    //keyIndex 用于遍历原始数组
    //newIndex 用于将非空（non-null）的键值对压缩到数组的前端
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {
        //将有效的键值对从旧位置（keyIndex）复制到新位置（newIndex）
        data(2 * newIndex) = data(2 * keyIndex)
        //复制与键对应的值
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))
    //Sorter 是 Spark 内部用于对数组进行高效排序的工具类
    // new KVArraySortDataFormat[K, AnyRef]: 为 Sorter 提供一个数据格式处理器。这个处理器告诉 Sorter 如何访问和比较 data 数组中的键值对
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)
    //创建一个匿名内部类，实现了 Scala 的 Iterator 接口。这个迭代器将按排序后的顺序返回键值对
    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

private object AppendOnlyMap {
  val MAXIMUM_CAPACITY = (1 << 29)  //哈希表允许的最大容量（2^29），大概5亿
}
