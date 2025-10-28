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

package org.apache.spark.sql.execution.joins

import java.io._

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.config.{BUFFER_PAGESIZE, MEMORY_OFFHEAP_ENABLED}
import org.apache.spark.memory._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.{KnownSizeEstimation, Utils}

/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
// 代表一个已经构建完成、基于哈希键组织的关系表（通常是 Join 操作中较小的那个表，即 Build Side）。它是一个高度优化的查找结构，用于在 Join 期间提供快速的键查找能力
// 核心职能： 封装了不同类型的哈希结构（如基于内存数组、基于 BytesToBytesMap 或其他自定义结构），提供统一的 API，允许查询另一侧（Probe Side）的数据记录，快速查找匹配的行。
// 应用场景： 主要用于 Spark SQL 的物理操作符，如 BroadcastHashJoinExec，在查询执行器上查找被广播或 Shuffle 后的数据。
private[execution] sealed trait HashedRelation extends KnownSizeEstimation {
  /**
   * Returns matched rows.
   *
   * Returns null if there is no matched rows.
   */
  // 获取匹配行（多行）。
  // 根据给定的 Join Key (InternalRow 类型) 进行查找。返回一个匹配行（即被 Join 的另一侧数据）的迭代器。如果没有匹配行，则返回 null
  def get(key: InternalRow): Iterator[InternalRow]

  /**
   * Returns matched rows for a key that has only one column with LongType.
   *
   * Returns null if there is no matched rows.
   */
  // 获取匹配行（LongKey 优化）
  // 针对单列且列类型为 LongType 的特殊 Join Key 优化。提供更快的基于原始 Long 值的查找
  def get(key: Long): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns the matched single row.
   */
  // 获取匹配的单行
  // 用于键唯一（keyIsUnique = true）或只需要返回第一个匹配行的场景。返回匹配的单行数据。
  def getValue(key: InternalRow): InternalRow

  /**
   * Returns the matched single row with key that have only one column of LongType.
   */
  // 获取匹配的单行（LongKey 优化）。
  // 针对单列 LongType 键的单行查找优化
  def getValue(key: Long): InternalRow = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns an iterator for key index and matched rows.
   *
   * Returns null if there is no matched rows.
   */
  // 获取键索引和匹配行。
  // 根据键查找匹配行，返回一个包含键索引和匹配行的迭代器
  def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns key index and matched single row.
   * This is for unique key case.
   *
   * Returns null if there is no matched rows.
   */
  // 获取键索引和匹配单行。
  // 针对唯一键的场景，返回包含键索引和匹配单行的封装对象。
  def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns an iterator for keys index and rows of InternalRow type.
   */
  // 迭代所有行和键索引。
  // 返回一个迭代器，遍历哈希关系中所有存储的行及其对应的键索引。
  def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns the maximum number of allowed keys index.
   */
  // 返回最大键索引数
  // 返回哈希关系中允许的最大键索引数量。用于预分配数组或管理索引空间。
  def maxNumKeysIndex: Int = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns true iff all the keys are unique.
   */
  // 键是否唯一
  // 返回 true 当且仅当该哈希关系中的所有键都是唯一的。如果为 true，查找操作可以优化为只返回单个匹配行（通过 getValue）
  def keyIsUnique: Boolean

  /**
   * Returns an iterator for keys of InternalRow type.
   */
  // 返回所有键的迭代器。
  // 返回一个迭代器，遍历哈希关系中存储的所有 Join Key（InternalRow 类型）
  def keys(): Iterator[InternalRow]

  /**
   * Returns a read-only copy of this, to be safely used in current thread.
   */
  // 创建只读副本
  def asReadOnlyCopy(): HashedRelation

  /**
   * Release any used resources.
   */
  // 释放资源
  def close(): Unit
}

private[execution] object HashedRelation {

  /**
   * Create a HashedRelation from an Iterator of InternalRow.
   *
   * @param allowsNullKey        Allow NULL keys in HashedRelation.
   * @param ignoresDuplicatedKey Ignore rows with duplicated keys in HashedRelation.
   *                             This is only used for semi and anti join without join condition in
   *                             `ShuffledHashJoinExec` only.
   */
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int = 64,
      taskMemoryManager: TaskMemoryManager = null,
      isNullAware: Boolean = false,
      allowsNullKey: Boolean = false,
      ignoresDuplicatedKey: Boolean = false): HashedRelation = {
    val mm = Option(taskMemoryManager).getOrElse {
      new TaskMemoryManager(
        new UnifiedMemoryManager(
          new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"),
          Long.MaxValue,
          Long.MaxValue / 2,
          1),
        0)
    }

    if (!input.hasNext && !allowsNullKey) {
      EmptyHashedRelation
    } else if (key.length == 1 && key.head.dataType == LongType && !allowsNullKey) {
      // NOTE: LongHashedRelation does not support NULL keys.
      LongHashedRelation(input, key, sizeEstimate, mm, isNullAware, ignoresDuplicatedKey)
    } else {
      UnsafeHashedRelation(input, key, sizeEstimate, mm, isNullAware, allowsNullKey,
        ignoresDuplicatedKey)
    }
  }
}

/**
 * A wrapper for key index and value in InternalRow type.
 * Designed to be instantiated once per thread and reused.
 */
private[execution] class ValueRowWithKeyIndex {
  private var keyIndex: Int = _
  private var value: InternalRow = _

  /** Updates this ValueRowWithKeyIndex by updating its key index.  Returns itself. */
  def withNewKeyIndex(newKeyIndex: Int): ValueRowWithKeyIndex = {
    keyIndex = newKeyIndex
    this
  }

  /** Updates this ValueRowWithKeyIndex by updating its value.  Returns itself. */
  def withNewValue(newValue: InternalRow): ValueRowWithKeyIndex = {
    value = newValue
    this
  }

  /** Updates this ValueRowWithKeyIndex.  Returns itself. */
  def update(newKeyIndex: Int, newValue: InternalRow): ValueRowWithKeyIndex = {
    keyIndex = newKeyIndex
    value = newValue
    this
  }

  def getKeyIndex: Int = {
    keyIndex
  }

  def getValue: InternalRow = {
    value
  }
}

/**
 * A HashedRelation for UnsafeRow, which is backed BytesToBytesMap.
 *
 * It's serialized in the following format:
 *  [number of keys] [number of fields]
 *  [size of key] [size of value] [key bytes] [bytes for value]
 */
// 存储和查找数据： 它以键值对的形式高效存储一个关系表（通常是广播连接中的小表），键和值都是 UnsafeRow 格式
// 利用堆外内存： 它使用 Spark 的堆外内存管理工具 BytesToBytesMap 作为底层存储。这使得数据的存储和查找操作完全在序列化字节数组上进行，避免了 JVM 对象的开销和垃圾回收（GC）的压力，极大地提高了 Join 查找的性能和内存效率。
// 支持序列化： 它实现了 Externalizable 和 KryoSerializable 接口，意味着它可以被高效地序列化和反序列化，这对于通过网络进行广播（Broadcast）传输至其他 Executor 节点至关重要。
private[joins] class UnsafeHashedRelation(
    private var numKeys: Int, // 键的字段数量，Join Key 中包含的字段个数。用于后续创建正确的 UnsafeRow 实例来表示键。
    private var numFields: Int, // 值的字段数量。被哈希存储的行（值）中包含的字段个数。用于后续创建正确的 UnsafeRow 实例来表示值。
    private var binaryMap: BytesToBytesMap)
  extends HashedRelation with Externalizable with KryoSerializable {

  private[joins] def this() = this(0, 0, null)  // Needed for serialization

  override def keyIsUnique: Boolean = binaryMap.numKeys() == binaryMap.numValues()

  override def asReadOnlyCopy(): UnsafeHashedRelation = {
    new UnsafeHashedRelation(numKeys, numFields, binaryMap)
  }

  override def estimatedSize: Long = binaryMap.getTotalMemoryConsumption

  // re-used in get()/getValue()/getWithKeyIndex()/getValueWithKeyIndex()/valuesWithKeyIndex()
  // 结果行复用对象
  var resultRow = new UnsafeRow(numFields)

  // re-used in getWithKeyIndex()/getValueWithKeyIndex()/valuesWithKeyIndex()
  // 键索引结果复用对象
  val valueRowWithKeyIndex = new ValueRowWithKeyIndex
  // 获取匹配行迭代器。
  override def get(key: InternalRow): Iterator[InternalRow] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      new Iterator[UnsafeRow] {
        private var _hasNext = true
        override def hasNext: Boolean = _hasNext
        override def next(): UnsafeRow = {
          resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
          _hasNext = loc.nextValue()
          resultRow
        }
      }
    } else {
      null
    }
  }

  def getValue(key: InternalRow): InternalRow = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      resultRow
    } else {
      null
    }
  }

  override def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      valueRowWithKeyIndex.withNewKeyIndex(loc.getKeyIndex)
      new Iterator[ValueRowWithKeyIndex] {
        private var _hasNext = true
        override def hasNext: Boolean = _hasNext
        override def next(): ValueRowWithKeyIndex = {
          resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
          _hasNext = loc.nextValue()
          valueRowWithKeyIndex.withNewValue(resultRow)
        }
      }
    } else {
      null
    }
  }

  override def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      valueRowWithKeyIndex.update(loc.getKeyIndex, resultRow)
    } else {
      null
    }
  }

  override def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    val iter = binaryMap.iteratorWithKeyIndex()

    new Iterator[ValueRowWithKeyIndex] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): ValueRowWithKeyIndex = {
        if (!hasNext) {
          throw QueryExecutionErrors.endOfIteratorError()
        }
        val loc = iter.next()
        resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
        valueRowWithKeyIndex.update(loc.getKeyIndex, resultRow)
      }
    }
  }

  override def maxNumKeysIndex: Int = {
    binaryMap.maxNumKeysIndex
  }

  override def keys(): Iterator[InternalRow] = {
    val iter = binaryMap.iterator()

    new Iterator[InternalRow] {
      val unsafeRow = new UnsafeRow(numKeys)

      override def hasNext: Boolean = {
        iter.hasNext
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw QueryExecutionErrors.endOfIteratorError()
        } else {
          val loc = iter.next()
          unsafeRow.pointTo(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
          unsafeRow
        }
      }
    }
  }

  override def close(): Unit = {
    binaryMap.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeInt, out.writeLong, out.write)
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeInt, out.writeLong, out.write)
  }

  private def write(
      writeInt: (Int) => Unit,
      writeLong: (Long) => Unit,
      writeBuffer: (Array[Byte], Int, Int) => Unit) : Unit = {
    writeInt(numKeys)
    writeInt(numFields)
    // TODO: move these into BytesToBytesMap
    writeLong(binaryMap.numKeys())
    writeLong(binaryMap.numValues())

    var buffer = new Array[Byte](64)
    def write(base: Object, offset: Long, length: Int): Unit = {
      if (buffer.length < length) {
        buffer = new Array[Byte](length)
      }
      Platform.copyMemory(base, offset, buffer, Platform.BYTE_ARRAY_OFFSET, length)
      writeBuffer(buffer, 0, length)
    }

    val iter = binaryMap.iterator()
    while (iter.hasNext) {
      val loc = iter.next()
      // [key size] [values size] [key bytes] [value bytes]
      writeInt(loc.getKeyLength)
      writeInt(loc.getValueLength)
      write(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
      write(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readInt(), () => in.readLong(), in.readFully)
  }

  private def read(
      readInt: () => Int,
      readLong: () => Long,
      readBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    numKeys = readInt()
    numFields = readInt()
    resultRow = new UnsafeRow(numFields)
    val nKeys = readLong()
    val nValues = readLong()
    // This is used in Broadcast, shared by multiple tasks, so we use on-heap memory
    // TODO(josh): This needs to be revisited before we merge this patch; making this change now
    // so that tests compile:
    val taskMemoryManager = new TaskMemoryManager(
      new UnifiedMemoryManager(
        new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"),
        Long.MaxValue,
        Long.MaxValue / 2,
        1),
      0)

    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().get(BUFFER_PAGESIZE).getOrElse(16L * 1024 * 1024))

    // TODO(josh): We won't need this dummy memory manager after future refactorings; revisit
    // during code review

    binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      (nKeys * 1.5 + 1).toInt, // reduce hash collision
      pageSizeBytes)

    var i = 0
    var keyBuffer = new Array[Byte](1024)
    var valuesBuffer = new Array[Byte](1024)
    while (i < nValues) {
      val keySize = readInt()
      val valuesSize = readInt()
      if (keySize > keyBuffer.length) {
        keyBuffer = new Array[Byte](keySize)
      }
      readBuffer(keyBuffer, 0, keySize)
      if (valuesSize > valuesBuffer.length) {
        valuesBuffer = new Array[Byte](valuesSize)
      }
      readBuffer(valuesBuffer, 0, valuesSize)

      val loc = binaryMap.lookup(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize)
      val putSucceeded = loc.append(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize,
        valuesBuffer, Platform.BYTE_ARRAY_OFFSET, valuesSize)
      if (!putSucceeded) {
        binaryMap.free()
        throw QueryExecutionErrors.cannotAllocateMemoryToGrowBytesToBytesMapError()
      }
      i += 1
    }
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readInt(), () => in.readLong(), in.readBytes)
  }
}

private[joins] object UnsafeHashedRelation {
  // 作为一个工厂，专门负责**构建（Build）**高效的、基于 UnsafeRow 和 BytesToBytesMap 的哈希关系结构
  def apply(
      input: Iterator[InternalRow], // 输入数据，待构建哈希关系的输入数据行迭代器，通常是 Join 中 Build Side 的结果。期望这些行已经是 UnsafeRow 格式
      key: Seq[Expression], // Join Key 表达式。定义了用于构建哈希键的表达式序列（例如，t1.colA, t1.colB）。这些表达式将用于生成 UnsafeProjection
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager, // 负责管理当前任务所使用的内存，包括用于 BytesToBytesMap 的堆外内存或堆上内存
      isNullAware: Boolean = false, // 空值敏感标志。默认为 false。如果为 true，表示这是一个空值敏感半连接（Null-Aware Anti Join, NAAJ）。如果输入数据中出现任何含 NULL 的键，则整个 HashedRelation 结构会被废弃，直接返回特殊的 HashedRelationWithAllNullKeys，触发特定的执行逻辑
      allowsNullKey: Boolean = false, // 允许空键标志。默认为 false。如果为 true，则允许包含 NULL 值的键参与哈希映射的构建。这用于空值安全等值连接（EqualNullSafe）
      ignoresDuplicatedKey: Boolean = false): HashedRelation = { // 忽略重复键标志。默认为 false。如果为 true，在向映射中插入键值对时，如果键已经存在，则忽略当前行（即不存储重复的值）
    require(!(isNullAware && allowsNullKey),
      "isNullAware and allowsNullKey cannot be enabled at same time")

    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().get(BUFFER_PAGESIZE).getOrElse(16L * 1024 * 1024))
    val binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      // Only 70% of the slots can be used before growing, more capacity help to reduce collision
      (sizeEstimate * 1.5 + 1).toInt,
      pageSizeBytes)

    // Create a mapping of buildKeys -> rows
    // 创建键投影器。
    val keyGenerator = UnsafeProjection.create(key)
    var numFields = 0
    while (input.hasNext) {
      val row = input.next().asInstanceOf[UnsafeRow]
      numFields = row.numFields()
      val key = keyGenerator(row)
      if (!key.anyNull || allowsNullKey) {
        val loc = binaryMap.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes)
        if (!(ignoresDuplicatedKey && loc.isDefined)) {
          val success = loc.append(
            key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
            row.getBaseObject, row.getBaseOffset, row.getSizeInBytes)
          if (!success) {
            binaryMap.free()
            throw QueryExecutionErrors.cannotAcquireMemoryToBuildUnsafeHashedRelationError()
          }
        }
      } else if (isNullAware) {
        binaryMap.free()
        return HashedRelationWithAllNullKeys
      }
    }

    new UnsafeHashedRelation(key.size, numFields, binaryMap)
  }
}

/**
 * An append-only hash map mapping from key of Long to UnsafeRow.
 *
 * The underlying bytes of all values (UnsafeRows) are packed together as a single byte array
 * (`page`) in this format:
 *
 *  [bytes of row1][address1][bytes of row2][address1] ...
 *
 *  address1 (8 bytes) is the offset and size of next value for the same key as row1, any key
 *  could have multiple values. the address at the end of last value for every key is 0.
 *
 * The keys and addresses of their values could be stored in two modes:
 *
 * 1) sparse mode: the keys and addresses are stored in `array` as:
 *
 *  [key1][address1][key2][address2]...[]
 *
 *  address1 (Long) is the offset (in `page`) and size of the value for key1. The position of key1
 *  is determined by `key1 % cap`. Quadratic probing with triangular numbers is used to address
 *  hash collision.
 *
 * 2) dense mode: all the addresses are packed into a single array of long, as:
 *
 *  [address1] [address2] ...
 *
 *  address1 (Long) is the offset (in `page`) and size of the value for key1, the position is
 *  determined by `key1 - minKey`.
 *
 * The map is created as sparse mode, then key-value could be appended into it. Once finish
 * appending, caller could call optimize() to try to turn the map into dense mode, which is faster
 * to probe.
 *
 * see http://java-performance.info/implementing-world-fastest-java-int-to-int-hash-map/
 */
private[execution] final class LongToUnsafeRowMap(
    val mm: TaskMemoryManager,
    capacity: Int,
    ignoresDuplicatedKey: Boolean = false)
  extends MemoryConsumer(mm, MemoryMode.ON_HEAP) with Externalizable with KryoSerializable {

  // Whether the keys are stored in dense mode or not.
  private var isDense = false

  // The minimum key
  private var minKey = Long.MaxValue

  // The maximum key
  private var maxKey = Long.MinValue

  // The array to store the key and offset of UnsafeRow in the page.
  //
  // Sparse mode: [key1] [offset1 | size1] [key2] [offset | size2] ...
  // Dense mode: [offset1 | size1] [offset2 | size2]
  private var array: Array[Long] = null
  private var mask: Int = 0

  // The page to store all bytes of UnsafeRow and the pointer to next rows.
  // [row1][pointer1] [row2][pointer2]
  private var page: Array[Long] = null

  // Current write cursor in the page.
  private var cursor: Long = Platform.LONG_ARRAY_OFFSET

  // The number of bits for size in address
  private val SIZE_BITS = 28
  private val SIZE_MASK = 0xfffffff

  // The total number of values of all keys.
  private var numValues = 0L

  // The number of unique keys.
  private var numKeys = 0L

  // needed by serializer
  def this() = {
    this(
      new TaskMemoryManager(
        new UnifiedMemoryManager(
          new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"),
          Long.MaxValue,
          Long.MaxValue / 2,
          1),
        0),
      0)
  }

  private def ensureAcquireMemory(size: Long): Unit = {
    // do not support spilling
    val got = acquireMemory(size)
    if (got < size) {
      freeMemory(got)
      throw QueryExecutionErrors.cannotAcquireMemoryToBuildLongHashedRelationError(size, got)
    }
  }

  private def init(): Unit = {
    if (mm != null) {
      require(capacity < 512000000, "Cannot broadcast 512 million or more rows")
      var n = 1
      while (n < capacity) n *= 2
      ensureAcquireMemory(n * 2L * 8 + (1 << 20))
      array = new Array[Long](n * 2)
      mask = n * 2 - 2
      page = new Array[Long](1 << 17)  // 1M bytes
    }
  }

  init()

  def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  /**
   * Returns whether all the keys are unique.
   */
  def keyIsUnique: Boolean = numKeys == numValues

  /**
   * Returns total memory consumption.
   */
  def getTotalMemoryConsumption: Long = array.length * 8L + page.length * 8L

  /**
   * Returns the first slot of array that store the keys (sparse mode).
   */
  private def firstSlot(key: Long): Int = {
    val h = key * 0x9E3779B9L
    (h ^ (h >> 32)).toInt & mask
  }

  /**
   * Returns the next probe in the array.
   */
  private def nextSlot(pos: Int): Int = (pos + 2) & mask

  private[this] def toAddress(offset: Long, size: Int): Long = {
    ((offset - Platform.LONG_ARRAY_OFFSET) << SIZE_BITS) | size
  }

  private[this] def toOffset(address: Long): Long = {
    (address >>> SIZE_BITS) + Platform.LONG_ARRAY_OFFSET
  }

  private[this] def toSize(address: Long): Int = {
    (address & SIZE_MASK).toInt
  }

  private def getRow(address: Long, resultRow: UnsafeRow): UnsafeRow = {
    resultRow.pointTo(page, toOffset(address), toSize(address))
    resultRow
  }

  /**
   * Returns the single UnsafeRow for given key, or null if not found.
   */
  def getValue(key: Long, resultRow: UnsafeRow): UnsafeRow = {
    if (isDense) {
      if (key >= minKey && key <= maxKey) {
        val value = array((key - minKey).toInt)
        if (value > 0) {
          return getRow(value, resultRow)
        }
      }
    } else {
      var pos = firstSlot(key)
      while (array(pos + 1) != 0) {
        if (array(pos) == key) {
          return getRow(array(pos + 1), resultRow)
        }
        pos = nextSlot(pos)
      }
    }
    null
  }

  /**
   * Returns an iterator of UnsafeRow for multiple linked values.
   */
  private def valueIter(address: Long, resultRow: UnsafeRow): Iterator[UnsafeRow] = {
    new Iterator[UnsafeRow] {
      var addr = address
      override def hasNext: Boolean = addr != 0
      override def next(): UnsafeRow = {
        val offset = toOffset(addr)
        val size = toSize(addr)
        resultRow.pointTo(page, offset, size)
        addr = Platform.getLong(page, offset + size)
        resultRow
      }
    }
  }

  /**
   * Returns an iterator for all the values for the given key, or null if no value found.
   */
  def get(key: Long, resultRow: UnsafeRow): Iterator[UnsafeRow] = {
    if (isDense) {
      if (key >= minKey && key <= maxKey) {
        val value = array((key - minKey).toInt)
        if (value > 0) {
          return valueIter(value, resultRow)
        }
      }
    } else {
      var pos = firstSlot(key)
      while (array(pos + 1) != 0) {
        if (array(pos) == key) {
          return valueIter(array(pos + 1), resultRow)
        }
        pos = nextSlot(pos)
      }
    }
    null
  }

  /**
   * Builds an iterator on a sparse array.
   */
  def keys(): Iterator[InternalRow] = {
    val row = new GenericInternalRow(1)
    // a) in dense mode the array stores the address
    //  => (k, v) = (minKey + index, array(index))
    // b) in sparse mode the array stores both the key and the address
    //  => (k, v) = (array(index), array(index+1))
    new Iterator[InternalRow] {
      // cursor that indicates the position of the next key which was not read by a next() call
      var pos = 0
      // when we iterate in dense mode we need to jump two positions at a time
      val step = if (isDense) 0 else 1

      override def hasNext: Boolean = {
        // go to the next key if the current key slot is empty
        while (pos + step < array.length) {
          if (array(pos + step) > 0) {
            return true
          }
          pos += step + 1
        }
        false
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw QueryExecutionErrors.endOfIteratorError()
        } else {
          // the key is retrieved based on the map mode
          val ret = if (isDense) minKey + pos else array(pos)
          // advance the cursor to the next index
          pos += step + 1
          row.setLong(0, ret)
          row
        }
      }
    }
  }

  /**
   * Appends the key and row into this map.
   */
  def append(key: Long, row: UnsafeRow): Unit = {
    val sizeInBytes = row.getSizeInBytes
    if (sizeInBytes >= (1 << SIZE_BITS)) {
      throw QueryExecutionErrors.rowLargerThan256MUnsupportedError()
    }

    val pos = findKeyPosition(key)
    if (ignoresDuplicatedKey && array(pos + 1) != 0) {
      return
    }

    if (key < minKey) {
      minKey = key
    }
    if (key > maxKey) {
      maxKey = key
    }

    grow(row.getSizeInBytes)

    // copy the bytes of UnsafeRow
    val offset = cursor
    Platform.copyMemory(row.getBaseObject, row.getBaseOffset, page, cursor, row.getSizeInBytes)
    cursor += row.getSizeInBytes
    Platform.putLong(page, cursor, 0)
    cursor += 8
    numValues += 1
    updateIndex(key, pos, toAddress(offset, row.getSizeInBytes))
  }

  private def findKeyPosition(key: Long): Int = {
    var pos = firstSlot(key)
    assert(numKeys < array.length / 2)
    while (array(pos) != key && array(pos + 1) != 0) {
      pos = nextSlot(pos)
    }
    pos
  }

  /**
   * Update the address in array for given key.
   */
  private def updateIndex(key: Long, pos: Int, address: Long): Unit = {
    if (array(pos + 1) == 0) {
      // this is the first value for this key, put the address in array.
      array(pos) = key
      array(pos + 1) = address
      numKeys += 1
      if (numKeys * 4 > array.length) {
        // reach half of the capacity
        if (array.length < (1 << 30)) {
          // Cannot allocate an array with 2G elements
          growArray()
        } else if (numKeys > array.length / 2 * 0.75) {
          // The fill ratio should be less than 0.75
          throw QueryExecutionErrors.cannotBuildHashedRelationWithUniqueKeysExceededError()
        }
      }
    } else {
      // there are some values for this key, put the address in the front of them.
      val pointer = toOffset(address) + toSize(address)
      Platform.putLong(page, pointer, array(pos + 1))
      array(pos + 1) = address
    }
  }

  private def grow(inputRowSize: Int): Unit = {
    // There is 8 bytes for the pointer to next value
    val neededNumWords = (cursor - Platform.LONG_ARRAY_OFFSET + 8 + inputRowSize + 7) / 8
    if (neededNumWords > page.length) {
      if (neededNumWords > (1 << 30)) {
        throw QueryExecutionErrors.cannotBuildHashedRelationLargerThan8GError()
      }
      val newNumWords = math.max(neededNumWords, math.min(page.length * 2, 1 << 30))
      ensureAcquireMemory(newNumWords * 8L)
      val newPage = new Array[Long](newNumWords.toInt)
      Platform.copyMemory(page, Platform.LONG_ARRAY_OFFSET, newPage, Platform.LONG_ARRAY_OFFSET,
        cursor - Platform.LONG_ARRAY_OFFSET)
      val used = page.length
      page = newPage
      freeMemory(used * 8L)
    }
  }

  private def growArray(): Unit = {
    var old_array = array
    val n = array.length
    numKeys = 0
    ensureAcquireMemory(n * 2 * 8L)
    array = new Array[Long](n * 2)
    mask = n * 2 - 2
    var i = 0
    while (i < old_array.length) {
      if (old_array(i + 1) > 0) {
        val key = old_array(i)
        updateIndex(key, findKeyPosition(key), old_array(i + 1))
      }
      i += 2
    }
    old_array = null  // release the reference to old array
    freeMemory(n * 8L)
  }

  /**
   * Try to turn the map into dense mode, which is faster to probe.
   */
  def optimize(): Unit = {
    val range = maxKey - minKey
    // Convert to dense mode if it does not require more memory or could fit within L1 cache
    // SPARK-16740: Make sure range doesn't overflow if minKey has a large negative value
    if (range >= 0 && (range < array.length || range < 1024)) {
      try {
        ensureAcquireMemory((range + 1) * 8L)
      } catch {
        case e: SparkException =>
          // there is no enough memory to convert
          return
      }
      val denseArray = new Array[Long]((range + 1).toInt)
      var i = 0
      while (i < array.length) {
        if (array(i + 1) > 0) {
          val idx = (array(i) - minKey).toInt
          denseArray(idx) = array(i + 1)
        }
        i += 2
      }
      val old_length = array.length
      array = denseArray
      isDense = true
      freeMemory(old_length * 8L)
    }
  }

  /**
   * Free all the memory acquired by this map.
   */
  def free(): Unit = {
    if (page != null) {
      freeMemory(page.length * 8L)
      page = null
    }
    if (array != null) {
      freeMemory(array.length * 8L)
      array = null
    }
  }

  private def writeLongArray(
      writeBuffer: (Array[Byte], Int, Int) => Unit,
      arr: Array[Long],
      len: Int): Unit = {
    val buffer = new Array[Byte](4 << 10)
    var offset: Long = Platform.LONG_ARRAY_OFFSET
    val end = len * 8L + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      Platform.copyMemory(arr, offset, buffer, Platform.BYTE_ARRAY_OFFSET, size)
      writeBuffer(buffer, 0, size.toInt)
      offset += size
    }
  }

  private def write(
      writeBoolean: (Boolean) => Unit,
      writeLong: (Long) => Unit,
      writeBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    writeBoolean(isDense)
    writeLong(minKey)
    writeLong(maxKey)
    writeLong(numKeys)
    writeLong(numValues)

    writeLong(array.length)
    writeLongArray(writeBuffer, array, array.length)
    val used = ((cursor - Platform.LONG_ARRAY_OFFSET) / 8).toInt
    writeLong(used)
    writeLongArray(writeBuffer, page, used)
  }

  override def writeExternal(output: ObjectOutput): Unit = {
    write(output.writeBoolean, output.writeLong, output.write)
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    write(out.writeBoolean, out.writeLong, out.write)
  }

  private def readLongArray(
      readBuffer: (Array[Byte], Int, Int) => Unit,
      length: Int): Array[Long] = {
    val array = new Array[Long](length)
    val buffer = new Array[Byte](4 << 10)
    var offset: Long = Platform.LONG_ARRAY_OFFSET
    val end = length * 8L + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      readBuffer(buffer, 0, size.toInt)
      Platform.copyMemory(buffer, Platform.BYTE_ARRAY_OFFSET, array, offset, size)
      offset += size
    }
    array
  }

  private def read(
      readBoolean: () => Boolean,
      readLong: () => Long,
      readBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    isDense = readBoolean()
    minKey = readLong()
    maxKey = readLong()
    numKeys = readLong()
    numValues = readLong()

    val length = readLong().toInt
    mask = length - 2
    array = readLongArray(readBuffer, length)
    val pageLength = readLong().toInt
    page = readLongArray(readBuffer, pageLength)
    // Restore cursor variable to make this map able to be serialized again on executors.
    cursor = pageLength * 8 + Platform.LONG_ARRAY_OFFSET
  }

  override def readExternal(in: ObjectInput): Unit = {
    read(() => in.readBoolean(), () => in.readLong(), in.readFully)
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    read(() => in.readBoolean(), () => in.readLong(), in.readBytes)
  }
}

class LongHashedRelation(
    private var nFields: Int,
    private var map: LongToUnsafeRowMap) extends HashedRelation with Externalizable {

  private var resultRow: UnsafeRow = new UnsafeRow(nFields)

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(0, null)

  override def asReadOnlyCopy(): LongHashedRelation = new LongHashedRelation(nFields, map)

  override def estimatedSize: Long = map.getTotalMemoryConsumption

  override def get(key: InternalRow): Iterator[InternalRow] = {
    if (key.isNullAt(0)) {
      null
    } else {
      get(key.getLong(0))
    }
  }

  override def getValue(key: InternalRow): InternalRow = {
    if (key.isNullAt(0)) {
      null
    } else {
      getValue(key.getLong(0))
    }
  }

  override def get(key: Long): Iterator[InternalRow] = map.get(key, resultRow)

  override def getValue(key: Long): InternalRow = map.getValue(key, resultRow)

  override def keyIsUnique: Boolean = map.keyIsUnique

  override def close(): Unit = {
    map.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(nFields)
    out.writeObject(map)
  }

  override def readExternal(in: ObjectInput): Unit = {
    nFields = in.readInt()
    resultRow = new UnsafeRow(nFields)
    map = in.readObject().asInstanceOf[LongToUnsafeRowMap]
  }

  /**
   * Returns an iterator for keys of InternalRow type.
   */
  override def keys(): Iterator[InternalRow] = map.keys()

  override def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  override def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    throw new UnsupportedOperationException
  }

  override def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  override def maxNumKeysIndex: Int = {
    throw new UnsupportedOperationException
  }
}

/**
 * Create hashed relation with key that is long.
 */
private[joins] object LongHashedRelation {
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager,
      isNullAware: Boolean = false,
      ignoresDuplicatedKey: Boolean = false): HashedRelation = {

    val map = new LongToUnsafeRowMap(taskMemoryManager, sizeEstimate, ignoresDuplicatedKey)
    val keyGenerator = UnsafeProjection.create(key)

    // Create a mapping of key -> rows
    var numFields = 0
    while (input.hasNext) {
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]
      numFields = unsafeRow.numFields()
      val rowKey = keyGenerator(unsafeRow)
      if (!rowKey.isNullAt(0)) {
        val key = rowKey.getLong(0)
        map.append(key, unsafeRow)
      } else if (isNullAware) {
        map.free()
        return HashedRelationWithAllNullKeys
      }
    }
    map.optimize()
    new LongHashedRelation(numFields, map)
  }
}

/**
 * A special HashedRelation indicating that it's built from a empty input:Iterator[InternalRow].
 * get & getValue will return null just like
 * empty LongHashedRelation or empty UnsafeHashedRelation does.
 */
case object EmptyHashedRelation extends HashedRelation {
  override def get(key: Long): Iterator[InternalRow] = null

  override def get(key: InternalRow): Iterator[InternalRow] = null

  override def getValue(key: Long): InternalRow = null

  override def getValue(key: InternalRow): InternalRow = null

  override def asReadOnlyCopy(): EmptyHashedRelation.type = this

  override def keyIsUnique: Boolean = true

  override def keys(): Iterator[InternalRow] = {
    Iterator.empty
  }

  override def close(): Unit = {}

  override def estimatedSize: Long = 0
}

/**
 * A special HashedRelation indicating that it's built from a non-empty input:Iterator[InternalRow]
 * with all the keys to be null.
 */
case object HashedRelationWithAllNullKeys extends HashedRelation {
  override def get(key: InternalRow): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def getValue(key: InternalRow): InternalRow = {
    throw new UnsupportedOperationException
  }

  override def asReadOnlyCopy(): HashedRelationWithAllNullKeys.type = this

  override def keyIsUnique: Boolean = true

  override def keys(): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def close(): Unit = {}

  override def estimatedSize: Long = 0
}
//广播数据作为哈希关系（HashedRelation）的具体实现。这个模式通常用于需要通过哈希表查找来加速连接操作的场景
//key: Seq[Expression]：这是一个 Seq[Expression] 类型的参数，表示哈希关系的键，即用于构建哈希表的列
//isNullAware: Boolean：这是一个可选的布尔值，默认为 false。如果设置为 true，表示该哈希关系在处理 NULL 值时会有特殊的行为，
// 例如在 NULL 值匹配时执行特殊的连接操作（例如 null-aware 连接）
/** The HashedRelationBroadcastMode requires that rows are broadcasted as a HashedRelation. */
case class HashedRelationBroadcastMode(key: Seq[Expression], isNullAware: Boolean = false)
  extends BroadcastMode {

  override def transform(rows: Array[InternalRow]): HashedRelation = {
    transform(rows.iterator, Some(rows.length))
  }

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): HashedRelation = {
    sizeHint match {
      case Some(numRows) =>
        HashedRelation(rows, key, numRows.toInt, isNullAware = isNullAware)
      case None =>
        HashedRelation(rows, key, isNullAware = isNullAware)
    }
  }

  override lazy val canonicalized: HashedRelationBroadcastMode = {
    this.copy(key = key.map(_.canonicalized))
  }
}
