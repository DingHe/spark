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

package org.apache.spark.shuffle.sort;

import java.util.Comparator;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.util.collection.unsafe.sort.RadixSort;

//Spark 基于排序的 Shuffle (Sort-based Shuffle) 中的核心组件，负责对任务在内存中积累的记录指针进行高效排序
//记录指针收集：它存储一系列经过特殊编码的 long 整数（即 PackedRecordPointer），每个 long 包含了记录的分区 ID 和内存地址
//内存管理：它负责向 TaskMemoryManager 申请和释放用于存储这些指针的长整型数组 (LongArray) 内存
//快速排序：当内存空间用尽或需要溢写时，它会使用基数排序（RadixSort） 或归并排序（TimSort） 等高效算法对这些指针进行排序，实现按分区 ID 的快速分组
//通过仅对轻量级的 8 字节指针进行排序，而不是移动实际的记录数据（记录数据保留在 MemoryBlock 中），它极大地提高了 Shuffle 排序的性能
final class ShuffleInMemorySorter {
  //它只根据 PackedRecordPointer 中的分区 ID 进行比较，这是排序的核心逻辑
  private static final class SortComparator implements Comparator<PackedRecordPointer> {
    @Override
    public int compare(PackedRecordPointer left, PackedRecordPointer right) {
      return Integer.compare(left.getPartitionId(), right.getPartitionId());
    }
  }
  private static final SortComparator SORT_COMPARATOR = new SortComparator();
  //指向其父级 ShuffleExternalSorter，用于调用其方法来分配和释放 LongArray
  private final MemoryConsumer consumer;

  /**
   * An array of record pointers and partition ids that have been encoded by
   * {@link PackedRecordPointer}. The sort operates on this array instead of directly manipulating
   * records.
   *
   * Only part of the array will be used to store the pointers, the rest part is preserved as
   * temporary buffer for sorting.
   */
  //存储所有已打包的记录指针 (PackedRecordPointer) 的 LongArray。排序操作主要针对这个数组。该数组的一部分空间会预留给排序算法作为临时缓冲区
  private LongArray array;

  /**
   * Whether to use radix sort for sorting in-memory partition ids. Radix sort is much faster
   * but requires additional memory to be reserved memory as pointers are added.
   */
  //配置项 spark.shuffle.sort.useRadixSort 的值。决定使用基数排序（更快，但需要更多缓冲区）还是归并排序
  private final boolean useRadixSort;

  /**
   * The position in the pointer array where new records can be inserted.
   */
  //当前数组中下一个记录指针应该插入的位置，也是数组中已存储的记录总数。
  private int pos = 0;

  /**
   * How many records could be inserted, because part of the array should be left for sorting.
   */
  //数组中可用于存储记录指针的实际容量。由于排序需要缓冲区，这个值小于 array.size()
  private int usableCapacity = 0;
  //第一次分配 array 时请求的元素数量。用于 reset() 方法中重新分配相同大小的数组
  private final int initialSize;

  ShuffleInMemorySorter(MemoryConsumer consumer, int initialSize, boolean useRadixSort) {
    this.consumer = consumer;
    assert (initialSize > 0);
    this.initialSize = initialSize;
    this.useRadixSort = useRadixSort;
    //申请初始内存
    this.array = consumer.allocateArray(initialSize);
    this.usableCapacity = getUsableCapacity();
  }
  //根据是否使用基数排序来计算 LongArray 中可用于存储记录指针的容量
  private int getUsableCapacity() {
    // Radix sort requires same amount of used memory as buffer, Tim sort requires
    // half of the used memory as buffer.
    return (int) (array.size() / (useRadixSort ? 2 : 1.5));
  }
  //释放持有的 LongArray 内存
  public void free() {
    if (array != null) {
      consumer.freeArray(array);
      array = null;
    }
  }

  public int numRecords() {
    return pos;
  }

  public void reset() {
    // Reset `pos` here so that `spill` triggered by the below `allocateArray` will be no-op.
    pos = 0;
    if (consumer != null) {
      consumer.freeArray(array);
      // As `array` has been released, we should set it to  `null` to avoid accessing it before
      // `allocateArray` returns. `usableCapacity` is also set to `0` to avoid any codes writing
      // data to `ShuffleInMemorySorter` when `array` is `null` (e.g., in
      // ShuffleExternalSorter.growPointerArrayIfNecessary, we may try to access
      // `ShuffleInMemorySorter` when `allocateArray` throws SparkOutOfMemoryError).
      array = null;
      usableCapacity = 0;
      array = consumer.allocateArray(initialSize);
      usableCapacity = getUsableCapacity();
    }
  }
  //当 array 空间不足时，将记录从旧数组安全地拷贝到更大的 newArray 中，然后释放旧数组并更新为新数组
  public void expandPointerArray(LongArray newArray) {
    assert(newArray.size() > array.size());
    Platform.copyMemory(
      array.getBaseObject(),
      array.getBaseOffset(),
      newArray.getBaseObject(),
      newArray.getBaseOffset(),
      pos * 8L
    );
    consumer.freeArray(array);
    array = newArray;
    usableCapacity = getUsableCapacity();
  }
  //是否还有空间写入记录指针
  public boolean hasSpaceForAnotherRecord() {
    return pos < usableCapacity;
  }

  public long getMemoryUsage() {
    return array.size() * 8;
  }

  /**
   * Inserts a record to be sorted.
   *
   * @param recordPointer a pointer to the record, encoded by the task memory manager. Due to
   *                      certain pointer compression techniques used by the sorter, the sort can
   *                      only operate on pointers that point to locations in the first
   *                      {@link PackedRecordPointer#MAXIMUM_PAGE_SIZE_BYTES} bytes of a data page.
   * @param partitionId the partition id, which must be less than or equal to
   *                    {@link PackedRecordPointer#MAXIMUM_PARTITION_ID}.
   */
  //核心插入方法。它首先检查是否有空间，然后调用 PackedRecordPointer.packPointer 将地址和分区 ID 编码，最后将编码后的 long 存入 array[pos] 并递增 pos
  public void insertRecord(long recordPointer, int partitionId) {
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
    }
    array.set(pos, PackedRecordPointer.packPointer(recordPointer, partitionId));
    pos++;
  }

  /**
   * An iterator-like class that's used instead of Java's Iterator in order to facilitate inlining.
   */
  //一个自定义的、类似 Iterator 的类，用于遍历排序后的记录指针。它通过内部维护的 position 索引，按顺序读取 pointerArray 中的值，并用 PackedRecordPointer 解码
  public static final class ShuffleSorterIterator {

    private final LongArray pointerArray;
    private final int limit;
    final PackedRecordPointer packedRecordPointer = new PackedRecordPointer();
    private int position = 0;

    ShuffleSorterIterator(int numRecords, LongArray pointerArray, int startingPosition) {
      this.limit = numRecords + startingPosition;
      this.pointerArray = pointerArray;
      this.position = startingPosition;
    }

    public boolean hasNext() {
      return position < limit;
    }

    public void loadNext() {
      packedRecordPointer.set(pointerArray.get(position));
      position++;
    }
  }

  /**
   * Return an iterator over record pointers in sorted order.
   */
  //触发实际内存排序并返回结果迭代器的核心方法。它根据配置（是否使用基数排序）执行不同的排序逻辑
  public ShuffleSorterIterator getSortedIterator() {
    //用于记录排序后的结果在指针数组 array 中的起始位置。默认初始化为 0
    int offset = 0;
    if (useRadixSort) {
      //调用 Spark 内部实现的基数排序算法。基数排序对整数类型的数据非常快
      //只是按照分区排序
      offset = RadixSort.sort(
        array, pos,
        PackedRecordPointer.PARTITION_ID_START_BYTE_INDEX,
        PackedRecordPointer.PARTITION_ID_END_BYTE_INDEX, false, false);
    } else {
      //创建一个 MemoryBlock，指向 array 中尚未使用的部分。这部分内存将用作排序算法的临时缓冲区
      //只是按照分区排序
      MemoryBlock unused = new MemoryBlock(
        array.getBaseObject(),
        array.getBaseOffset() + pos * 8L,
        (array.size() - pos) * 8L);
      LongArray buffer = new LongArray(unused);
      Sorter<PackedRecordPointer, LongArray> sorter =
        new Sorter<>(new ShuffleSortDataFormat(buffer));

      sorter.sort(array, 0, pos, SORT_COMPARATOR);
    }
    return new ShuffleSorterIterator(pos, array, offset);
  }
}
