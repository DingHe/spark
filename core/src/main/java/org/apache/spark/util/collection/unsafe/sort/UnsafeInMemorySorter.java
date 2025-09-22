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

package org.apache.spark.util.collection.unsafe.sort;

import java.util.Comparator;
import java.util.LinkedList;

import javax.annotation.Nullable;

import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.Sorter;

/** 用于高效排序的类，特别适用于大数据量的排序任务。它通过在内存中对记录进行排序，使用了“键前缀排序”算法，并在内部存储了指向记录的指针及其排序键的前缀，从而提高了排序效率
 * Sorts records using an AlphaSort-style key-prefix sort. This sort stores pointers to records
 * alongside a user-defined prefix of the record's sorting key. When the underlying sort algorithm
 * compares records, it will first compare the stored key prefixes; if the prefixes are not equal,
 * then we do not need to traverse the record pointers to compare the actual records. Avoiding these
 * random memory accesses improves cache hit rates.
 */
public final class UnsafeInMemorySorter {

  private static final class SortComparator implements Comparator<RecordPointerAndKeyPrefix> {

    private final RecordComparator recordComparator; //用于比较完整记录的比较器
    private final PrefixComparator prefixComparator; //用于比较记录的键前缀的比较器
    private final TaskMemoryManager memoryManager;

    SortComparator(
        RecordComparator recordComparator,
        PrefixComparator prefixComparator,
        TaskMemoryManager memoryManager) {
      this.recordComparator = recordComparator;
      this.prefixComparator = prefixComparator;
      this.memoryManager = memoryManager;
    }

    @Override
    public int compare(RecordPointerAndKeyPrefix r1, RecordPointerAndKeyPrefix r2) {
      final int prefixComparisonResult = prefixComparator.compare(r1.keyPrefix, r2.keyPrefix); // 先比较键前缀
      int uaoSize = UnsafeAlignedOffset.getUaoSize();
      if (prefixComparisonResult == 0) { // 如果键前缀相等，则比较完整记录
        final Object baseObject1 = memoryManager.getPage(r1.recordPointer); // 获取第一个记录的内存信息
        final long baseOffset1 = memoryManager.getOffsetInPage(r1.recordPointer) + uaoSize;
        final int baseLength1 = UnsafeAlignedOffset.getSize(baseObject1, baseOffset1 - uaoSize);
        final Object baseObject2 = memoryManager.getPage(r2.recordPointer); // 获取第二个记录的内存信息
        final long baseOffset2 = memoryManager.getOffsetInPage(r2.recordPointer) + uaoSize;
        final int baseLength2 = UnsafeAlignedOffset.getSize(baseObject2, baseOffset2 - uaoSize);
        return recordComparator.compare(baseObject1, baseOffset1, baseLength1, baseObject2,
          baseOffset2, baseLength2);
      } else {
        return prefixComparisonResult;
      }
    }
  }

  private final MemoryConsumer consumer; //用于为排序过程分配和释放内存
  private final TaskMemoryManager memoryManager;
  @Nullable
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator; //用于在排序过程中比较记录的键前缀和完整记录

  /**
   * If non-null, specifies the radix sort parameters and that radix sort will be used.
   */
  @Nullable
  private final PrefixComparators.RadixSortSupport radixSortSupport; //如果排序使用基数排序（Radix Sort），则此属性存储基数排序的支持信息

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   *
   * Only part of the array will be used to store the pointers, the rest part is preserved as
   * temporary buffer for sorting.
   */
  private LongArray array; //存储记录指针和键前缀的数组。每个记录占用两个数组元素：一个存储记录的指针，一个存储键前缀

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int pos = 0; //表示当前可以插入新记录的位置

  /**
   * If sorting with radix sort, specifies the starting position in the sort buffer where records
   * with non-null prefixes are kept. Positions [0..nullBoundaryPos) will contain null-prefixed
   * records, and positions [nullBoundaryPos..pos) non-null prefixed records. This lets us avoid
   * radix sorting over null values.
   */
  private int nullBoundaryPos = 0; //当使用基数排序时，记录中存储 null 键前缀的边界位置。所有 null 键前缀的记录都排在前面

  /*
   * How many records could be inserted, because part of the array should be left for sorting.
   */
  private int usableCapacity = 0; //数组中可以插入记录的最大容量

  private long initialSize; //数组初始分配的大小

  private long totalSortTimeNanos = 0L; //记录排序过程中消耗的时间（单位：纳秒）

  public UnsafeInMemorySorter(
    final MemoryConsumer consumer,
    final TaskMemoryManager memoryManager,
    final RecordComparator recordComparator,
    final PrefixComparator prefixComparator,
    int initialSize,
    boolean canUseRadixSort) {
    this(consumer, memoryManager, recordComparator, prefixComparator,
      consumer.allocateArray(initialSize * 2L), canUseRadixSort);
  }

  public UnsafeInMemorySorter(
      final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      LongArray array,
      boolean canUseRadixSort) {
    this.consumer = consumer;
    this.memoryManager = memoryManager;
    this.initialSize = array.size();
    if (recordComparator != null) {
      this.sortComparator = new SortComparator(recordComparator, prefixComparator, memoryManager);
      if (canUseRadixSort && prefixComparator instanceof PrefixComparators.RadixSortSupport) {
        this.radixSortSupport = (PrefixComparators.RadixSortSupport)prefixComparator;
      } else {
        this.radixSortSupport = null;
      }
    } else {
      this.sortComparator = null;
      this.radixSortSupport = null;
    }
    this.array = array;
    this.usableCapacity = getUsableCapacity();
  }
  //返回可以用于插入记录的最大容量。基数排序和 Tim Sort 对内存的使用方式不同，基数排序使用的内存是缓冲区的两倍，Tim Sort 则需要将内存的 2/3 用于排序
  private int getUsableCapacity() {
    // Radix sort requires same amount of used memory as buffer, Tim sort requires
    // half of the used memory as buffer.
    return (int) (array.size() / (radixSortSupport != null ? 2 : 1.5));
  }

  public long getInitialSize() {
    return initialSize;
  }

  /**
   * Free the memory used by pointer array.
   */
  public void freeMemory() {
    if (consumer != null) {
      if (array != null) {
        consumer.freeArray(array);
      }

      // Set the array to null instead of allocating a new array. Allocating an array could have
      // triggered another spill and this method already is called from UnsafeExternalSorter when
      // spilling. Attempting to allocate while spilling is dangerous, as we could be holding onto
      // a large partially complete allocation, which may prevent other memory from being allocated.
      // Instead we will allocate the new array when it is necessary.
      array = null;
      usableCapacity = 0;
    }
    pos = 0;
    nullBoundaryPos = 0;
  }

  /** 返回当前已插入的记录数量。由于每条记录占用数组的两个元素，返回的是 pos / 2
   * @return the number of records that have been inserted into this sorter.
   */
  public int numRecords() {
    return pos / 2;
  }

  /**
   * @return the total amount of time spent sorting data (in-memory only).
   */
  public long getSortTimeNanos() {
    return totalSortTimeNanos;
  }
 //返回 array 使用的内存大小，单位是字节
  public long getMemoryUsage() {
    if (array == null) {
      return 0L;
    }

    return array.size() * 8;
  }
  //判断是否还有足够的空间来插入新记录。判断依据是 pos + 1 是否小于 usableCapacity
  public boolean hasSpaceForAnotherRecord() {
    return pos + 1 < usableCapacity;
  }
  //扩展指针数组。若当前 array 数组容量不够，创建一个新的更大的数组并将数据复制过去。
  public void expandPointerArray(LongArray newArray) {
    if (array != null) {
      if (newArray.size() < array.size()) {
        // checkstyle.off: RegexpSinglelineJava
        throw new SparkOutOfMemoryError("Not enough memory to grow pointer array");
        // checkstyle.on: RegexpSinglelineJava
      }
      Platform.copyMemory(
        array.getBaseObject(),
        array.getBaseOffset(),
        newArray.getBaseObject(),
        newArray.getBaseOffset(),
        pos * 8L);
      consumer.freeArray(array);
    }
    array = newArray;
    usableCapacity = getUsableCapacity();
  }

  /**
   * Inserts a record to be sorted. Assumes that the record pointer points to a record length
   * stored as a uaoSize(4 or 8) bytes integer, followed by the record's bytes.
   * 插入一条记录。记录的指针和键前缀分别存储在 array 数组的两个元素中。如果使用基数排序且键前缀为 null，会将 null 键前缀的记录插入到数组的前面
   * @param recordPointer pointer to a record in a data page, encoded by {@link TaskMemoryManager}.
   * @param keyPrefix a user-defined key prefix
   */
  public void insertRecord(long recordPointer, long keyPrefix, boolean prefixIsNull) {
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
    }
    if (prefixIsNull && radixSortSupport != null) {
      // Swap forward a non-null record to make room for this one at the beginning of the array.
      array.set(pos, array.get(nullBoundaryPos));
      pos++;
      array.set(pos, array.get(nullBoundaryPos + 1));
      pos++;
      // Place this record in the vacated position.
      array.set(nullBoundaryPos, recordPointer);
      nullBoundaryPos++;
      array.set(nullBoundaryPos, keyPrefix);
      nullBoundaryPos++;
    } else {
      array.set(pos, recordPointer);
      pos++;
      array.set(pos, keyPrefix);
      pos++;
    }
  }
  //用于遍历已排序的数据。它实现了 UnsafeSorterIterator 接口，可以高效地按排序顺序遍历记录。
  public final class SortedIterator extends UnsafeSorterIterator implements Cloneable {

    private final int numRecords;
    private int position;
    private int offset;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;
    private long currentPageNumber;
    private final TaskContext taskContext = TaskContext.get();

    private SortedIterator(int numRecords, int offset) {
      this.numRecords = numRecords;
      this.position = 0;
      this.offset = offset;
    }

    @Override
    public SortedIterator clone() {
      SortedIterator iter = new SortedIterator(numRecords, offset);
      iter.position = position;
      iter.baseObject = baseObject;
      iter.baseOffset = baseOffset;
      iter.keyPrefix = keyPrefix;
      iter.recordLength = recordLength;
      iter.currentPageNumber = currentPageNumber;
      return iter;
    }

    @Override
    public int getNumRecords() {
      return numRecords;
    }

    @Override
    public boolean hasNext() {
      return position / 2 < numRecords;
    }

    @Override
    public void loadNext() {
      // Kill the task in case it has been marked as killed. This logic is from
      // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
      // to avoid performance overhead. This check is added here in `loadNext()` instead of in
      // `hasNext()` because it's technically possible for the caller to be relying on
      // `getNumRecords()` instead of `hasNext()` to know when to stop.
      if (taskContext != null) {
        taskContext.killTaskIfInterrupted();
      }
      // This pointer points to a 4-byte record length, followed by the record's bytes
      final long recordPointer = array.get(offset + position);
      currentPageNumber = TaskMemoryManager.decodePageNumber(recordPointer);
      int uaoSize = UnsafeAlignedOffset.getUaoSize();
      baseObject = memoryManager.getPage(recordPointer);
      // Skip over record length
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + uaoSize;
      recordLength = UnsafeAlignedOffset.getSize(baseObject, baseOffset - uaoSize);
      keyPrefix = array.get(offset + position + 1);
      position += 2;
    }

    @Override
    public Object getBaseObject() { return baseObject; }

    @Override
    public long getBaseOffset() { return baseOffset; }

    @Override
    public long getCurrentPageNumber() {
      return currentPageNumber;
    }

    @Override
    public int getRecordLength() { return recordLength; }

    @Override
    public long getKeyPrefix() { return keyPrefix; }
  }

  /**
   * Return an iterator over record pointers in sorted order. For efficiency, all calls to
   * {@code next()} will return the same mutable object.
   */
  public UnsafeSorterIterator getSortedIterator() {
    if (numRecords() == 0) {
      // `array` might be null, so make sure that it is not accessed by returning early.
      return new SortedIterator(0, 0);
    }

    int offset = 0;
    long start = System.nanoTime();
    if (sortComparator != null) {
      if (this.radixSortSupport != null) {
        offset = RadixSort.sortKeyPrefixArray(
          array, nullBoundaryPos, (pos - nullBoundaryPos) / 2L, 0, 7,
          radixSortSupport.sortDescending(), radixSortSupport.sortSigned());
      } else {
        MemoryBlock unused = new MemoryBlock(
          array.getBaseObject(),
          array.getBaseOffset() + pos * 8L,
          (array.size() - pos) * 8L);
        LongArray buffer = new LongArray(unused);
        Sorter<RecordPointerAndKeyPrefix, LongArray> sorter =
          new Sorter<>(new UnsafeSortDataFormat(buffer));
        sorter.sort(array, 0, pos / 2, sortComparator);
      }
    }
    totalSortTimeNanos += System.nanoTime() - start;
    if (nullBoundaryPos > 0) {
      assert radixSortSupport != null : "Nulls are only stored separately with radix sort";
      LinkedList<UnsafeSorterIterator> queue = new LinkedList<>();

      // The null order is either LAST or FIRST, regardless of sorting direction (ASC|DESC)
      if (radixSortSupport.nullsFirst()) {
        queue.add(new SortedIterator(nullBoundaryPos / 2, 0));
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 2, offset));
      } else {
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 2, offset));
        queue.add(new SortedIterator(nullBoundaryPos / 2, 0));
      }
      return new UnsafeExternalSorter.ChainedIterator(queue);
    } else {
      return new SortedIterator(pos / 2, offset);
    }
  }
}
