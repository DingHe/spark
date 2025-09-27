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

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.zip.Checksum;

import org.apache.spark.SparkException;
import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.checksum.ShuffleChecksumSupport;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link ShuffleInMemorySorter}). The sorted records are then
 * written to a single output file (or multiple files, if we've spilled). The format of the output
 * files is the same as the format of the final output file written by
 * {@link org.apache.spark.shuffle.sort.SortShuffleWriter}: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link UnsafeShuffleWriter}, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 */
//Spark 专为基于排序的 Shuffle 实现的一个外部排序器。它的核心作用是在任务执行过程中，将输入的记录按目标分区 ID 进行快速、内存内排序。当内存中的数据量达到阈值或内存不足时，它会将这些已排序的数据溢写（spill） 到磁盘文件中
  //是 Spark Tungsten 内存管理和 Unsafe 机制在 Shuffle 写入阶段的应用，它通过直接操作内存地址和字节，避免了 Java 对象的创建和序列化/反序列化的开销，从而大幅提升 Shuffle 性能
final class ShuffleExternalSorter extends MemoryConsumer implements ShuffleChecksumSupport {

  private static final Logger logger = LoggerFactory.getLogger(ShuffleExternalSorter.class);

  @VisibleForTesting
  static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;
  //目标分区的总数量
  private final int numPartitions;
  //任务内存管理器
  private final TaskMemoryManager taskMemoryManager;
  //用于创建临时 Shuffle 块 ID，并在磁盘上创建和管理溢写文件
  private final BlockManager blockManager;
  //用于获取任务尝试 ID，并更新任务指标（如溢写字节数）
  private final TaskContext taskContext;
  //
  private final ShuffleWriteMetricsReporter writeMetrics;

  /**
   * Force this sorter to spill when there are this many elements in memory.
   */
  //当内存中的记录数达到此阈值时，即使内存未满也会触发溢写，防止单个数组过大
  private final int numElementsForSpillThreshold;

  /** The buffer size to use when writing spills using DiskBlockObjectWriter */
  /
  private final int fileBufferSizeBytes;

  /** The buffer size to use when writing the sorted records to an on-disk file */
  //写入磁盘溢写文件时使用的内部字节数组缓冲区大小，优化 IO 性能
  private final int diskWriteBufferSize;

  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  //存储了所有当前被用于保存记录数据的内存页。溢写时这些页会被释放
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();
  //溢写文件信息链表
  private final LinkedList<SpillInfo> spills = new LinkedList<>();

  /** Peak memory used by this sorter so far, in bytes. **/
  private long peakMemoryUsedBytes;

  // These variables are reset after spilling:
  //负责维护记录指针（Packed Record Pointer）并执行快速的内存内排序
  @Nullable private ShuffleInMemorySorter inMemSorter;
  //当前正在写入的内存页
  @Nullable private MemoryBlock currentPage = null;
  //当前内存页内的写入偏移量
  private long pageCursor = -1;

  // Checksum calculator for each partition. Empty when shuffle checksum disabled.
  private final Checksum[] partitionChecksums;

  ShuffleExternalSorter(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics) throws SparkException {
    super(memoryManager,
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = memoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.numPartitions = numPartitions;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSizeBytes =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.numElementsForSpillThreshold =
        (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
    this.writeMetrics = writeMetrics;
    this.inMemSorter = new ShuffleInMemorySorter(
      this, initialSize, (boolean) conf.get(package$.MODULE$.SHUFFLE_SORT_USE_RADIXSORT()));
    this.peakMemoryUsedBytes = getMemoryUsage();
    this.diskWriteBufferSize =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());
    this.partitionChecksums = createPartitionChecksums(numPartitions, conf);
  }

  public long[] getChecksums() {
    return getChecksumValues(partitionChecksums);
  }

  /**
   * Sorts the in-memory records and writes the sorted records to an on-disk file.
   * This method does not free the sort data structures.
   *
   * @param isFinalFile if true, this indicates that we're writing the final output file and that
   *                    the bytes written should be counted towards shuffle write metrics rather
   *                    than shuffle spill metrics.
   */
  private void writeSortedFile(boolean isFinalFile) {
    // Only emit the log if this is an actual spilling.
    //仅当 isFinalFile 为 false（即这是一次内存溢写）时，记录一条 INFO 级别的日志，报告当前任务、线程、溢出数据量和溢写次数
    if (!isFinalFile) {
      logger.info(
        "Task {} on Thread {} spilling sort data of {} to disk ({} {} so far)",
        taskContext.taskAttemptId(),
        Thread.currentThread().getId(),
        Utils.bytesToString(getMemoryUsage()),
        spills.size(),
        spills.size() != 1 ? " times" : " time");
    }

    // This call performs the actual sort.
    //调用 inMemSorter（内存内排序器）的 getSortedIterator() 方法。实际的内存内排序操作在此处完成，返回一个按分区 ID 有序的记录迭代器
    final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
      inMemSorter.getSortedIterator();

    // If there are no sorted records, so we don't need to create an empty spill file.
    if (!sortedRecords.hasNext()) {
      return;
    }

    final ShuffleWriteMetricsReporter writeMetricsToUse;

    if (isFinalFile) {
      // We're writing the final non-spill file, so we _do_ want to count this as shuffle bytes.
      writeMetricsToUse = writeMetrics;
    } else {
      // We're spilling, so bytes written should be counted towards spill rather than write.
      // Create a dummy WriteMetrics object to absorb these metrics, since we don't want to count
      // them towards shuffle bytes written.
      // The actual shuffle bytes written will be counted when we merge the spill files.
      writeMetricsToUse = new ShuffleWriteMetrics();
    }

    // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
    // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
    // data through a byte array. This array does not need to be large enough to hold a single
    // record;
    //创建一个字节数组缓冲区。Shuffle 数据是 Unsafe 内存中的字节，需要通过这个缓冲区进行块拷贝，以优化向磁盘写入的效率
    final byte[] writeBuffer = new byte[diskWriteBufferSize];

    // Because this output will be read during shuffle, its compression codec must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more details.
    //创建临时文件与元数据
    final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempShuffleBlock();
    final File file = spilledFileInfo._2();
    final TempShuffleBlockId blockId = spilledFileInfo._1();
    //创建 SpillInfo 对象，用于记录本次溢写文件的元数据，特别是用于存储每个分区写入的字节长度
    final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    //创建一个虚拟的无操作 (no-op) 序列化器。因为 Unsafe Shuffle 直接操作字节，不使用 Java 序列化
    final SerializerInstance ser = DummySerializerInstance.INSTANCE;

    int currentPartition = -1;
    final FileSegment committedSegment;
    try (DiskBlockObjectWriter writer =
        blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse)) {
      //获取存储记录长度的字节数（通常是 4 或 8 字节），用于跳过或读取记录长度
      final int uaoSize = UnsafeAlignedOffset.getUaoSize();
      //磁盘写入循环
      while (sortedRecords.hasNext()) {
        //将下一条记录的指针信息加载到迭代器的内部字段中
        sortedRecords.loadNext();
        //从记录指针中提取当前记录的目标分区 ID
        final int partition = sortedRecords.packedRecordPointer.getPartitionId();
        assert (partition >= currentPartition);
        //如果当前记录的分区 ID 与前一条记录不同，表示需要切换到写入新分区的数据流
        if (partition != currentPartition) {
          // Switch to the new partition
          if (currentPartition != -1) {
            final FileSegment fileSegment = writer.commitAndGet();
            spillInfo.partitionLengths[currentPartition] = fileSegment.length();
          }
          currentPartition = partition;
          if (partitionChecksums.length > 0) {
            writer.setChecksum(partitionChecksums[currentPartition]);
          }
        }
        //从记录指针中获取 Unsafe 编码的内存地址（包含页编号和页内偏移量）
        final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
        final Object recordPage = taskMemoryManager.getPage(recordPointer);
        //通过 TaskMemoryManager 将 Unsafe 编码的地址解码为基对象 (recordPage) 和页内偏移量 (recordOffsetInPage)
        final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
        int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
        long recordReadPosition = recordOffsetInPage + uaoSize; // skip over record length
        //循环将记录数据从内存页拷贝到磁盘
        while (dataRemaining > 0) {
          final int toTransfer = Math.min(diskWriteBufferSize, dataRemaining);

          Platform.copyMemory(
            recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
          writer.write(writeBuffer, 0, toTransfer);
          recordReadPosition += toTransfer;
          dataRemaining -= toTransfer;
        }
        writer.recordWritten();
      }

      committedSegment = writer.commitAndGet();
    }
    // If `writeSortedFile()` was called from `closeAndGetSpills()` and no records were inserted,
    // then the file might be empty. Note that it might be better to avoid calling
    // writeSortedFile() in that case.
    if (currentPartition != -1) {
      spillInfo.partitionLengths[currentPartition] = committedSegment.length();
      spills.add(spillInfo);
    }

    if (!isFinalFile) {  // i.e. this is a spill file
      // The current semantics of `shuffleRecordsWritten` seem to be that it's updated when records
      // are written to disk, not when they enter the shuffle sorting code. DiskBlockObjectWriter
      // relies on its `recordWritten()` method being called in order to trigger periodic updates to
      // `shuffleBytesWritten`. If we were to remove the `recordWritten()` call and increment that
      // counter at a higher-level, then the in-progress metrics for records written and bytes
      // written would get out of sync.
      //
      // When writing the last file, we pass `writeMetrics` directly to the DiskBlockObjectWriter;
      // in all other cases, we pass in a dummy write metrics to capture metrics, then copy those
      // metrics to the true write metrics here. The reason for performing this copying is so that
      // we can avoid reporting spilled bytes as shuffle write bytes.
      //
      // Note that we intentionally ignore the value of `writeMetricsToUse.shuffleWriteTime()`.
      // Consistent with ExternalSorter, we do not count this IO towards shuffle write time.
      // SPARK-3577 tracks the spill time separately.

      // This is guaranteed to be a ShuffleWriteMetrics based on the if check in the beginning
      // of this method.
      writeMetrics.incRecordsWritten(
        ((ShuffleWriteMetrics)writeMetricsToUse).recordsWritten());
      taskContext.taskMetrics().incDiskBytesSpilled(
        ((ShuffleWriteMetrics)writeMetricsToUse).bytesWritten());
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
      return 0L;
    }

    writeSortedFile(false);
    final long spillSize = freeMemory();
    inMemSorter.reset();
    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // records. Otherwise, if the task is over allocated memory, then without freeing the memory
    // pages, we might not be able to get memory for the pointer array.
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    return spillSize;
  }

  private long getMemoryUsage() {
    long totalPageSize = 0;
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  public void cleanupResources() {
    freeMemory();
    if (inMemSorter != null) {
      inMemSorter.free();
      inMemSorter = null;
    }
    for (SpillInfo spill : spills) {
      if (spill.file.exists() && !spill.file.delete()) {
        logger.error("Unable to delete spill file {}", spill.file.getPath());
      }
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  //作用是确保 ShuffleInMemorySorter 中用于存储记录指针的数组有足够的空间来插入新记录。
  // 如果空间不足，它会尝试分配一个更大的数组；如果分配内存失败，则会触发内存溢写（Spill）
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    //如果空间不足，进入扩容逻辑
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      //获取当前指针数组 array 占用的总字节数
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        //尝试申请一个两倍于当前使用量的内存。used / 8 是当前指针的数量，乘以 2 就是期望的新数组大小（元素个数）
        array = allocateArray(used / 8 * 2);
      } catch (TooLargePageException e) {
        //捕获当请求的内存大小（新指针数组）超过 TaskMemoryManager 单个内存页最大限制时抛出的异常
        // The pointer array is too big to fix in a single page, spill.
        spill();
        return;
      } catch (SparkOutOfMemoryError e) {
        //捕获 TaskMemoryManager 确认无法分配所需内存时抛出的异常。这通常意味着尽管尝试了溢写，但内存仍不足
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
      //再次检查 inMemSorter 是否有空间。如果此时有空间，说明内存可能是在 allocateArray 调用过程中（在 TaskMemoryManager 中）通过溢写释放的，并且新的空间已经足够
      //此时，array 变量中存储的新分配内存块是多余的，因为现有数组已经足够。所以释放这个新分配的内存块
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        freeArray(array);
      } else {
        //如果此时 inMemSorter 仍然没有空间，说明内存分配成功，但不足以满足现有数组的需求，或者内存是干净的，没有溢写发生，需要将记录复制到新数组中
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
      // TODO: try to find space in previous pages
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  /**
   * Write a record to the shuffle sorter.
   */
  //将一条记录从输入源复制到内部管理内存，并创建其排序指针的核心方法。它确保了内存页和指针数组有足够空间，然后执行高效的字节拷贝和指针记录
  //记录的基对象 (recordBase)、记录在基对象内的偏移量 (recordOffset)、记录数据的长度 (length)、目标分区 ID (partitionId)
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
    throws IOException {

    // for tests
    assert(inMemSorter != null);
    //检查当前内存中已存储的记录数是否达到强制溢写阈值（numElementsForSpillThreshold）
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      logger.info("Spilling data because number of spilledRecords crossed the threshold " +
        numElementsForSpillThreshold);
      spill();
    }

    growPointerArrayIfNecessary();
    //获取用于存储记录长度的 UAO (Unsafe Aligned Offset) 头部所需的字节数（通常是 4 字节或 8 字节）
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    // Need 4 or 8 bytes to store the record length.
    //计算存储新记录所需的总字节数：记录数据长度 (length) 加上长度占位符所需的空间 (uaoSize)
    final int required = length + uaoSize;
    acquireNewPageIfNecessary(required);

    assert(currentPage != null);
    //即数据将被写入的内存块的引用
    final Object base = currentPage.getBaseObject();
    //使用 TaskMemoryManager 将当前的内存页 (currentPage) 和页内偏移量 (pageCursor) 编码成一个Unsafe 内存地址。这个地址将被用于排序指针
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    //在 currentPage 的当前写入位置 (pageCursor) 写入记录的实际数据长度 (length)。这是 UAO 格式的头部信息。
    UnsafeAlignedOffset.putSize(base, pageCursor, length);
    //将页内游标（pageCursor）向前移动 uaoSize，跳过刚刚写入的记录长度信息，指向实际数据应该开始的位置
    pageCursor += uaoSize;
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;
    inMemSorter.insertRecord(recordAddress, partitionId);
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   */
  public SpillInfo[] closeAndGetSpills() throws IOException {
    if (inMemSorter != null) {
      // Here we are spilling the remaining data in the buffer. If there is no spill before, this
      // final spill file will be the final shuffle output file.
      writeSortedFile(/* isFinalFile = */spills.isEmpty());
      freeMemory();
      inMemSorter.free();
      inMemSorter = null;
    }
    return spills.toArray(new SpillInfo[spills.size()]);
  }

}
