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

/**
 * Wrapper around an 8-byte word that holds a 24-bit partition number and 40-bit record pointer.
 * <p>
 * Within the long, the data is laid out as follows:
 * <pre>
 *   [24 bit partition number][13 bit memory page number][27 bit offset in page]
 * </pre>
 * This implies that the maximum addressable page size is 2^27 bits = 128 megabytes, assuming that
 * our offsets in pages are not 8-byte-word-aligned. Since we have 2^13 pages (based off the
 * 13-bit page numbers assigned by {@link org.apache.spark.memory.TaskMemoryManager}), this
 * implies that we can address 2^13 * 128 megabytes = 1 terabyte of RAM per task.
 * <p>
 * Assuming word-alignment would allow for a 1 gigabyte maximum page size, but we leave this
 * optimization to future work as it will require more careful design to ensure that addresses are
 * properly aligned (e.g. by padding records).
 */
//Spark Tungsten 引擎中用于 Shuffle 排序阶段的核心数据结构。它的主要作用是将两个关键信息（记录的内存地址和它所属的分区 ID） 紧凑地编码到一个标准的 8 字节 long 整数中
//内存效率：排序器需要维护大量的记录指针。通过将地址和分区 ID打包到一个 long 中，可以使每个指针只占用 8 字节，显著减少内存开销
//排序效率：在对记录进行排序时，ShuffleInMemorySorter 只需要对这个 long 数组进行排序。
// 由于分区 ID 被放置在 long 的高位（Most Significant Bits），对整个 long 进行数值排序（如 Radix Sort 或快速排序）就可以直接实现按分区 ID 的正确分组和排序
  //24 位：分区 ID（最高位）
  //13 位：内存页编号
  //27 位：页内偏移量（最低位）
final class PackedRecordPointer {

  //单个内存页的最大尺寸
  // 2**27 字节 (128 MB)。这是由页内偏移量 (27 位) 决定的最大可寻址页面大小
  static final int MAXIMUM_PAGE_SIZE_BYTES = 1 << 27;  // 128 megabytes

  /**
   * The maximum partition identifier that can be encoded. Note that partition ids start from 0.
   */
  //可编码的最大分区 ID
  //(16,777,215)。由分区 ID 占用的 24 位决定。Spark 任务支持的分区数被限制在这个值
  static final int MAXIMUM_PARTITION_ID = (1 << 24) - 1;  // 16777215

  /**
   * The index of the first byte of the partition id, counting from the least significant byte.
   */
  static final int PARTITION_ID_START_BYTE_INDEX = 5;

  /**
   * The index of the last byte of the partition id, counting from the least significant byte.
   */
  static final int PARTITION_ID_END_BYTE_INDEX = 7;

  /** Bit mask for the lower 40 bits of a long. */
  //低 40 位掩码
  //用于屏蔽 long 的高 24 位，仅保留内存地址（页编号和偏移量）信息
  //0000000000001111111111111111111111111111111111111111
  private static final long MASK_LONG_LOWER_40_BITS = (1L << 40) - 1;

  /** Bit mask for the upper 24 bits of a long */
  //高 24 位掩码
  //用于屏蔽 long 的低 40 位，仅保留分区 ID 信息
  private static final long MASK_LONG_UPPER_24_BITS = ~MASK_LONG_LOWER_40_BITS;

  /** Bit mask for the lower 27 bits of a long. */
  //低 27 位掩码
  //用于从 TaskMemoryManager 的地址中提取页内偏移量
  private static final long MASK_LONG_LOWER_27_BITS = (1L << 27) - 1;

  /** Bit mask for the lower 51 bits of a long. */
  private static final long MASK_LONG_LOWER_51_BITS = (1L << 51) - 1;

  /** Bit mask for the upper 13 bits of a long */
  //高 13 位掩码
  //用于从 TaskMemoryManager 的地址中提取内存页编号
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  /**
   * Pack a record address and partition id into a single word.
   *
   * @param recordPointer a record pointer encoded by TaskMemoryManager.
   * @param partitionId a shuffle partition id (maximum value of 2^24).
   * @return a packed pointer that can be decoded using the {@link PackedRecordPointer} class.
   */
  //将原始的记录内存地址和分区 ID 编码成一个 8 字节的 long
  public static long packPointer(long recordPointer, int partitionId) {
    //确保 partitionId 不超过最大值
    assert (partitionId <= MAXIMUM_PARTITION_ID);
    // Note that without word alignment we can address 2^27 bytes = 128 megabytes per page.
    // Also note that this relies on some internals of how TaskMemoryManager encodes its addresses.
    //记录地址取高13位作为页编码，右移24是因为编码后的地址最高24位是分区编码
    final long pageNumber = (recordPointer & MASK_LONG_UPPER_13_BITS) >>> 24;
    //将页编号和页内偏移量通过位或操作合并为 40 位的 compressedAddress
    final long compressedAddress = pageNumber | (recordPointer & MASK_LONG_LOWER_27_BITS);
    return (((long) partitionId) << 40) | compressedAddress;
  }
  //存储已打包的 8 字节 long 指针
  private long packedRecordPointer;
  //设置打包指针
  public void set(long packedRecordPointer) {
    this.packedRecordPointer = packedRecordPointer;
  }
  //从打包指针中解包出 24 位的分区 ID
  public int getPartitionId() {
    return (int) ((packedRecordPointer & MASK_LONG_UPPER_24_BITS) >>> 40);
  }
  //获取原始记录内存地址
  public long getRecordPointer() {
    final long pageNumber = (packedRecordPointer << 24) & MASK_LONG_UPPER_13_BITS;
    final long offsetInPage = packedRecordPointer & MASK_LONG_LOWER_27_BITS;
    return pageNumber | offsetInPage;
  }

}
