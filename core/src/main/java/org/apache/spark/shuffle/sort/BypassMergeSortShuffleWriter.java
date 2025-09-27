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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.zip.Checksum;
import javax.annotation.Nullable;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.checksum.ShuffleChecksumSupport;
import org.apache.spark.internal.config.package$;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;

/**
 * This class implements sort-based shuffle's hash-style shuffle fallback path. This write path
 * writes incoming records to separate files, one file per reduce partition, then concatenates these
 * per-partition files to form a single output file, regions of which are served to reducers.
 * Records are not buffered in memory. It writes output in a format
 * that can be served / consumed via {@link org.apache.spark.shuffle.IndexShuffleBlockResolver}.
 * <p>
 * This write path is inefficient for shuffles with large numbers of reduce partitions because it
 * simultaneously opens separate serializers and file streams for all partitions. As a result,
 * {@link SortShuffleManager} only selects this write path when
 * <ul>
 *    <li>no map-side combine is specified, and</li>
 *    <li>the number of partitions is less than or equal to
 *      <code>spark.shuffle.sort.bypassMergeThreshold</code>.</li>
 * </ul>
 *
 * This code used to be part of {@link org.apache.spark.util.collection.ExternalSorter} but was
 * refactored into its own class in order to reduce code complexity; see SPARK-7855 for details.
 * <p>
 * There have been proposals to completely remove this code path; see SPARK-6026 for details.
 */
//Spark ShuffleWriter 的一个实现，它提供了一种不进行内存排序或合并的 Shuffle 写入方式，作为 SortShuffleManager 的一个优化/旁路（Bypass）路径
  //哈希风格写入（Hash-style Write）
  //它将 Map 任务的输出记录直接写入到多个临时文件中，每个 Reduce 分区对应一个文件
  //这个过程不涉及记录在内存中的缓冲、排序或 Map 端的 Combine 操作
  //旁路合并（Bypass Merge）
  //在写入所有记录后，它会将这些临时的分区文件按顺序拼接（Concatenate）成一个单独的、最终的 Shuffle 输出文件，并生成一个相应的索引文件
  //因此得名“Bypass Merge”，因为它跳过了标准的 Sort Shuffle 路径中通常涉及的内存排序和可能的磁盘文件合并步骤
  //由于它需要同时打开并维护所有 Reduce 分区的写入流，当 Reduce 分区数量过多时，会消耗大量文件句柄和系统资源，效率较低
  //SortShuffleManager 只有在满足以下两个条件时才会选择此优化路径：
  //没有指定 Map 端的 Combine 操作 (no map-side combine is specified)
  //Reduce 分区的数量 (numPartitions) 小于或等于配置参数 spark.shuffle.sort.bypassMergeThreshold（默认值通常是 200）
final class BypassMergeSortShuffleWriter<K, V>
  extends ShuffleWriter<K, V>
  implements ShuffleChecksumSupport {

  private static final Logger logger = LoggerFactory.getLogger(BypassMergeSortShuffleWriter.class);

  private final int fileBufferSize; //文件缓冲区大小（以字节为单位），通过参数spark.shuffle.file.buffer.size配置
  //NIO transferTo 启用标志。配置参数 spark.shuffle.merge.prefer.nio 决定，用于在将临时文件拼接成最终文件时，
  // 是否尝试使用更高效的 FileChannel.transferTo() 方法进行文件传输
  private final boolean transferToEnabled;
  private final int numPartitions; //reduce 分区的数量
  private final BlockManager blockManager;
  private final Partitioner partitioner; //分区器。决定 Map 任务的记录如何被分配到特定的 Reduce 分区
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final int shuffleId; //本次 shuffle 操作的唯一标识 ID
  private final long mapId; //当前 map 任务的唯一标识 ID
  private final Serializer serializer; //序列化器。用于将数据结构（键、值）转换成字节流写入磁盘
  private final ShuffleExecutorComponents shuffleExecutorComponents; //提供创建 shuffle 输出写入器的组件

  /** Array of file writers, one for each partition */
  private DiskBlockObjectWriter[] partitionWriters; //分区写入器数组。核心属性。数组中的每个元素是一个 DiskBlockObjectWriter，用于将特定分区的数据实时写入一个临时的磁盘文件
  private FileSegment[] partitionWriterSegments; //分区文件段数组。用于存储每个临时分区文件写入后的文件元信息（如文件本身、数据开始的偏移量、长度），在拼接文件时使用
  //Map 输出状态（可为空）。保存 Shuffle 输出文件的状态，主要是 blockManager.shuffleServerId()（Shuffle 数据所在的位置）和 partitionLengths（每个分区数据的长度）。Reducer 客户端需要它来获取数据位置
  @Nullable private MapStatus mapStatus;
  //存储最终 Shuffle 输出文件中，每个分区数据块的长度（以字节为单位），用于 Reducer 定位数据
  private long[] partitionLengths;
  /** Checksum calculator for each partition. Empty when shuffle checksum disabled. */
  private final Checksum[] partitionChecksums;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false; //标志是否正在停止写入，防止重复停止操作

  BypassMergeSortShuffleWriter(
      BlockManager blockManager,
      BypassMergeSortShuffleHandle<K, V> handle,
      long mapId,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) throws SparkException {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.transferToEnabled = (boolean) conf.get(package$.MODULE$.SHUFFLE_MERGE_PREFER_NIO());
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.mapId = mapId;
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = writeMetrics;
    this.serializer = dep.serializer();
    this.shuffleExecutorComponents = shuffleExecutorComponents;
    this.partitionChecksums = createPartitionChecksums(numPartitions, conf);
  }
  //通过为每个目标分区创建独立的临时文件，直接将记录写入对应的文件，从而绕过（Bypass） 传统的排序和合并步骤
  //接收一个记录迭代器 (records)，其中每个记录是一个键值对 (Product2<K, V>)，表示要写入的数据
  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    //创建 Map 输出写入器
    ShuffleMapOutputWriter mapOutputWriter = shuffleExecutorComponents
        .createMapOutputWriter(shuffleId, mapId, numPartitions);
    try {
      if (!records.hasNext()) {
        //如果记录迭代器 (records) 为空（没有数据需要 Shuffle）
        partitionLengths = mapOutputWriter.commitAllPartitions(
          ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE).getPartitionLengths();
        mapStatus = MapStatus$.MODULE$.apply(
          blockManager.shuffleServerId(), partitionLengths, mapId);
        return;
      }
      final SerializerInstance serInstance = serializer.newInstance();
      final long openStartTime = System.nanoTime();
      //创建一个数组，用于存储每个分区的 DiskBlockObjectWriter（磁盘块对象写入器），该写入器负责将序列化后的数据写入磁盘上的临时文件
      partitionWriters = new DiskBlockObjectWriter[numPartitions];
      //创建一个数组，用于存储每个分区写入完成后对应的 FileSegment 信息（文件位置和长度）
      partitionWriterSegments = new FileSegment[numPartitions];
      for (int i = 0; i < numPartitions; i++) {
        final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
            blockManager.diskBlockManager().createTempShuffleBlock();
        final File file = tempShuffleBlockIdPlusFile._2();
        final BlockId blockId = tempShuffleBlockIdPlusFile._1();
        //通过 BlockManager 的 DiskBlockManager 创建一个临时 Shuffle 块，这会返回一个包含临时块 ID (TempShuffleBlockId) 和对应临时文件 (File) 的元组
        DiskBlockObjectWriter writer =
          blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
        if (partitionChecksums.length > 0) {
          writer.setChecksum(partitionChecksums[i]);
        }
        //将创建好的 DiskBlockObjectWriter 存储到数组中，以便后续使用
        partitionWriters[i] = writer;
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, and can take a long time in aggregate when we open many files, so should be
      // included in the shuffle write time.
      writeMetrics.incWriteTime(System.nanoTime() - openStartTime);
     //开始循环，处理输入迭代器中的每一条记录
      while (records.hasNext()) {
        final Product2<K, V> record = records.next();
        final K key = record._1();
        //写入到目标分区
        partitionWriters[partitioner.getPartition(key)].write(key, record._2());
      }
      //提交临时文件
      for (int i = 0; i < numPartitions; i++) {
        try (DiskBlockObjectWriter writer = partitionWriters[i]) {
          partitionWriterSegments[i] = writer.commitAndGet();
        }
      }
      //调用 writePartitionedData 方法（这是 BypassMergeSortShuffleWriter 的另一个核心方法）。
      // 在这个阶段，它会利用之前得到的 FileSegment 信息，将所有临时文件的数据提交给 ShuffleMapOutputWriter，
      // 由后者负责将这些数据合并或链接到最终的 Shuffle 数据文件。该方法返回最终每个分区的长度数组
      partitionLengths = writePartitionedData(mapOutputWriter);
      //使用最终的分区长度 (partitionLengths) 创建 MapStatus 对象，作为 ShuffleMapTask 的最终结果报告给 Driver
      mapStatus = MapStatus$.MODULE$.apply(
        blockManager.shuffleServerId(), partitionLengths, mapId);
    } catch (Exception e) {
      try {
        mapOutputWriter.abort(e);
      } catch (Exception e2) {
        logger.error("Failed to abort the writer after failing to write map output.", e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  @Override
  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  //将之前为每个分区创建的独立临时文件中的数据，高效地连接（Concatenate）并写入到最终的 Shuffle 数据文件（通常是单个大文件）中，并返回每个分区在最终文件中的长度
  private long[] writePartitionedData(ShuffleMapOutputWriter mapOutputWriter) throws IOException {
    // Track location of the partition starts in the output file
    if (partitionWriters != null) {
      final long writeStartTime = System.nanoTime();
      try {
        for (int i = 0; i < numPartitions; i++) {
          //从 partitionWriterSegments 数组中获取当前分区 i 对应的临时文件对象
          final File file = partitionWriterSegments[i].file();
          //通过 mapOutputWriter 获取一个用于写入当前分区 i 数据的 ShufflePartitionWriter。这个写入器是针对最终 Shuffle 文件的抽象接口
          ShufflePartitionWriter writer = mapOutputWriter.getPartitionWriter(i);
          if (file.exists()) {
            //检查是否启用了 transferTo 优化（即零拷贝技术）。这是一种高效的文件数据传输机制，可以避免数据在内核空间和用户空间之间不必要的复制
            if (transferToEnabled) {
              // Using WritableByteChannelWrapper to make resource closing consistent between
              // this implementation and UnsafeShuffleWriter.
              Optional<WritableByteChannelWrapper> maybeOutputChannel = writer.openChannelWrapper();
              if (maybeOutputChannel.isPresent()) {
                //如果成功获取了通道（通常意味着底层支持零拷贝）
                writePartitionedDataWithChannel(file, maybeOutputChannel.get());
              } else {
                //如果 writer 无法提供通道（例如，目标输出不是文件通道，或者零拷贝不可用）
                writePartitionedDataWithStream(file, writer);
              }
            } else {
              writePartitionedDataWithStream(file, writer);
            }
            if (!file.delete()) {
              logger.error("Unable to delete file for partition {}", i);
            }
          }
        }
      } finally {
        writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
      }
      partitionWriters = null;
    }
    return mapOutputWriter.commitAllPartitions(getChecksumValues(partitionChecksums))
      .getPartitionLengths();
  }

  private void writePartitionedDataWithChannel(
      File file,
      WritableByteChannelWrapper outputChannel) throws IOException {
    boolean copyThrewException = true;
    try {
      FileInputStream in = new FileInputStream(file);
      try (FileChannel inputChannel = in.getChannel()) {
        Utils.copyFileStreamNIO(
            inputChannel, outputChannel.channel(), 0L, inputChannel.size());
        copyThrewException = false;
      } finally {
        Closeables.close(in, copyThrewException);
      }
    } finally {
      Closeables.close(outputChannel, copyThrewException);
    }
  }

  private void writePartitionedDataWithStream(File file, ShufflePartitionWriter writer)
      throws IOException {
    boolean copyThrewException = true;
    FileInputStream in = new FileInputStream(file);
    OutputStream outputStream;
    try {
      outputStream = writer.openStream();
      try {
        Utils.copyStream(in, outputStream, false, false);
        copyThrewException = false;
      } finally {
        Closeables.close(outputStream, copyThrewException);
      }
    } finally {
      Closeables.close(in, copyThrewException);
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (DiskBlockObjectWriter writer : partitionWriters) {
              // This method explicitly does _not_ throw exceptions:
              writer.closeAndDelete();
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}
