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

package org.apache.spark.shuffle.sort.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.internal.config.package$;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage;

/**
 * Implementation of {@link ShuffleMapOutputWriter} that replicates the functionality of shuffle
 * persisting shuffle data to local disk alongside index files, identical to Spark's historic
 * canonical shuffle storage mechanism.
 */
//用于将 shuffle 数据以结构化的方式存储到本地磁盘
public class LocalDiskShuffleMapOutputWriter implements ShuffleMapOutputWriter {

  private static final Logger log =
    LoggerFactory.getLogger(LocalDiskShuffleMapOutputWriter.class);

  private final int shuffleId; //标识 shuffle 阶段的唯一 ID
  private final long mapId; //标识生成 shuffle 数据的 map 任务的唯一标识符
  private final IndexShuffleBlockResolver blockResolver; //负责管理 shuffle 的元数据文件
  //用于存储每个分区写入的字节数
  private final long[] partitionLengths;
  private final int bufferSize; //文件缓冲区的大小（以字节为单位）
  private int lastPartitionId = -1;
  private long currChannelPosition;
  //记录总共写入了多少字节
  private long bytesWrittenToMergedFile = 0L;
  //最终文件的输出路径
  private final File outputFile;
  //临时文件路径
  private File outputTempFile;
  //传统Stream的写入方式
  private FileOutputStream outputFileStream;
  //根据outputFileStream构建缓冲流
  private BufferedOutputStream outputBufferedFileStream;
  //Channel写入方式的通道
  private FileChannel outputFileChannel;


  public LocalDiskShuffleMapOutputWriter(
      int shuffleId,
      long mapId,
      int numPartitions,
      IndexShuffleBlockResolver blockResolver,
      SparkConf sparkConf) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.blockResolver = blockResolver;
    this.bufferSize =
      (int) (long) sparkConf.get(
        package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    this.partitionLengths = new long[numPartitions];
    this.outputFile = blockResolver.getDataFile(shuffleId, mapId);
    this.outputTempFile = null;
  }
  //用于获取特定分区的写入器 LocalDiskShufflePartitionWriter
  @Override
  public ShufflePartitionWriter getPartitionWriter(int reducePartitionId) throws IOException {
    if (reducePartitionId <= lastPartitionId) {
      throw new IllegalArgumentException("Partitions should be requested in increasing order.");
    }
    lastPartitionId = reducePartitionId;
    //创建临时文件
    if (outputTempFile == null) {
      outputTempFile = blockResolver.createTempFile(outputFile);
    }
    if (outputFileChannel != null) {
      currChannelPosition = outputFileChannel.position();
    } else {
      currChannelPosition = 0L;
    }
    return new LocalDiskShufflePartitionWriter(reducePartitionId);
  }
  //在所有分区数据成功写入后调用，用于提交所有写入的分区数据
  @Override
  public MapOutputCommitMessage commitAllPartitions(long[] checksums) throws IOException {
    // Check the position after transferTo loop to see if it is in the right position and raise a
    // exception if it is incorrect. The position will not be increased to the expected length
    // after calling transferTo in kernel version 2.6.32. This issue is described at
    // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
    if (outputFileChannel != null && outputFileChannel.position() != bytesWrittenToMergedFile) {
      throw new IOException(
          "Current position " + outputFileChannel.position() + " does not equal expected " +
              "position " + bytesWrittenToMergedFile + " after transferTo. Please check your " +
              " kernel version to see if it is 2.6.32, as there is a kernel bug which will lead " +
              "to unexpected behavior when using transferTo. You can set " +
              "spark.file.transferTo=false to disable this NIO feature.");
    }
    cleanUp();
    File resolvedTmp = outputTempFile != null && outputTempFile.isFile() ? outputTempFile : null;
    log.debug("Writing shuffle index file for mapId {} with length {}", mapId,
        partitionLengths.length);
    blockResolver   //写入元数据文件并提交临时文件
      .writeMetadataFileAndCommit(shuffleId, mapId, partitionLengths, checksums, resolvedTmp);
    return MapOutputCommitMessage.of(partitionLengths);
  }
  //发生错误就把临时文件清理掉
  @Override
  public void abort(Throwable error) throws IOException {
    cleanUp();
    if (outputTempFile != null && outputTempFile.exists() && !outputTempFile.delete()) {
      log.warn("Failed to delete temporary shuffle file at {}", outputTempFile.getAbsolutePath());
    }
  }
  //关闭相关的流
  private void cleanUp() throws IOException {
    if (outputBufferedFileStream != null) {
      outputBufferedFileStream.close();
    }
    if (outputFileChannel != null) {
      outputFileChannel.close();
    }
    if (outputFileStream != null) {
      outputFileStream.close();
    }
  }
  //初始化输出流
  private void initStream() throws IOException {
    if (outputFileStream == null) {
      outputFileStream = new FileOutputStream(outputTempFile, true);
    }
    if (outputBufferedFileStream == null) {
      outputBufferedFileStream = new BufferedOutputStream(outputFileStream, bufferSize);
    }
  }
  //初始化Channel写入方式
  private void initChannel() throws IOException {
    // This file needs to opened in append mode in order to work around a Linux kernel bug that
    // affects transferTo; see SPARK-3948 for more details.
    if (outputFileChannel == null) {
      outputFileChannel = new FileOutputStream(outputTempFile, true).getChannel();
    }
  }
  // Spark 中用于将 Shuffle 数据写入本地磁盘的抽象分区写入器。
  // 它的核心作用是根据写入模式（流式或通道式）来管理和返回正确的写入对象，从而实现将特定分区的数据写入到底层的共享文件中
  //实现了 ShufflePartitionWriter 接口，为外部调用者（例如 ShuffleMapOutputWriter）提供了一个统一的 API，使它们能够：
  //根据需要获取一个输出流 (OutputStream) 或一个可写字节通道 (WritableByteChannelWrapper)
  //确保一个分区在整个写入过程中，只能选择一种写入方式（要么用流，要么用通道），防止 API 混用导致的错误
  //追踪和报告该分区写入的字节数，而无需关心底层是如何写入的
  private class LocalDiskShufflePartitionWriter implements ShufflePartitionWriter {
    //存储这个写入器所对应的分区的唯一 ID
    private final int partitionId;
    private PartitionWriterStream partStream = null; //表示分区的输出流实例
    private PartitionWriterChannel partChannel = null; //表示分区的输出通道实例

    private LocalDiskShufflePartitionWriter(int partitionId) {
      this.partitionId = partitionId;
    }
    //获取一个用于流式写入的 OutputStream
    @Override
    public OutputStream openStream() throws IOException {
      if (partStream == null) {
        //进行一个安全检查。如果底层已经创建了通道 (outputFileChannel)，这说明可能已经开始使用通道模式进行写入。此时抛出 IllegalStateException
        if (outputFileChannel != null) {
          throw new IllegalStateException("Requested an output channel for a previous write but" +
              " now an output stream has been requested. Should not be using both channels" +
              " and streams to write.");
        }
        initStream();
        partStream = new PartitionWriterStream(partitionId);
      }
      return partStream;
    }
    //获取一个用于通道式写入的 WritableByteChannelWrapper
    @Override
    public Optional<WritableByteChannelWrapper> openChannelWrapper() throws IOException {
      if (partChannel == null) {
        if (partStream != null) {
          throw new IllegalStateException("Requested an output stream for a previous write but" +
              " now an output channel has been requested. Should not be using both channels" +
              " and streams to write.");
        }
        initChannel();
        partChannel = new PartitionWriterChannel(partitionId);
      }
      return Optional.of(partChannel);
    }

    @Override
    public long getNumBytesWritten() {
      if (partChannel != null) {
        try {
          return partChannel.getCount();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else if (partStream != null) {
        return partStream.getCount();
      } else {
        // Assume an empty partition if stream and channel are never created
        return 0;
      }
    }
  }
  //主要作用是为每个分区提供一个包装过的 OutputStream，并准确记录该分区写入的字节数
  private class PartitionWriterStream extends OutputStream {
    //存储当前流所对应的分区 ID，作为其唯一标识
    private final int partitionId;
    //记录通过这个流写入的字节总数
    private long count = 0;
    //标记这个流是否已经被关闭。用于防止在流关闭后继续写入数据
    private boolean isClosed = false;

    PartitionWriterStream(int partitionId) {
      this.partitionId = partitionId;
    }
    //返回当前分区通过这个流写入的字节数
    public long getCount() {
      return count;
    }
    //写入一个字节。它重写了 OutputStream 的抽象方法
    @Override
    public void write(int b) throws IOException {
      verifyNotClosed();
      //共享输出流
      outputBufferedFileStream.write(b);
      count++;
    }
    //写入一个字节数组中的一部分。它重写了 OutputStream 的方法
    @Override
    public void write(byte[] buf, int pos, int length) throws IOException {
      verifyNotClosed();
      outputBufferedFileStream.write(buf, pos, length);
      count += length;
    }

    @Override
    public void close() {
      isClosed = true;
      //记录partitionID分区写入的字节数
      partitionLengths[partitionId] = count;
      bytesWrittenToMergedFile += count;
    }

    private void verifyNotClosed() {
      if (isClosed) {
        throw new IllegalStateException("Attempting to write to a closed block output stream.");
      }
    }
  }
  //核心作用是为每个分区提供一个可写的字节通道（WritableByteChannel）的包装器，并记录该分区写入的字节数
  //在 SortShuffleWriter 中，所有分区的输出数据通常会被写入同一个文件。为了实现这个功能，
  // SortShuffleWriter 需要为每个分区维护一个独立的写入通道，并精确地记录每个分区写入的起始和结束位置
  private class PartitionWriterChannel implements WritableByteChannelWrapper {
    //存储当前通道包装器所对应的分区 ID
    private final int partitionId;

    PartitionWriterChannel(int partitionId) {
      this.partitionId = partitionId;
    }
    //计算并返回当前分区已经写入的字节数
    public long getCount() throws IOException {
      long writtenPosition = outputFileChannel.position();
      return writtenPosition - currChannelPosition;
    }
    //共享输出Channel
    @Override
    public WritableByteChannel channel() {
      return outputFileChannel;
    }

    @Override
    public void close() throws IOException {
      partitionLengths[partitionId] = getCount();
      bytesWrittenToMergedFile += partitionLengths[partitionId];
    }
  }
}
