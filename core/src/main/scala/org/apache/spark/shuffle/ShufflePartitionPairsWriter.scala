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

package org.apache.spark.shuffle

import java.io.{Closeable, OutputStream}
import java.util.zip.Checksum

import org.apache.spark.SparkException
import org.apache.spark.io.MutableCheckedOutputStream
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.api.ShufflePartitionWriter
import org.apache.spark.storage.{BlockId, TimeTrackingOutputStream}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.PairsWriter

/**
 * A key-value writer inspired by {@link DiskBlockObjectWriter} that pushes the bytes to an
 * arbitrary partition writer instead of writing to local disk through the block manager.
 */
// Spark 中一个用于 Shuffle 阶段的键值对写入器
// 作用是将 键值对数据序列化并写入到特定的分区输出流 中
//
private[spark] class ShufflePartitionPairsWriter(
    partitionWriter: ShufflePartitionWriter,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    blockId: BlockId,
    writeMetrics: ShuffleWriteMetricsReporter,
    checksum: Checksum)
  extends PairsWriter with Closeable {
  // 用于跟踪写入器是否已被关闭
  private var isClosed = false
  // 提供的原始分区输出流
  private var partitionStream: OutputStream = _
  // 用于追踪写入数据所花费的时间
  private var timeTrackingStream: OutputStream = _
  // 一个被 SerializerManager 包装过的流，通常用于压缩（compression）或加密（encryption）
  private var wrappedStream: OutputStream = _
  // 序列化流。这是实际写入键值对的对象。它负责将键值对对象转换成字节流
  private var objOut: SerializationStream = _
  // 记录已写入的键值对总数
  private var numRecordsWritten = 0
  //记录当前已写入的字节数，用于增量更新写入指标
  private var curNumBytesWritten = 0L
  // this would be only initialized when checksum != null,
  // which indicates shuffle checksum is enabled.
  //一个可选的校验和流
  private var checksumOutputStream: MutableCheckedOutputStream = _

  override def write(key: Any, value: Any): Unit = {
    if (isClosed) {
      throw SparkException.internalError("Partition pairs writer is already closed.", "SHUFFLE")
    }
    if (objOut == null) {
      open()
    }
    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }
  // 初始化所有内部的输出流链
  private def open(): Unit = {
    try {
      // 获取原始输出流
      partitionStream = partitionWriter.openStream
      // 包装它，以记录写入时间
      timeTrackingStream = new TimeTrackingOutputStream(writeMetrics, partitionStream)
      // 如果启用了校验和，则用 MutableCheckedOutputStream 再次包装
      if (checksum != null) {
        checksumOutputStream = new MutableCheckedOutputStream(timeTrackingStream)
        checksumOutputStream.setChecksum(checksum)
      }
      // 进行最终包装（例如压缩）
      wrappedStream = serializerManager.wrapStream(blockId,
        if (checksumOutputStream != null) checksumOutputStream else timeTrackingStream)
      objOut = serializerInstance.serializeStream(wrappedStream)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          close()
        }
        throw e
    }
  }
  // 关闭所有内部的流，释放资源，并最终更新写入指标
  override def close(): Unit = {
    if (!isClosed) {
      Utils.tryWithSafeFinally {
        Utils.tryWithSafeFinally {
          objOut = closeIfNonNull(objOut)
          // Setting these to null will prevent the underlying streams from being closed twice
          // just in case any stream's close() implementation is not idempotent.
          wrappedStream = null
          timeTrackingStream = null
          partitionStream = null
        } {
          // Normally closing objOut would close the inner streams as well, but just in case there
          // was an error in initialization etc. we make sure we clean the other streams up too.
          Utils.tryWithSafeFinally {
            wrappedStream = closeIfNonNull(wrappedStream)
            // Same as above - if wrappedStream closes then assume it closes underlying
            // partitionStream and don't close again in the finally
            timeTrackingStream = null
            partitionStream = null
          } {
            Utils.tryWithSafeFinally {
              timeTrackingStream = closeIfNonNull(timeTrackingStream)
              partitionStream = null
            } {
              partitionStream = closeIfNonNull(partitionStream)
            }
          }
        }
        updateBytesWritten()
      } {
        isClosed = true
      }
    }
  }

  private def closeIfNonNull[T <: Closeable](closeable: T): T = {
    if (closeable != null) {
      closeable.close()
    }
    null.asInstanceOf[T]
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  private def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  private def updateBytesWritten(): Unit = {
    val numBytesWritten = partitionWriter.getNumBytesWritten
    val bytesWrittenDiff = numBytesWritten - curNumBytesWritten
    writeMetrics.incBytesWritten(bytesWrittenDiff)
    curNumBytesWritten = numBytesWritten
  }
}
