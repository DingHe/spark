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
package org.apache.spark.sql.execution.datasources

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.internal.SQLConf

/**
 * A collection of file blocks that should be read as a single task
 * (possibly from multiple partitioned directories).
 */
//主要作用是 组织 Spark 任务对文件进行并行读取
case class FilePartition(index: Int, files: Array[PartitionedFile])
  extends Partition with InputPartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    //遍历 files 数组，统计每个主机上的数据量
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }
    //返回数据最多的前三台机器作为偏好位置
    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }.toArray
  }
}

object FilePartition extends Logging {

  private def getFilePartitions(
      partitionedFiles: Seq[PartitionedFile], //待分配的文件列表，每个文件都是 PartitionedFile
      maxSplitBytes: Long, //单个 FilePartition 最大数据量，取“默认的128M”和“所有文件的大小除以最小分区数量”的最小值
      openCostInBytes: Long): Seq[FilePartition] = { //每个文件的打开开销
    val partitions = new ArrayBuffer[FilePartition]  //当前 FilePartition 存放的文件列表
    val currentFiles = new ArrayBuffer[PartitionedFile] //当前 FilePartition 的数据大小
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        // Copy to a new Array.
        val newPartition = FilePartition(partitions.size, currentFiles.toArray)
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // Assign files to partitions using "Next Fit Decreasing"
    partitionedFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()
    partitions.toSeq
  }
  //通过 Spark 配置动态调整 maxSplitBytes，控制分区数量
  def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    val openCostBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxPartNum = sparkSession.sessionState.conf.filesMaxPartitionNum  //文件可以切分的最大分区
    val partitions = getFilePartitions(partitionedFiles, maxSplitBytes, openCostBytes)
    //如果设置了spark.sql.files.maxPartitionNum，并且FilePartition的数量大于此参数值，则要重新计算分区。
    if (maxPartNum.exists(partitions.size > _)) {
      val totalSizeInBytes =
        partitionedFiles.map(_.length + openCostBytes).map(BigDecimal(_)).sum[BigDecimal]
      val desiredSplitBytes =  //按照spark.sql.files.maxPartitionNum计算文件的分区大小
        (totalSizeInBytes / BigDecimal(maxPartNum.get)).setScale(0, RoundingMode.UP).longValue
      val desiredPartitions = getFilePartitions(partitionedFiles, desiredSplitBytes, openCostBytes)
      logWarning(s"The number of partitions is ${partitions.size}, which exceeds the maximum " +
        s"number configured: ${maxPartNum.get}. Spark rescales it to ${desiredPartitions.size} " +
        s"by ignoring the configuration of ${SQLConf.FILES_MAX_PARTITION_BYTES.key}.")
      desiredPartitions
    } else {
      partitions
    }
  }
  //用于计算每个文件分区的最大字节数，它主要考虑了文件的总大小、打开文件的成本以及分区的最小数量
  def maxSplitBytes(
      sparkSession: SparkSession,
      selectedPartitions: Seq[PartitionDirectory]): Long = {//表示所有被选中的 Hive 分区
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes  //默认的最大分区字节数
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes   //文件打开成本
    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum  //最小分区数量
      .getOrElse(sparkSession.leafNodeDefaultParallelism)
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / minPartitionNum  //每个分区应处理的字节数
    //按照这个代码理解，每个分区的数据大小上限是defaultMaxSplitBytes，如果所有文件的大小除以最小分区数量，则取该值
    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
}
