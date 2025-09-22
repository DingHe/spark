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

package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._

object PartitionedFileUtil {
  def splitFiles(   //主要用于将一个大文件拆分为多个小的 PartitionedFile，方便 Spark 在多节点上并行处理
      sparkSession: SparkSession,
      file: FileStatusWithMetadata, //表示一个待处理的文件，包含文件的路径、长度、元数据（如修改时间等）
      isSplitable: Boolean, //文件是否可切分的标志
      maxSplitBytes: Long,  //每个分片的最大字节数，决定了文件分片的大小
      partitionValues: InternalRow): Seq[PartitionedFile] = { //文件所在的分区值，表示与文件相关的分区信息，在处理分区表时使用
    if (isSplitable) { //如果文件 可以切分（如文本文件），进入切分逻辑
      (0L until file.getLen by maxSplitBytes).map { offset =>  //创建一个以指定步长递增或递减的整数范围，语法start until end by step或者start to end by step，to 方法包含结束值，until 方法不包含
        val remaining = file.getLen - offset
        val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining  //如果剩余数据大于 maxSplitBytes，当前分片大小为 maxSplitBytes，如果剩余数据不足，则使用剩余数据的大小
        val hosts = getBlockHosts(getBlockLocations(file.fileStatus), offset, size)
        PartitionedFile(partitionValues, SparkPath.fromPath(file.getPath), offset, size, hosts,
          file.getModificationTime, file.getLen, file.metadata)
      }
    } else {
      Seq(getPartitionedFile(file, partitionValues))
    }
  }
  //如果文件不可切分，则整个读取
  def getPartitionedFile(
      file: FileStatusWithMetadata,
      partitionValues: InternalRow): PartitionedFile = {
    val hosts = getBlockHosts(getBlockLocations(file.fileStatus), 0, file.getLen)
    PartitionedFile(partitionValues, SparkPath.fromPath(file.getPath), 0, file.getLen, hosts,
      file.getModificationTime, file.getLen, file.metadata)
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }
  //主要用于根据文件的分块信息，确定给定数据片段（offset 和 length）所在的最佳数据块，并返回该数据块所在的主机列表。Spark 使用此信息优化数据读取，尽量将计算推送到数据所在的节点，提升数据局部性（Data Locality）
  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(
      blockLocations: Array[BlockLocation], //文件在 HDFS（或其他分布式文件系统）上的所有数据块（Block）的位置信息，getOffset: Long：数据块的起始偏移量，getLength: Long：数据块的长度（字节数），getHosts: Array[String]：该数据块所在的主机列表（DataNode 节点）
      offset: Long,
      length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block. It handles the case where the
      // fragment is fully contained in the block.
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>  //数据片段起始偏移在当前数据块中
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if b.getOffset < offset + length && offset + length < b.getOffset + b.getLength =>  //数据片段终止偏移在当前数据块中
        b.getHosts -> (offset + length - b.getOffset)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>  //当前数据块被数据片段完全覆盖
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>  //数据片段与当前数据块没有重叠
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }
    //返回最佳数据块的主机列表
    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }
}

