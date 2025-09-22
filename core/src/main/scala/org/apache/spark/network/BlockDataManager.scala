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

package org.apache.spark.network

import scala.reflect.ClassTag

import org.apache.spark.TaskContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.shuffle.checksum.Cause
import org.apache.spark.storage.{BlockId, StorageLevel}
//特质提供了一些用于操作、存储和检索数据块的功能，特别是关于本地存储、数据块操作和错误诊断的内容
private[spark]
trait BlockDataManager {

  /** 诊断Shuffle数据损坏的可能原因。通过校验数据块的校验和来确认问题
   * Diagnose the possible cause of the shuffle data corruption by verifying the shuffle checksums
   */
  def diagnoseShuffleBlockCorruption(
      blockId: BlockId,
      checksumByReader: Long,
      algorithm: String): Cause

  /** 获取用于保存数据块的本地磁盘目录
   * Get the local directories that used by BlockManager to save the blocks to disk
   */
  def getLocalDiskDirs: Array[String]

  /** 获取主机本地的Shuffle数据。若数据块不存在或无法成功读取，会抛出异常
   * Interface to get host-local shuffle block data. Throws an exception if the block cannot be
   * found or cannot be read successfully.
   */
  def getHostLocalShuffleData(blockId: BlockId, dirs: Array[String]): ManagedBuffer

  /** 获取本地存储的数据块。如果数据块不存在或读取失败，抛出异常。
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   */
  def getLocalBlockData(blockId: BlockId): ManagedBuffer

  /**
   * Put the block locally, using the given storage level.
   * 将数据块存储到本地。可以设置存储级别（StorageLevel），控制数据的存储方式
   * Returns true if the block was stored and false if the put operation failed or the block
   * already existed.
   */
  def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Boolean

  /**
   * Put the given block that will be received as a stream.
   * 将接收到的数据块作为流来存储。该方法在数据块实际可用之前被调用，数据将通过 StreamCallbackWithID 进行处理
   * When this method is called, the block data itself is not available -- it will be passed to the
   * returned StreamCallbackWithID.
   */
  def putBlockDataAsStream(
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_]): StreamCallbackWithID

  /**
   * Release locks acquired by [[putBlockData()]] and [[getLocalBlockData()]].
   */
  def releaseLock(blockId: BlockId, taskContext: Option[TaskContext]): Unit
}
