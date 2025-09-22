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

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.storage.{BlockId, ShuffleMergedBlockId}

private[spark]
/**
 * Implementers of this trait understand how to retrieve block data for a logical shuffle block
 * identifier (i.e. map, reduce, and shuffle). Implementations may use files or file segments to
 * encapsulate shuffle data. This is used by the BlockStore to abstract over different shuffle
 * implementations when shuffle data is retrieved.
 */
// 用于检索逻辑shuffle块数据
trait ShuffleBlockResolver {
  type ShuffleId = Int

  /**
   * Retrieve the data for the specified block.
   *
   * When the dirs parameter is None then use the disk manager's local directories. Otherwise,
   * read from the specified directories.
   * If the data for that block is not available, throws an unspecified exception.
   */
  // 该方法用于根据给定的 blockId 检索对应的数据。blockId 是指一个逻辑的 Shuffle 块标识符，
  // 通常是 Map 块或 Reduce 块等。方法的返回值是一个 ManagedBuffer，表示该数据块的缓冲区
  def getBlockData(blockId: BlockId, dirs: Option[Array[String]] = None): ManagedBuffer

  /**该方法用于获取与给定 Shuffle ID 和 Map ID 相关的所有块的 BlockId 列表。
   * 这对于清理操作非常重要，尤其是在外部 Shuffle 服务中删除与特定 Map 任务相关的文件时
   * Retrieve a list of BlockIds for a given shuffle map. Used to delete shuffle files
   * from the external shuffle service after the associated executor has been removed.
   */
  def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    Seq.empty
  }

  /** 该方法用于获取合并的 Shuffle 数据块的多个部分（分块）。
   * 当多个 Shuffle 数据块被合并时，这个方法可以返回合并后的多个数据块。
   * 它返回的数据是多个 ManagedBuffer，每个缓冲区表示合并的一个分块
   * Retrieve the data for the specified merged shuffle block as multiple chunks.
   */
  def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer]

  /** 该方法用于检索与合并 Shuffle 数据块相关的元数据。MergedBlockMeta 提供了合并块的额外信息
   * Retrieve the meta data for the specified merged shuffle block.
   */
  def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta

  def stop(): Unit
}
