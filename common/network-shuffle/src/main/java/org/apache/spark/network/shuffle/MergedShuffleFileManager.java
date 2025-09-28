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

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.util.Collections;

import com.codahale.metrics.MetricSet;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.FinalizeShuffleMerge;
import org.apache.spark.network.shuffle.protocol.MergeStatuses;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.shuffle.protocol.RemoveShuffleMerge;

/**
 * The MergedShuffleFileManager is used to process push based shuffle when enabled. It works
 * along side {@link ExternalBlockHandler} and serves as an RPCHandler for
 * {@link org.apache.spark.network.server.RpcHandler#receiveStream}, where it processes the
 * remotely pushed streams of shuffle blocks to merge them into merged shuffle files. Right
 * now, support for push based shuffle is only implemented for external shuffle service in
 * YARN mode.
 *
 * @since 3.1.0
 */
// Spark 中用于支持 **Push-based Shuffle（基于推送的 Shuffle）**机制的核心接口。它运行在 外部 Shuffle Service 进程中（在 YARN 模式下尤为常见）
  //传统的 Spark Shuffle 是 Pull-based：Mapper 任务完成并写入本地文件后，Reducer 任务再远程拉取这些数据。
  //MergedShuffleFileManager 实现了 Push 机制：
  //Executor (Mapper) 任务不再写入自己的本地磁盘，而是直接将 Shuffle 数据流式 推送到 外部 Shuffle Service
  //Shuffle Service 使用这个管理器接收、合并 (Merge) 并将这些数据写入合并后的 Shuffle 文件。
  //核心职责：
  //处理数据流： 作为 Netty RPC 处理器的一部分，它接收来自 Executor 的 Shuffle 数据流，并在数据到达时即时进行处理和合并。
  //文件合并： 将来自不同 Mapper（可能属于不同的 Executor）的、发往同一个 Reducer 的数据合并到一个文件中，从而减少 Reducer 拉取时需要打开的文件数量，提高拉取效率。
  //元数据管理： 注册 Executor 信息，管理合并后的 Shuffle 文件的元数据（如文件路径、块信息）。
  //提供服务： 当 Reducer 任务运行时，它负责从合并后的文件中检索和提供请求的数据块。
@Evolving
public interface MergedShuffleFileManager {
  /**
   * Provides the stream callback used to process a remotely pushed block. The callback is
   * used by the {@link org.apache.spark.network.client.StreamInterceptor} installed on the
   * channel to process the block data in the channel outside of the message frame.
   *
   * @param msg metadata of the remotely pushed blocks. This is processed inside the message frame
   * @return A stream callback to process the block data in streaming fashion as it arrives
   */
  //用于处理基于推送的 Shuffle 数据流。它接收一个 PushBlockStream 消息（包含推送块的元数据），并返回一个 StreamCallbackWithID。
  // 这个回调函数会被 Netty StreamInterceptor 用来在消息帧之外以流式方式处理实际的块数据（即进行合并写入操作），防止内存阻塞
  StreamCallbackWithID receiveBlockDataAsStream(PushBlockStream msg);

  /**
   * Handles the request to finalize shuffle merge for a given shuffle.
   *
   * @param msg contains appId and shuffleId to uniquely identify a shuffle to be finalized
   * @return The statuses of the merged shuffle partitions for the given shuffle on this
   *         shuffle service
   * @throws IOException
   */
  // 当一个 Shuffle Stage 完成所有 Map 任务的推送后，Executor 或 Driver 会调用此方法通知 Shuffle Service。
  // 此方法执行最终的收尾工作（如关闭所有合并文件句柄、清理临时状态），并返回合并分区的状态 (MergeStatuses)
  MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg) throws IOException;

  /**
   * Registers an executor with MergedShuffleFileManager. This executor-info provides
   * the directories and number of sub-dirs per dir so that MergedShuffleFileManager knows where to
   * store and look for shuffle data for a given application. It is invoked by the RPC call when
   * executor tries to register with the local shuffle service.
   *
   * @param appId application ID
   * @param executorInfo The list of local dirs that this executor gets granted from NodeManager
   */
  //允许 Shuffle Service 记录 Executor 的关键信息（如本地目录 → localDirs 和子目录数量 → subDirsPerLocalDir），以便知道在哪里存储和查找该应用的数据。
  void registerExecutor(String appId, ExecutorShuffleInfo executorInfo);

  /**
   * Invoked when an application finishes. This cleans up any remaining metadata associated with
   * this application, and optionally deletes the application specific directory path.
   *
   * @param appId application ID
   * @param cleanupLocalDirs flag indicating whether MergedShuffleFileManager should handle
   *                         deletion of local dirs itself.
   */
  //当一个应用程序结束时调用。它负责清理与该应用程序相关的所有内存元数据和持久化状态。cleanupLocalDirs 参数指示是否需要在底层文件系统上删除该应用遗留的 Shuffle 目录。
  void applicationRemoved(String appId, boolean cleanupLocalDirs);

  /**
   * Get the buffer for a given merged shuffle chunk when serving merged shuffle to reducers
   *
   * @param appId application ID
   * @param shuffleId shuffle ID
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reducer ID
   * @param chunkId merged shuffle file chunk ID
   * @return The {@link ManagedBuffer} for the given merged shuffle chunk
   */

  //供 Reducer 任务拉取数据时调用。它根据应用的 ID、Shuffle ID、Merge ID、Reduce ID 和 Chunk ID，返回一个封装了实际数据片段的 ManagedBuffer
  ManagedBuffer getMergedBlockData(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      int chunkId);

  /**
   * Get the meta information of a merged block.
   *
   * @param appId application ID
   * @param shuffleId shuffle ID
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reducer ID
   * @return meta information of a merged block
   */

  //用于获取合并后的数据块的元数据（如块的总长度、块的数量等）。这些信息对于 Reducer 正确规划数据拉取至关重要
  MergedBlockMeta getMergedBlockMeta(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId);

  /**
   * Get the local directories which stores the merged shuffle files.
   *
   * @param appId application ID
   */
  //返回存储特定 appId 的合并后的 Shuffle 文件的本地目录数组
  String[] getMergedBlockDirs(String appId);

  /**
   * Remove shuffle merge data files.
   *
   * @param removeShuffleMerge contains shuffle details (appId, shuffleId, etc) to uniquely
   * identify a shuffle to be removed
   */
  //用于清除与特定 Shuffle Merge 过程相关的中间或最终数据文件和元数据。这通常在 Shuffle 任务失败或不再需要数据时调用。
  void removeShuffleMerge(RemoveShuffleMerge removeShuffleMerge);

  /**
   * Optionally close any resources associated the MergedShuffleFileManager, such as the
   * leveldb for state persistence.
   */
  default void close() {}

  /**
   * Get the metrics associated with the MergedShuffleFileManager. E.g., this is used to collect
   * the push merged metrics within RemoteBlockPushResolver.
   *
   * @return the map contains the metrics
   */
  default MetricSet getMetrics() {
    return Collections::emptyMap;
  }
}
