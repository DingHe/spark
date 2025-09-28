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

package org.apache.spark.network.client;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * Callback for the result of a single
 * {@link org.apache.spark.network.protocol.MergedBlockMetaRequest}.
 *
 * @since 3.2.0
 */
// 核心作用是为 合并块元数据请求 (MergedBlockMetaRequest) 提供一个完整的、客户端回调机制的契约
  //回调专门用于处理 Push-Based Shuffle（推送式 Shuffle） 中，Reduce 任务向 Shuffle Server 请求已合并的 Shuffle 数据块的元数据时的响应
public interface MergedBlockMetaResponseCallback extends BaseResponseCallback {
  /**
   * Called upon receipt of a particular merged block meta.
   *
   * The given buffer will initially have a refcount of 1, but will be release()'d as soon as this
   * call returns. You must therefore either retain() the buffer or copy its contents before
   * returning.
   *
   * @param numChunks number of merged chunks in the merged block
   * @param buffer the buffer contains an array of roaring bitmaps. The i-th roaring bitmap
   *               contains the mapIds that were merged to the i-th merged chunk.
   */
  // 当成功从 Shuffle Server 接收到合并块的元数据时被调用。它提供了两个关键信息： 1. numChunks：这个合并块被逻辑上分割成了多少个分块 (Chunk)，后续客户端将按这个数量发起分块数据拉取。
  // 2. buffer：一个 ManagedBuffer，它包含了一系列 Roaring Bitmap。第 i 个 Roaring Bitmap 存储着被合并到第 i 个分块中的所有 Map 任务的 ID
  void onSuccess(int numChunks, ManagedBuffer buffer);
}
