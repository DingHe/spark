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

package org.apache.spark.network.server;

import io.netty.channel.Channel;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/** 主要职责是管理流的数据块（chunk），处理传输请求，并确保客户端在请求数据块时得到适当的响应。
 * The StreamManager is used to fetch individual chunks from a stream. This is used in
 * {@link TransportRequestHandler} in order to respond to fetchChunk() requests. Creation of the
 * stream is outside the scope of the transport layer, but a given stream is guaranteed to be read
 * by only one client connection, meaning that getChunk() for a particular stream will be called
 * serially and that once the connection associated with the stream is closed, that stream will
 * never be used again.
 */
public abstract class StreamManager {
  /**
   * Called in response to a fetchChunk() request. The returned buffer will be passed as-is to the
   * client. A single stream will be associated with a single TCP connection, so this method
   * will not be called in parallel for a particular stream.
   *
   * Chunks may be requested in any order, and requests may be repeated, but it is not required
   * that implementations support this behavior.
   * 根据给定的 streamId 和 chunkIndex 返回相应的数据块。每个流只能由一个客户端连接进行读取，因此该方法在特定流上不会并行调用
   * The returned ManagedBuffer will be release()'d after being written to the network.
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @param chunkIndex 0-indexed chunk of the stream that's requested
   */
  public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);

  /**
   * Called in response to a stream() request. The returned data is streamed to the client
   * through a single TCP connection.
   *
   * Note the <code>streamId</code> argument is not related to the similarly named argument in the
   * {@link #getChunk(long, int)} method.
   * 该方法在响应 stream() 请求时被调用。返回一个用于流式传输数据的 ManagedBuffer
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @return A managed buffer for the stream, or null if the stream was not found.
   */
  public ManagedBuffer openStream(String streamId) {
    throw new UnsupportedOperationException();
  }

  /** 该方法在 TCP 连接终止时被调用。它通知 StreamManager 不再从该连接的流中读取数据，因此可以进行清理工作
   * Indicates that the given channel has been terminated. After this occurs, we are guaranteed not
   * to read from the associated streams again, so any state can be cleaned up.
   */
  public void connectionTerminated(Channel channel) { }

  /**
   * Verify that the client is authorized to read from the given stream.
   * 该方法用于检查给定的客户端是否有权读取指定的流数据。如果客户端未获得授权，抛出 SecurityException
   * @throws SecurityException If client is not authorized.
   */
  public void checkAuthorization(TransportClient client, long streamId) { } //client：表示请求流数据的客户端

  /** 该方法返回当前正在传输且尚未完成的数据块数
   * Return the number of chunks being transferred and not finished yet in this StreamManager.
   */
  public long chunksBeingTransferred() {
    return 0;
  }

  /** 该方法在开始发送某个数据块时被调用，提供流的 streamId。它表示当前开始向客户端发送该流的一个块
   * Called when start sending a chunk.
   */
  public void chunkBeingSent(long streamId) { }

  /**
   * Called when start sending a stream.
   */
  public void streamBeingSent(String streamId) { }

  /** 它表示当前开始向客户端发送整个流
   * Called when a chunk is successfully sent.
   */
  public void chunkSent(long streamId) { }

  /**
   * Called when a stream is successfully sent.
   */
  public void streamSent(String streamId) { }

}
