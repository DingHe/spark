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

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.MergedBlockMetaRequest;

/** 处理传入的 RPC 消息的方法，包括常规的 RPC 和流式 RPC。它还管理网络连接的状态和生命周期（例如，当通道激活或失效时）并处理 RPC 通信中的错误和异常
 * Handler for sendRPC() messages sent by {@link org.apache.spark.network.client.TransportClient}s.
 */
public abstract class RpcHandler {

  private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();
  private static final MergedBlockMetaReqHandler NOOP_MERGED_BLOCK_META_REQ_HANDLER =
    new NoopMergedBlockMetaReqHandler();

  /**
   * Receive a single RPC message. Any exception thrown while in this method will be sent back to
   * the client in string form as a standard RPC failure.
   *
   * Neither this method nor #receiveStream will be called in parallel for a single
   * TransportClient (i.e., channel).
   * 此方法用于处理接收单个 RPC 消息。message 是序列化的字节缓冲区，callback 是用于处理响应的回调函数，处理完成后会调用回调函数
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   */
  public abstract void receive(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback);

  /**
   * Receive a single RPC message which includes data that is to be received as a stream. Any
   * exception thrown while in this method will be sent back to the client in string form as a
   * standard RPC failure.
   *
   * Neither this method nor #receive will be called in parallel for a single TransportClient
   * (i.e., channel).
   *
   * An error while reading data from the stream
   * ({@link org.apache.spark.network.client.StreamCallback#onData(String, ByteBuffer)})
   * will fail the entire channel.  A failure in "post-processing" the stream in
   * {@link org.apache.spark.network.client.StreamCallback#onComplete(String)} will result in an
   * rpcFailure, but the channel will remain active.
   * 此方法用于接收包含流式数据的 RPC 消息。消息包含一个相对较小的头部部分，会完全缓存在内存中，而实际的数据会作为流式数据异步接收和处理，用于大数据传输，比如文件传输。它返回一个 StreamCallbackWithID，用于管理接收的数据流
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param messageHeader The serialized bytes of the header portion of the RPC.  This is in meant
   *                      to be relatively small, and will be buffered entirely in memory, to
   *                      facilitate how the streaming portion should be received.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   * @return a StreamCallback for handling the accompanying streaming data
   */
  public StreamCallbackWithID receiveStream(
      TransportClient client,
      ByteBuffer messageHeader,
      RpcResponseCallback callback) {
    throw new UnsupportedOperationException();
  }

  /** 此方法返回 StreamManager 实例，管理由 TransportClient 正在获取的流的状态。对于 Spark 中的文件流操作特别有用
   * Returns the StreamManager which contains the state about which streams are currently being
   * fetched by a TransportClient.
   */
  public abstract StreamManager getStreamManager();

  /**
   * Receives an RPC message that does not expect a reply. The default implementation will
   * call "{@link #receive(TransportClient, ByteBuffer, RpcResponseCallback)}" and log a warning if
   * any of the callback methods are called.
   * 这是简化版的 receive 方法，适用于不需要响应的单向 RPC。它会调用标准的 receive 方法，并传入默认的回调函数（ONE_WAY_CALLBACK）
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   */
  public void receive(TransportClient client, ByteBuffer message) {
    receive(client, message, ONE_WAY_CALLBACK);
  }

  public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler() {
    return NOOP_MERGED_BLOCK_META_REQ_HANDLER;
  }

  /** 当与给定客户端的通道激活（即网络连接成功建立）时调用此方法。可以重写此方法，以在通道激活时进行必要的操作
   * Invoked when the channel associated with the given client is active.
   */
  public void channelActive(TransportClient client) { }

  /** 当与给定客户端的通道失效（即网络连接关闭或中断）时调用此方法。可以在此方法中处理与客户端相关的资源清理工作
   * Invoked when the channel associated with the given client is inactive.
   * No further requests will come from this client.
   */
  public void channelInactive(TransportClient client) { }

  public void exceptionCaught(Throwable cause, TransportClient client) { }
  //仅仅是打印成功或者失败的日志
  private static class OneWayRpcCallback implements RpcResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(OneWayRpcCallback.class);

    @Override
    public void onSuccess(ByteBuffer response) {
      logger.warn("Response provided for one-way RPC.");
    }

    @Override
    public void onFailure(Throwable e) {
      logger.error("Error response provided for one-way RPC.", e);
    }

  }

  /**
   * Handler for {@link MergedBlockMetaRequest}.
   *
   * @since 3.2.0
   */
  public interface MergedBlockMetaReqHandler {

    /**
     * Receive a {@link MergedBlockMetaRequest}.
     *
     * @param client A channel client which enables the handler to make requests back to the sender
     *     of this RPC.
     * @param mergedBlockMetaRequest Request for merged block meta.
     * @param callback Callback which should be invoked exactly once upon success or failure.
     */
    void receiveMergeBlockMetaReq(
        TransportClient client,
        MergedBlockMetaRequest mergedBlockMetaRequest,
        MergedBlockMetaResponseCallback callback);
  }

  /**
   * A Noop implementation of {@link MergedBlockMetaReqHandler}. This Noop implementation is used
   * by all the RPC handlers which don't eventually delegate the {@link MergedBlockMetaRequest} to
   * ExternalBlockHandler in the network-shuffle module.
   *
   * @since 3.2.0
   */
  private static class NoopMergedBlockMetaReqHandler implements MergedBlockMetaReqHandler {

    @Override
    public void receiveMergeBlockMetaReq(TransportClient client,
      MergedBlockMetaRequest mergedBlockMetaRequest, MergedBlockMetaResponseCallback callback) {
      // do nothing
    }
  }
}
