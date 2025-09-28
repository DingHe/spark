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

/**
 * Handler for sendRPC() messages sent by {@link org.apache.spark.network.client.TransportClient}s.
 */
// Spark 服务器端（如 Shuffle Server、Block Manager 或 Driver）处理所有传入客户端请求的核心入口和抽象层
// RPC 消息分发：定义了处理普通 RPC 消息 (receive) 和流式 RPC 消息 (receiveStream) 的抽象方法，将网络层接收到的字节流转化为业务逻辑可以处理的消息
// 任何需要接收来自 TransportClient 请求的 Spark 组件（如 ExternalBlockHandler）都需要继承并实现 RpcHandler 的抽象方法
public abstract class RpcHandler {
  // 单向 RPC（不期望响应）的占位符回调。如果单向请求意外收到了响应或错误，它会记录警告/错误日志
  private static final RpcResponseCallback ONE_WAY_CALLBACK = new OneWayRpcCallback();
  // 静态的 No-Op (空操作) 合并块元数据请求处理器实例。这是默认实现，用于那些不支持 Push-Based Shuffle 协议的 RpcHandler 子类
  private static final MergedBlockMetaReqHandler NOOP_MERGED_BLOCK_META_REQ_HANDLER =
    new NoopMergedBlockMetaReqHandler();

  /**
   * Receive a single RPC message. Any exception thrown while in this method will be sent back to
   * the client in string form as a standard RPC failure.
   *
   * Neither this method nor #receiveStream will be called in parallel for a single
   * TransportClient (i.e., channel)
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param message The serialized bytes of the RPC.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   */
  // 用于处理客户端发来的单个常规 RPC 请求。请求的业务数据在 ByteBuffer message 中，服务器处理完成后必须调用 RpcResponseCallback callback 将结果或失败信息返回给客户端
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
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.
   * @param messageHeader The serialized bytes of the header portion of the RPC.  This is in meant
   *                      to be relatively small, and will be buffered entirely in memory, to
   *                      facilitate how the streaming portion should be received.
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   * @return a StreamCallback for handling the accompanying streaming data
   */
  // 用于处理包含流式数据的 RPC 请求。请求的头部信息在 ByteBuffer messageHeader 中（通常较小，完全缓存），而主体数据将作为流异步接收。
  // 默认实现是抛出 UnsupportedOperationException，意味着流式 RPC 默认不被支持
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
  //核心作用是作为单向 RPC（One-Way RPC） 请求的占位符（Placeholder）回调
  //单向 RPC 是指客户端发送请求给服务器后，不期望或不等待服务器返回任何响应数据（fire-and-forget）
  //OneWayRpcCallback 类的职责是：
  //忽略成功响应：如果服务器意外地返回了成功响应（onSuccess），它只会记录一个警告，并忽略返回的 ByteBuffer
  private static class OneWayRpcCallback implements RpcResponseCallback {r

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

  // 核心作用是为 Shuffle Server（例如 External Shuffle Service 或 Spark Executor）提供一个处理合并块元数据请求（MergedBlockMetaRequest） 的标准契约
  // 在 Push-Based Shuffle（推送式 Shuffle） 架构中：
  // 当 Reduce 任务 需要拉取 Shuffle 数据时，它首先会发送一个 MergedBlockMetaRequest 请求来获取已合并块的索引信息。
  // MergedBlockMetaReqHandler 的实现类（如 ExternalBlockHandler 的一部分）负责接收这个请求，并执行查找合并块元数据的实际逻辑。
  public interface MergedBlockMetaReqHandler {

    /**
     * Receive a {@link MergedBlockMetaRequest}.
     *
     * @param client A channel client which enables the handler to make requests back to the sender
     *     of this RPC.
     * @param mergedBlockMetaRequest Request for merged block meta.
     * @param callback Callback which should be invoked exactly once upon success or failure.
     */
    // 负责接收和处理来自客户端的 MergedBlockMetaRequest。该方法有三个参数：
    // 1. TransportClient client：这是一个网络客户端通道，允许处理程序在处理完请求后，将响应消息（MergedBlockMetaResponse）发送回请求的源头（即发送请求的 Reduce 任务）。
    // 2. MergedBlockMetaRequest mergedBlockMetaRequest：客户端发送的实际请求对象，包含了定位合并块元数据所需的所有信息（appId、shuffleId 等）。
    // 3. MergedBlockMetaResponseCallback callback：一个回调对象，用于通知客户端请求的结果。
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
