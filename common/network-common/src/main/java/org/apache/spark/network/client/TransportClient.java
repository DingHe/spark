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

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.protocol.*;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * Client for fetching consecutive chunks of a pre-negotiated stream. This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 *
 * Note that while this client deals with the fetching of chunks from a stream (i.e., data plane),
 * the actual setup of the streams is done outside the scope of the transport layer. The convenience
 * method "sendRPC" is provided to enable control plane communication between the client and server
 * to perform this setup.
 *
 * For example, a typical workflow might be:
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * Construct an instance of TransportClient using {@link TransportClientFactory}. A single
 * TransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 *
 * NB: This class is used to make requests to the server, while {@link TransportResponseHandler} is
 * responsible for handling responses from the server.
 * 客户端类，用于高效地从远程服务器获取流数据或执行远程过程调用 (RPC)
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel; //表示与远程服务器建立的网络连接
  private final TransportResponseHandler handler; //负责处理服务器响应，管理请求和回调
  @Nullable private String clientId; //存储客户端的标识符。用于身份验证
  private volatile boolean timedOut;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
    this.timedOut = false;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }
  //返回当前通道的远程地址，即服务器的地址
  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  /**
   * Returns the ID used by the client to authenticate itself when authentication is enabled.
   *
   * @return The client ID, or null if authentication is disabled.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Sets the authenticated client ID. This is meant to be used by the authentication layer.
   *
   * Trying to set a different client ID after it's been set will result in an exception.
   */
  public void setClientId(String id) {
    Preconditions.checkState(clientId == null, "Client ID has already been set.");
    this.clientId = id;
  }

  /**
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   *
   * Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times, though
   * some streams may not support this.
   *
   * Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are guaranteed
   * to be returned in the same order that they were requested, assuming only a single
   * TransportClient is used to fetch the chunks.
   * 从远程服务器请求一个数据块。通过指定 streamId 和 chunkIndex 获取数据块。请求的响应会调用提供的 callback 回调。
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
   *                 be agreed upon by client and server beforehand.
   * @param chunkIndex 0-based index of the chunk to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   */
  public void fetchChunk(
      long streamId,
      int chunkIndex,
      ChunkReceivedCallback callback) {
    if (logger.isDebugEnabled()) {
      logger.debug("Sending fetch chunk request {} to {}", chunkIndex, getRemoteAddress(channel));
    }

    StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    StdChannelListener listener = new StdChannelListener(streamChunkId) {
      @Override
      void handleFailure(String errorMsg, Throwable cause) {
        handler.removeFetchRequest(streamChunkId);
        callback.onFailure(chunkIndex, new IOException(errorMsg, cause));
      }
    };
    handler.addFetchRequest(streamChunkId, callback);

    channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(listener);
  }

  /**
   * Request to stream the data with the given stream ID from the remote end.
   * 请求远程服务器开始数据流。此方法用于接收流数据，返回的数据将通过 callback 进行处理
   * @param streamId The stream to fetch.
   * @param callback Object to call with the stream data.
   */
  public void stream(String streamId, StreamCallback callback) {
    StdChannelListener listener = new StdChannelListener(streamId) {
      @Override
      void handleFailure(String errorMsg, Throwable cause) throws Exception {
        callback.onFailure(streamId, new IOException(errorMsg, cause));
      }
    };
    if (logger.isDebugEnabled()) {
      logger.debug("Sending stream request for {} to {}", streamId, getRemoteAddress(channel));
    }

    // Need to synchronize here so that the callback is added to the queue and the RPC is
    // written to the socket atomically, so that callbacks are called in the right order
    // when responses arrive.
    synchronized (this) {
      handler.addStreamCallback(streamId, callback);
      channel.writeAndFlush(new StreamRequest(streamId)).addListener(listener);
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
   * with the server's response or upon any failure.
   * 发送一个 RPC 请求到服务器。该请求的响应或失败会通过 callback 进行处理。返回请求的唯一标识符 requestId
   * @param message The message to send.
   * @param callback Callback to handle the RPC's reply.
   * @return The RPC's id.
   */
  public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", getRemoteAddress(channel));
    }

    long requestId = requestId();
    handler.addRpcRequest(requestId, callback);

    RpcChannelListener listener = new RpcChannelListener(requestId, callback);
    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
      .addListener(listener);

    return requestId;
  }

  /**
   * Sends a MergedBlockMetaRequest message to the server. The response of this message is
   * either a {@link MergedBlockMetaSuccess} or {@link RpcFailure}.
   *  发送合并块元数据请求到服务器，用于获取数据块合并的信息。返回的响应会调用 callback
   * @param appId applicationId.
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param callback callback the handle the reply.
   */
  public void sendMergedBlockMetaReq(
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId,
      MergedBlockMetaResponseCallback callback) {
    long requestId = requestId();
    if (logger.isTraceEnabled()) {
      logger.trace(
        "Sending RPC {} to fetch merged block meta to {}", requestId, getRemoteAddress(channel));
    }
    handler.addRpcRequest(requestId, callback);
    RpcChannelListener listener = new RpcChannelListener(requestId, callback);
    channel.writeAndFlush(
      new MergedBlockMetaRequest(requestId, appId, shuffleId, shuffleMergeId,
        reduceId)).addListener(listener);
  }

  /**
   * Send data to the remote end as a stream.  This differs from stream() in that this is a request
   * to *send* data to the remote end, not to receive it from the remote.
   * 向远程服务器上传一个数据流。包括元数据 meta 和实际数据 data，上传成功或失败时会调用 callback
   * @param meta meta data associated with the stream, which will be read completely on the
   *             receiving end before the stream itself.
   * @param data this will be streamed to the remote end to allow for transferring large amounts
   *             of data without reading into memory.
   * @param callback handles the reply -- onSuccess will only be called when both message and data
   *                 are received successfully.
   */
  public long uploadStream(
      ManagedBuffer meta,
      ManagedBuffer data,
      RpcResponseCallback callback) {
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", getRemoteAddress(channel));
    }

    long requestId = requestId();
    handler.addRpcRequest(requestId, callback);

    RpcChannelListener listener = new RpcChannelListener(requestId, callback);
    channel.writeAndFlush(new UploadStream(requestId, meta, data)).addListener(listener);

    return requestId;
  }

  /** 同步发送一个 RPC 请求，等待指定的超时时间获取响应。返回一个 ByteBuffer 作为响应
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
   * a specified timeout for a response.
   */
  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
    final SettableFuture<ByteBuffer> result = SettableFuture.create();

    sendRpc(message, new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          ByteBuffer copy = ByteBuffer.allocate(response.remaining());
          copy.put(response);
          // flip "copy" to make it readable
          copy.flip();
          result.set(copy);
        } catch (Throwable t) {
          logger.warn("Error in responding RPC callback", t);
          result.setException(t);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        result.setException(e);
      }
    });

    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /** 发送一个无响应的消息到服务器，不需要接收返回的消息
   * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
   * message, and no delivery guarantees are made.
   *
   * @param message The message to send.
   */
  public void send(ByteBuffer message) {
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
  }

  /**
   * Removes any state associated with the given RPC.
   * 移除与指定 requestId 相关的 RPC 请求
   * @param requestId The RPC id returned by {@link #sendRpc(ByteBuffer, RpcResponseCallback)}.
   */
  public void removeRpcRequest(long requestId) {
    handler.removeRpcRequest(requestId);
  }

  /** Mark this channel as having timed out. */
  public void timeOut() {
    this.timedOut = true;
  }

  @VisibleForTesting
  public TransportResponseHandler getHandler() {
    return handler;
  }

  @Override
  public void close() {
    // Mark the connection as timed out, so we do not return a connection that's being closed
    // from the TransportClientFactory if closing takes some time (e.g. with SSL)
    this.timedOut = true;
    // close should not take this long; use a timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("remoteAddress", channel.remoteAddress())
      .append("clientId", clientId)
      .append("isActive", isActive())
      .toString();
  }

  private static long requestId() {
    return Math.abs(UUID.randomUUID().getLeastSignificantBits());
  }
  //监听 channel 上的操作结果，处理成功或失败的情况。用于各种操作的回调处理
  private class StdChannelListener
      implements GenericFutureListener<Future<? super Void>> {
    final long startTime;
    final Object requestId;

    StdChannelListener(Object requestId) {
      this.startTime = System.currentTimeMillis();
      this.requestId = requestId;
    }

    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
      if (future.isSuccess()) {
        if (logger.isTraceEnabled()) {
          long timeTaken = System.currentTimeMillis() - startTime;
          logger.trace("Sending request {} to {} took {} ms", requestId,
              getRemoteAddress(channel), timeTaken);
        }
      } else {
        String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
            getRemoteAddress(channel), future.cause());
        logger.error(errorMsg, future.cause());
        channel.close();
        try {
          handleFailure(errorMsg, future.cause());
        } catch (Exception e) {
          logger.error("Uncaught exception in RPC response callback handler!", e);
        }
      }
    }

    void handleFailure(String errorMsg, Throwable cause) throws Exception {}
  }

  private class RpcChannelListener extends StdChannelListener {
    final long rpcRequestId;
    final BaseResponseCallback callback;

    RpcChannelListener(long rpcRequestId, BaseResponseCallback callback) {
      super("RPC " + rpcRequestId);
      this.rpcRequestId = rpcRequestId;
      this.callback = callback;
    }

    @Override
    void handleFailure(String errorMsg, Throwable cause) {
      handler.removeRpcRequest(rpcRequestId);
      callback.onFailure(new IOException(errorMsg, cause));
    }
  }

}
