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

package org.apache.spark.network;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.ChunkFetchRequestHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.NettyLogger;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a
 * {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 */
// Spark 网络通信模块中一个核心的门面（Facade）类，它封装了创建和配置 Spark 网络通信客户端和服务器所需的所有上下文和资源
// 主要作用是：
// 资源集中管理：负责持有和管理网络通信的核心配置 (TransportConf)、应用层的请求处理器 (RpcHandler)，以及专用于 Shuffle 数据块拉取（Chunk Fetch） 的线程池
// 工厂方法：提供工厂方法来创建 TransportServer（用于接收请求）和 TransportClientFactory（用于创建客户端连接）
// Netty 管道初始化：负责配置和初始化 Netty 的 ChannelPipeline。这个管道是网络数据流经的路径，它定义了数据的编码、解码、帧处理、心跳检测和最终的业务逻辑处理顺序
public class TransportContext implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  private static final NettyLogger nettyLogger = new NettyLogger();
  // 网络配置对象，包含了所有网络参数，如超时、线程数、I/O 模式等。
  private final TransportConf conf;
  // 业务逻辑处理程序。用于处理所有传入的 RPC 消息（例如 Shuffle 请求）
  private final RpcHandler rpcHandler;
  // 指示是否应关闭长时间空闲的连接（通过 IdleStateHandler 实现）
  private final boolean closeIdleConnections;
  // Number of registered connections to the shuffle service
  // 用于跟踪当前注册到 Shuffle Service 的连接数量
  private Counter registeredConnections = new Counter();

  /**
   * Force to create MessageEncoder and MessageDecoder so that we can make sure they will be created
   * before switching the current context class loader to ExecutorClassLoader.
   *
   * Netty's MessageToMessageEncoder uses Javassist to generate a matcher class and the
   * implementation calls "Class.forName" to check if this calls is already generated. If the
   * following two objects are created in "ExecutorClassLoader.findClass", it will cause
   * "ClassCircularityError". This is because loading this Netty generated class will call
   * "ExecutorClassLoader.findClass" to search this class, and "ExecutorClassLoader" will try to use
   * RPC to load it and cause to load the non-exist matcher class again. JVM will report
   * `ClassCircularityError` to prevent such infinite recursion. (See SPARK-17714)
   */
  // 静态且唯一的消息编码器实例。它将 Spark 协议消息编码为字节流，用于发送消息（服务器到客户端的响应，或客户端到服务器的请求）。被强制提前创建以避免类加载器问题。
  private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
  // 静态且唯一的消息解码器实例。它将字节流解码为 Spark 协议消息，用于接收消息。被强制提前创建以避免类加载器问题
  private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

  // Separate thread pool for handling ChunkFetchRequest. This helps to enable throttling
  // max number of TransportServer worker threads that are blocked on writing response
  // of ChunkFetchRequest message back to the client via the underlying channel.
  // 一个专门的 Netty 线程池，仅用于处理 Shuffle 的数据块拉取请求 (ChunkFetchRequest)。
  // 这样可以防止 I/O 密集型的数据拉取操作阻塞用于处理 RPC 请求的线程。如果配置不支持或非 Shuffle 模块，则为 null
  private final EventLoopGroup chunkFetchWorkers;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false, false);
  }

  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    this(conf, rpcHandler, closeIdleConnections, false);
  }

  /**
   * Enables TransportContext initialization for underlying client and server.
   *
   * @param conf TransportConf
   * @param rpcHandler RpcHandler responsible for handling requests and responses.
   * @param closeIdleConnections Close idle connections if it is set to true.
   * @param isClientOnly This config indicates the TransportContext is only used by a client.
   *                     This config is more important when external shuffle is enabled.
   *                     It stops creating extra event loop and subsequent thread pool
   *                     for shuffle clients to handle chunked fetch requests.
   */
  public TransportContext( //上面基础上添加了 isClientOnly 参数。如果设置为 true，则表示当前 TransportContext 仅用于客户端，不需要创建额外的线程池来处理“块获取请求”
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections,
      boolean isClientOnly) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.closeIdleConnections = closeIdleConnections;

    if (conf.getModuleName() != null &&
        conf.getModuleName().equalsIgnoreCase("shuffle") &&
        !isClientOnly && conf.separateChunkFetchRequest()) {
      chunkFetchWorkers = NettyUtils.createEventLoop(   //专门用于shuffle的块请求线程池
          IOMode.valueOf(conf.ioMode()),
          conf.chunkFetchHandlerThreads(),
          "shuffle-chunk-fetch-handler");
    } else {
      chunkFetchWorkers = null;
    }
  }

  /**
   * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
   * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
   * to create a Client.
   */
  //创建客户端工厂
  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(new ArrayList<>());
  }

  /** Create a server which will attempt to bind to a specific port. */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
  }

  /** Create a server which will attempt to bind to a specific host and port. */
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
  }

  /** Creates a new server, binding to any available ephemeral port. */
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  public TransportServer createServer() {
    return createServer(0, new ArrayList<>());
  }

  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   * 作用是为 SocketChannel 配置一个 Netty 管道，管道中包含了消息的编码、解码、空闲状态处理器、日志记录处理器以及处理请求的处理器
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      ChannelPipeline pipeline = channel.pipeline();
      if (nettyLogger.getLoggingHandler() != null) {
        pipeline.addLast("loggingHandler", nettyLogger.getLoggingHandler()); //添加日志处理器，用于记录 Netty 的网络操作日志
      }
      pipeline
        .addLast("encoder", ENCODER) //添加消息编码器，用于将传输的消息进行编码
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder()) //添加帧解码器，负责解码接收到的网络数据帧
        .addLast("decoder", DECODER) //添加消息解码器，将网络接收到的数据流解码成实际的消息
        .addLast("idleStateHandler", //添加空闲状态处理器，用于检测连接的空闲状态
          new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler); //最后添加消息处理器，它负责处理实际的请求和响应消息
      // Use a separate EventLoopGroup to handle ChunkFetchRequest messages for shuffle rpcs.
      if (chunkFetchWorkers != null) { //专门用于处理“块获取请求”的线程池。这个线程池使得处理数据块的请求不会和常规的请求处理混合，从而避免阻塞
        ChunkFetchRequestHandler chunkFetchHandler = new ChunkFetchRequestHandler(
          channelHandler.getClient(), rpcHandler.getStreamManager(),
          conf.maxChunksBeingTransferred(), true /* syncModeEnabled */);
        pipeline.addLast(chunkFetchWorkers, "chunkFetchHandler", chunkFetchHandler);
      }
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    boolean separateChunkFetchRequest = conf.separateChunkFetchRequest(); //是否专门的线程池处理块请求信息
    ChunkFetchRequestHandler chunkFetchRequestHandler = null;
    if (!separateChunkFetchRequest) {
      chunkFetchRequestHandler = new ChunkFetchRequestHandler(
        client, rpcHandler.getStreamManager(),
        conf.maxChunksBeingTransferred(), false /* syncModeEnabled */);
    }
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler, conf.maxChunksBeingTransferred(), chunkFetchRequestHandler);
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), separateChunkFetchRequest, closeIdleConnections, this);
  }

  public TransportConf getConf() { return conf; }

  public Counter getRegisteredConnections() {
    return registeredConnections;
  }

  @Override
  public void close() {
    if (chunkFetchWorkers != null) {
      chunkFetchWorkers.shutdownGracefully();
    }
  }
}
