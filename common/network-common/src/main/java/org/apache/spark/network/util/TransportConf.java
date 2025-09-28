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

package org.apache.spark.network.util;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;
import io.netty.util.NettyRuntime;

/**
 * A central location that tracks all the settings we expose to users.
 */
//  Spark 网络通信子系统（Netty 框架） 的核心配置管理类
//  集中管理网络配置：作为一个中心化的位置，它封装了所有暴露给用户的、用于配置 Spark 网络传输层行为的设置
// 模块化配置加载：它允许通过传入一个模块名称（module，例如 "shuffle" 或 "rpc"），动态地构建出完整的 Spark 配置键，
// 从而实现配置隔离和复用。例如，传入 "shuffle" 模块，它会将 "io.mode" 转换为配置键 spark.shuffle.io.mode
public class TransportConf {
  //一系列私有 final 字符串，存储了完整的 Spark 配置键（例如 spark.shuffle.io.mode）。这些键是在构造函数中通过 getConfKey 方法，结合 module 和配置后缀动态生成的
  private final String SPARK_NETWORK_IO_MODE_KEY;
  private final String SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY;
  private final String SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY;
  private final String SPARK_NETWORK_IO_CONNECTIONCREATIONTIMEOUT_KEY;
  private final String SPARK_NETWORK_IO_BACKLOG_KEY;
  private final String SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY;
  private final String SPARK_NETWORK_IO_SERVERTHREADS_KEY;
  private final String SPARK_NETWORK_IO_CLIENTTHREADS_KEY;
  private final String SPARK_NETWORK_IO_RECEIVEBUFFER_KEY;
  private final String SPARK_NETWORK_IO_SENDBUFFER_KEY;
  private final String SPARK_NETWORK_SASL_TIMEOUT_KEY;
  private final String SPARK_NETWORK_IO_MAXRETRIES_KEY;
  private final String SPARK_NETWORK_IO_RETRYWAIT_KEY;
  private final String SPARK_NETWORK_IO_LAZYFD_KEY;
  private final String SPARK_NETWORK_VERBOSE_METRICS;
  private final String SPARK_NETWORK_IO_ENABLETCPKEEPALIVE_KEY;
  //用于从底层配置源（如 Hadoop Configuration 或 Spark SparkConf）获取原始配置值。TransportConf 依赖这个对象来查找所有配置项
  private final ConfigProvider conf;
  //标识此配置对象所针对的 Spark 子系统（例如 "shuffle"、"blockmanager"、"rpc"）。它用于构造具体的配置键
  private final String module;

  public TransportConf(String module, ConfigProvider conf) {
    this.module = module;
    this.conf = conf;
    SPARK_NETWORK_IO_MODE_KEY = getConfKey("io.mode");
    SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY = getConfKey("io.preferDirectBufs");
    SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY = getConfKey("io.connectionTimeout");
    SPARK_NETWORK_IO_CONNECTIONCREATIONTIMEOUT_KEY = getConfKey("io.connectionCreationTimeout");
    SPARK_NETWORK_IO_BACKLOG_KEY = getConfKey("io.backLog");
    SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY =  getConfKey("io.numConnectionsPerPeer");
    SPARK_NETWORK_IO_SERVERTHREADS_KEY = getConfKey("io.serverThreads");
    SPARK_NETWORK_IO_CLIENTTHREADS_KEY = getConfKey("io.clientThreads");
    SPARK_NETWORK_IO_RECEIVEBUFFER_KEY = getConfKey("io.receiveBuffer");
    SPARK_NETWORK_IO_SENDBUFFER_KEY = getConfKey("io.sendBuffer");
    SPARK_NETWORK_SASL_TIMEOUT_KEY = getConfKey("sasl.timeout");
    SPARK_NETWORK_IO_MAXRETRIES_KEY = getConfKey("io.maxRetries");
    SPARK_NETWORK_IO_RETRYWAIT_KEY = getConfKey("io.retryWait");
    SPARK_NETWORK_IO_LAZYFD_KEY = getConfKey("io.lazyFD");
    SPARK_NETWORK_VERBOSE_METRICS = getConfKey("io.enableVerboseMetrics");
    SPARK_NETWORK_IO_ENABLETCPKEEPALIVE_KEY = getConfKey("io.enableTcpKeepAlive");
  }
  //委托给底层的 ConfigProvider，根据完整的配置键 name 获取整数值，如果未找到则返回 defaultValue
  public int getInt(String name, int defaultValue) {
    return conf.getInt(name, defaultValue);
  }
  //委托给底层的 ConfigProvider，根据完整的配置键 name 获取字符串值，如果未找到则返回 defaultValue
  public String get(String name, String defaultValue) {
    return conf.get(name, defaultValue);
  }
  //动态获取配置项
  private String getConfKey(String suffix) {
    return "spark." + module + "." + suffix;
  }

  public String getModuleName() {
    return module;
  }
  //返回网络 I/O 模式（NIO 或 EPOLL）。默认为 NIO
  /** IO mode: nio or epoll */
  public String ioMode() {
    return conf.get(SPARK_NETWORK_IO_MODE_KEY, "NIO").toUpperCase(Locale.ROOT);
  }

  /** If true, we will prefer allocating off-heap byte buffers within Netty. */
  //是否偏好使用堆外（Off-Heap） 的直接 ByteBuffer。默认为 true
  public boolean preferDirectBufs() {
    return conf.getBoolean(SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY, true);
  }

  /** Connection idle timeout in milliseconds. Default 120 secs. */
  //返回连接空闲超时时间（毫秒）。默认值基于 spark.network.timeout（默认 120 秒）
  public int connectionTimeoutMs() {
    long defaultNetworkTimeoutS = JavaUtils.timeStringAsSec(
      conf.get("spark.network.timeout", "120s"));
    long defaultTimeoutMs = JavaUtils.timeStringAsSec(
      conf.get(SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY, defaultNetworkTimeoutS + "s")) * 1000;
    return defaultTimeoutMs < 0 ? 0 : (int) defaultTimeoutMs;
  }

  /** Connect creation timeout in milliseconds. Default 120 secs. */
  //返回建立连接的超时时间（毫秒）。默认值与 connectionTimeoutMs() 相同
  public int connectionCreationTimeoutMs() {
    long connectionTimeoutS = TimeUnit.MILLISECONDS.toSeconds(connectionTimeoutMs());
    long defaultTimeoutMs = JavaUtils.timeStringAsSec(
      conf.get(SPARK_NETWORK_IO_CONNECTIONCREATIONTIMEOUT_KEY,  connectionTimeoutS + "s")) * 1000;
    return defaultTimeoutMs < 0 ? 0 : (int) defaultTimeoutMs;
  }

  /** Number of concurrent connections between two nodes for fetching data. */
  //返回两个节点之间用于数据拉取（Fetching）的并发连接数。默认为 1
  public int numConnectionsPerPeer() {
    return conf.getInt(SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY, 1);
  }

  /**
   * Requested maximum length of the queue of incoming connections. If  &lt; 1,
   * the default Netty value of {@link io.netty.util.NetUtil#SOMAXCONN} will be used.
   * Default to -1.
   */
  //返回服务器端请求的最大等待连接队列长度。小于 1 时使用 Netty 默认值。默认为 -1
  public int backLog() { return conf.getInt(SPARK_NETWORK_IO_BACKLOG_KEY, -1); }

  /** Number of threads used in the server thread pool. Default to 0, which is 2x#cores. */
  //返回服务器线程池中使用的线程数。默认为 0，表示使用 2 * #CPU 核心数
  public int serverThreads() { return conf.getInt(SPARK_NETWORK_IO_SERVERTHREADS_KEY, 0); }

  /** Number of threads used in the client thread pool. Default to 0, which is 2x#cores. */
  //返回客户端线程池中使用的线程数。默认为 0，表示使用 2 * #CPU 核心数
  public int clientThreads() { return conf.getInt(SPARK_NETWORK_IO_CLIENTTHREADS_KEY, 0); }

  /**
   * Receive buffer size (SO_RCVBUF).
   * Note: the optimal size for receive buffer and send buffer should be
   *  latency * network_bandwidth.
   * Assuming latency = 1ms, network_bandwidth = 10Gbps
   *  buffer size should be ~ 1.25MB
   */
  //返回 TCP 接收缓冲区大小 (SO_RCVBUF)。默认为 -1（使用系统默认）
  public int receiveBuf() { return conf.getInt(SPARK_NETWORK_IO_RECEIVEBUFFER_KEY, -1); }

  /** Send buffer size (SO_SNDBUF). */
  //返回 TCP 发送缓冲区大小 (SO_SNDBUF)。默认为 -1（使用系统默认）
  public int sendBuf() { return conf.getInt(SPARK_NETWORK_IO_SENDBUFFER_KEY, -1); }

  /** Timeout for a single round trip of auth message exchange, in milliseconds. */
  //返回单次认证消息交换的往返超时时间（毫秒）。默认是 30 秒
  public int authRTTimeoutMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get("spark.network.auth.rpcTimeout",
      conf.get(SPARK_NETWORK_SASL_TIMEOUT_KEY, "30s"))) * 1000;
  }

  /**
   * Max number of times we will try IO exceptions (such as connection timeouts) per request.
   * If set to 0, we will not do any retries.
   */
  //返回针对 I/O 异常（如连接超时）的最大重试次数。默认为 3 次
  public int maxIORetries() { return conf.getInt(SPARK_NETWORK_IO_MAXRETRIES_KEY, 3); }

  /**
   * Time (in milliseconds) that we will wait in order to perform a retry after an IOException.
   * Only relevant if maxIORetries &gt; 0.
   */
  //返回 I/O 异常重试之间的等待时间（毫秒）。默认为 5 秒。
  public int ioRetryWaitTimeMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get(SPARK_NETWORK_IO_RETRYWAIT_KEY, "5s")) * 1000;
  }

  /**
   * Minimum size of a block that we should start using memory map rather than reading in through
   * normal IO operations. This prevents Spark from memory mapping very small blocks. In general,
   * memory mapping has high overhead for blocks close to or below the page size of the OS.
   */
  //使用内存映射（mmap）读取文件而不是标准 I/O 的最小块大小。防止对小块进行内存映射带来的开销。默认为 2MB。
  public int memoryMapBytes() {
    return Ints.checkedCast(JavaUtils.byteStringAsBytes(
      conf.get("spark.storage.memoryMapThreshold", "2m")));
  }

  /**
   * Whether to initialize FileDescriptor lazily or not. If true, file descriptors are
   * created only when data is going to be transferred. This can reduce the number of open files.
   */
  //是否仅在实际需要传输数据时才创建文件描述符，以减少打开的文件句柄数量。默认为 true。
  public boolean lazyFileDescriptor() {
    return conf.getBoolean(SPARK_NETWORK_IO_LAZYFD_KEY, true);
  }

  /**
   * Whether to track Netty memory detailed metrics. If true, the detailed metrics of Netty
   * PoolByteBufAllocator will be gotten, otherwise only general memory usage will be tracked.
   */
  //是否跟踪 Netty PoolByteBufAllocator 的详细内存指标。默认为 false
  public boolean verboseMetrics() {
    return conf.getBoolean(SPARK_NETWORK_VERBOSE_METRICS, false);
  }

  /**
   * Whether to enable TCP keep-alive. If true, the TCP keep-alives are enabled, which removes
   * connections that are idle for too long.
   */
  public boolean enableTcpKeepAlive() {
    return conf.getBoolean(SPARK_NETWORK_IO_ENABLETCPKEEPALIVE_KEY, false);
  }

  /**
   * Maximum number of retries when binding to a port before giving up.
   */
  public int portMaxRetries() {
    return conf.getInt("spark.port.maxRetries", 16);
  }

  /**
   * Enables strong encryption. Also enables the new auth protocol, used to negotiate keys.
   */
  //是否启用网络层（非 SASL）的强加密。默认为 false
  public boolean encryptionEnabled() {
    return conf.getBoolean("spark.network.crypto.enabled", false);
  }

  /**
   * Version number to be used by the AuthEngine key agreement protocol. Valid values are 1 or 2.
   * The default version is 1 for backward compatibility. Version 2 is recommended for stronger
   * security properties.
   */
  //加密认证协议的版本号（1 或 2）。默认为 1，版本 2 提供更强的安全性。
  public int authEngineVersion() {
    return conf.getInt("spark.network.crypto.authEngineVersion", 1);
  }

  /**
   * The cipher transformation to use for encrypting session data.
   */
  public String cipherTransformation() {
    return conf.get("spark.network.crypto.cipher", "AES/CTR/NoPadding");
  }

  /**
   * Whether to fall back to SASL if the new auth protocol fails. Enabled by default for
   * backwards compatibility.
   */
  //如果新的认证协议失败，是否降级到传统的 SASL 认证。默认为 true
  public boolean saslFallback() {
    return conf.getBoolean("spark.network.crypto.saslFallback", true);
  }

  /**
   * Whether to enable SASL-based encryption when authenticating using SASL.
   */
  //在使用 SASL 认证时，是否同时启用 SASL 加密。默认为 false
  public boolean saslEncryption() {
    return conf.getBoolean("spark.authenticate.enableSaslEncryption", false);
  }

  /**
   * Maximum number of bytes to be encrypted at a time when SASL encryption is used.
   */
  //使用 SASL 加密时，一次加密的最大字节数。默认为 64KB
  public int maxSaslEncryptedBlockSize() {
    return Ints.checkedCast(JavaUtils.byteStringAsBytes(
      conf.get("spark.network.sasl.maxEncryptedBlockSize", "64k")));
  }

  /**
   * Whether the server should enforce encryption on SASL-authenticated connections.
   */
  public boolean saslServerAlwaysEncrypt() {
    return conf.getBoolean("spark.network.sasl.serverAlwaysEncrypt", false);
  }

  /**
   * Flag indicating whether to share the pooled ByteBuf allocators between the different Netty
   * channels. If enabled then only two pooled ByteBuf allocators are created: one where caching
   * is allowed (for transport servers) and one where not (for transport clients).
   * When disabled a new allocator is created for each transport servers and clients.
   */
  //是否在不同的 Netty 通道之间共享池化的 ByteBuf 分配器。默认为 true
  public boolean sharedByteBufAllocators() {
    return conf.getBoolean("spark.network.sharedByteBufAllocators.enabled", true);
  }

  /**
  * If enabled then off-heap byte buffers will be preferred for the shared ByteBuf allocators.
  */
  public boolean preferDirectBufsForSharedByteBufAllocators() {
    return conf.getBoolean("spark.network.io.preferDirectBufs", true);
  }

  /**
   * The commons-crypto configuration for the module.
   */
  //返回用于配置 Commons-Crypto 库的属性集合
  public Properties cryptoConf() {
    return CryptoUtils.toCryptoConf("spark.network.crypto.config.", conf.getAll());
  }

  /**
   * The max number of chunks allowed to be transferred at the same time on shuffle service.
   * Note that new incoming connections will be closed when the max number is hit. The client will
   * retry according to the shuffle retry configs (see `spark.shuffle.io.maxRetries` and
   * `spark.shuffle.io.retryWait`), if those limits are reached the task will fail with fetch
   * failure.
   */
  public long maxChunksBeingTransferred() {
    return conf.getLong("spark.shuffle.maxChunksBeingTransferred", Long.MAX_VALUE);
  }

  /**
   * Percentage of io.serverThreads used by netty to process ChunkFetchRequest.
   * When the config `spark.shuffle.server.chunkFetchHandlerThreadsPercent` is set,
   * shuffle server will use a separate EventLoopGroup to process ChunkFetchRequest messages.
   * Although when calling the async writeAndFlush on the underlying channel to send
   * response back to client, the I/O on the channel is still being handled by
   * {@link org.apache.spark.network.server.TransportServer}'s default EventLoopGroup
   * that's registered with the Channel, by waiting inside the ChunkFetchRequest handler
   * threads for the completion of sending back responses, we are able to put a limit on
   * the max number of threads from TransportServer's default EventLoopGroup that are
   * going to be consumed by writing response to ChunkFetchRequest, which are I/O intensive
   * and could take long time to process due to disk contentions. By configuring a slightly
   * higher number of shuffler server threads, we are able to reserve some threads for
   * handling other RPC messages, thus making the Client less likely to experience timeout
   * when sending RPC messages to the shuffle server. The number of threads used for handling
   * chunked fetch requests are percentage of io.serverThreads (if defined) else it is a percentage
   * of 2 * #cores. However, a percentage of 0 means netty default number of threads which
   * is 2 * #cores ignoring io.serverThreads. The percentage here is configured via
   * spark.shuffle.server.chunkFetchHandlerThreadsPercent. The returned value is rounded off to
   * ceiling of the nearest integer.
   */
  public int chunkFetchHandlerThreads() {
    if (!this.getModuleName().equalsIgnoreCase("shuffle")) {
      return 0;
    }
    int chunkFetchHandlerThreadsPercent =
      Integer.parseInt(conf.get("spark.shuffle.server.chunkFetchHandlerThreadsPercent"));
    int threads =
      this.serverThreads() > 0 ? this.serverThreads() : 2 * NettyRuntime.availableProcessors();
    return (int) Math.ceil(threads * (chunkFetchHandlerThreadsPercent / 100.0));
  }

  /**
   * Whether to use a separate EventLoopGroup to process ChunkFetchRequest messages, it is decided
   * by the config `spark.shuffle.server.chunkFetchHandlerThreadsPercent` is set or not.
   */
  public boolean separateChunkFetchRequest() {
    return conf.getInt("spark.shuffle.server.chunkFetchHandlerThreadsPercent", 0) > 0;
  }

  /**
   * Whether to use the old protocol while doing the shuffle block fetching.
   * It is only enabled while we need the compatibility in the scenario of new spark version
   * job fetching blocks from old version external shuffle service.
   */
  public boolean useOldFetchProtocol() {
    return conf.getBoolean("spark.shuffle.useOldFetchProtocol", false);
  }

  /** Whether to enable sasl retries or not. The number of retries is dictated by the config
   * `spark.shuffle.io.maxRetries`.
   */
  public boolean enableSaslRetries() {
    return conf.getBoolean("spark.shuffle.sasl.enableRetries", false);
  }

  /**
   * Class name of the implementation of MergedShuffleFileManager that merges the blocks
   * pushed to it when push-based shuffle is enabled. By default, push-based shuffle is disabled at
   * a cluster level because this configuration is set to
   * 'org.apache.spark.network.shuffle.NoOpMergedShuffleFileManager'.
   * To turn on push-based shuffle at a cluster level, set the configuration to
   * 'org.apache.spark.network.shuffle.RemoteBlockPushResolver'.
   */
  public String mergedShuffleFileManagerImpl() {
    return conf.get("spark.shuffle.push.server.mergedShuffleFileManagerImpl",
      "org.apache.spark.network.shuffle.NoOpMergedShuffleFileManager");
  }

  /**
   * The minimum size of a chunk when dividing a merged shuffle file into multiple chunks during
   * push-based shuffle.
   * A merged shuffle file consists of multiple small shuffle blocks. Fetching the complete
   * merged shuffle file in a single disk I/O increases the memory requirements for both the
   * clients and the external shuffle service. Instead, the external shuffle service serves
   * the merged file in MB-sized chunks. This configuration controls how big a chunk can get.
   * A corresponding index file for each merged shuffle file will be generated indicating chunk
   * boundaries.
   *
   * Setting this too high would increase the memory requirements on both the clients and the
   * external shuffle service.
   *
   * Setting this too low would increase the overall number of RPC requests to external shuffle
   * service unnecessarily.
   */
  public int minChunkSizeInMergedShuffleFile() {
    return Ints.checkedCast(JavaUtils.byteStringAsBytes(
      conf.get("spark.shuffle.push.server.minChunkSizeInMergedShuffleFile", "2m")));
  }

  /**
   * The maximum size of cache in memory which is used in push-based shuffle for storing merged
   * index files. This cache is in addition to the one configured via
   * spark.shuffle.service.index.cache.size.
   */
  public long mergedIndexCacheSize() {
    return JavaUtils.byteStringAsBytes(
      conf.get("spark.shuffle.push.server.mergedIndexCacheSize", "100m"));
  }

  /**
   * The threshold for number of IOExceptions while merging shuffle blocks to a shuffle partition.
   * When the number of IOExceptions while writing to merged shuffle data/index/meta file exceed
   * this threshold then the shuffle server will respond back to client to stop pushing shuffle
   * blocks for this shuffle partition.
   */
  public int ioExceptionsThresholdDuringMerge() {
    return conf.getInt("spark.shuffle.push.server.ioExceptionsThresholdDuringMerge", 4);
  }

  /**
   * The RemoteBlockPushResolver#mergedShuffleCleanermergedShuffleCleaner
   * shutdown timeout, in seconds.
   */
  public long mergedShuffleCleanerShutdownTimeout() {
    return JavaUtils.timeStringAsSec(
      conf.get("spark.shuffle.push.server.mergedShuffleCleaner.shutdown.timeout", "60s"));
  }
}
