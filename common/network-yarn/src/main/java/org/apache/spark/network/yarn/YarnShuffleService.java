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

package org.apache.spark.network.yarn;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.*;
import org.apache.spark.network.shuffle.Constants;
import org.apache.spark.network.shuffle.MergedShuffleFileManager;
import org.apache.spark.network.shuffle.NoOpMergedShuffleFileManager;
import org.apache.spark.network.shuffledb.DB;
import org.apache.spark.network.shuffledb.DBBackend;
import org.apache.spark.network.shuffledb.DBIterator;
import org.apache.spark.network.shuffledb.StoreVersion;
import org.apache.spark.network.util.DBProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.crypto.AuthServerBootstrap;
import org.apache.spark.network.sasl.ShuffleSecretManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.AppsWithRecoveryDisabled;
import org.apache.spark.network.shuffle.ExternalBlockHandler;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.yarn.util.HadoopConfigProvider;

/**
 * An external shuffle service used by Spark on Yarn.
 *
 * This is intended to be a long-running auxiliary service that runs in the NodeManager process.
 * A Spark application may connect to this service by setting `spark.shuffle.service.enabled`.
 * The application also automatically derives the service port through `spark.shuffle.service.port`
 * specified in the Yarn configuration. This is so that both the clients and the server agree on
 * the same port to communicate on.
 *
 * The service also optionally supports authentication. This ensures that executors from one
 * application cannot read the shuffle files written by those from another. This feature can be
 * enabled by setting `spark.authenticate` in the Yarn configuration before starting the NM.
 * Note that the Spark application must also set `spark.authenticate` manually and, unlike in
 * the case of the service port, will not inherit this setting from the Yarn configuration. This
 * is because an application running on the same Yarn cluster may choose to not use the external
 * shuffle service, in which case its setting of `spark.authenticate` should be independent of
 * the service's.
 *
 * The shuffle service will produce metrics via the YARN NodeManager's {@code metrics2} system
 * under a namespace specified by the {@value SPARK_SHUFFLE_SERVICE_METRICS_NAMESPACE_KEY} config.
 *
 * By default, all configurations for the shuffle service will be taken directly from the
 * Hadoop {@link Configuration} passed by the YARN NodeManager. It is also possible to configure
 * the shuffle service by placing a resource named
 * {@value SHUFFLE_SERVICE_CONF_OVERLAY_RESOURCE_NAME} into the classpath, which should be an
 * XML file in the standard Hadoop Configuration resource format. Note that when the shuffle
 * service is loaded in the default manner, without configuring
 * {@code yarn.nodemanager.aux-services.<service>.classpath}, this file must be on the classpath
 * of the NodeManager itself. When using the {@code classpath} configuration, it can be present
 * either on the NodeManager's classpath, or specified in the classpath configuration.
 * This {@code classpath} configuration is only supported on YARN versions >= 2.9.0.
 */

// Spark 在 YARN 集群管理器上实现 External Shuffle Service（外部 Shuffle 服务） 的核心
// 作为 YARN 辅助服务运行：它继承自 Hadoop 的 AuxiliaryService，因此可以在每个 YARN NodeManager (NM) 进程中以一个长期运行的、独立的服务身份运行
// 解耦 Shuffle I/O：它将 Shuffle 数据的读写操作从 Spark 执行器（Executor） 进程中分离出来。
// 这意味着即使 Spark Executor 退出（例如由于应用程序完成或动态资源分配释放了执行器），Shuffle 数据仍然安全地存储在 NodeManager 的磁盘上，并可通过该服务访问
// 提供数据传输：它通过内部的 TransportServer 监听一个端口（默认为 7337），响应其他执行器或 Reduce 任务发来的 Shuffle 数据拉取请求（Fetch Requests）
// 支持认证和恢复：它能够配置 SASL 认证来保护不同应用程序之间 Shuffle 数据的隔离性，并支持在 NodeManager 重启后恢复已注册的执行器状态和安全密钥
public class YarnShuffleService extends AuxiliaryService {
  private static final Logger defaultLogger = LoggerFactory.getLogger(YarnShuffleService.class);
  private Logger logger = defaultLogger;

  // Port on which the shuffle server listens for fetch requests
  //Shuffle Server 监听请求的端口的配置键（spark.shuffle.service.port）
  private static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port";
  //默认端口号，值为 7337
  private static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

  /**
   * The namespace to use for the metrics record which will contain all metrics produced by the
   * shuffle service.
   */
  static final String SPARK_SHUFFLE_SERVICE_METRICS_NAMESPACE_KEY =
      "spark.yarn.shuffle.service.metrics.namespace";
  private static final String DEFAULT_SPARK_SHUFFLE_SERVICE_METRICS_NAME = "sparkShuffleService";

  /**
   * The namespace to use for the logs produced by the shuffle service
   */
  static final String SPARK_SHUFFLE_SERVICE_LOGS_NAMESPACE_KEY =
      "spark.yarn.shuffle.service.logs.namespace";

  // Whether the shuffle server should authenticate fetch requests
  //Shuffle Fetch 请求是否需要认证的配置键（spark.authenticate）
  private static final String SPARK_AUTHENTICATE_KEY = "spark.authenticate";
  //默认不需要认证
  private static final boolean DEFAULT_SPARK_AUTHENTICATE = false;
  //存储已注册执行器状态的恢复文件名（registeredExecutors）
  private static final String RECOVERY_FILE_NAME = "registeredExecutors";
  //存储应用程序密钥的恢复文件名（sparkShuffleRecovery）
  private static final String SECRETS_RECOVERY_FILE_NAME = "sparkShuffleRecovery";
  //存储 Push-Based Shuffle 合并状态的恢复文件名（sparkShuffleMergeRecovery）
  @VisibleForTesting
  static final String SPARK_SHUFFLE_MERGE_RECOVERY_FILE_NAME = "sparkShuffleMergeRecovery";

  // Whether failure during service initialization should stop the NM.
  //当external suffle service失败时，nm是否要退出
  @VisibleForTesting
  static final String STOP_ON_FAILURE_KEY = "spark.yarn.shuffle.stopOnFailure";

  @VisibleForTesting
  static final String INTEGRATION_TESTING = "spark.yarn.shuffle.testing";

  private static final boolean DEFAULT_STOP_ON_FAILURE = false;

  @VisibleForTesting
  static final String SPARK_SHUFFLE_SERVER_RECOVERY_DISABLED =
      "spark.yarn.shuffle.server.recovery.disabled";
  @VisibleForTesting
  static final String SECRET_KEY = "secret";

  // just for testing when you want to find an open port
  //监听端口
  @VisibleForTesting
  static int boundPort = -1;
  //用于序列化和反序列化元数据和密钥的 Jackson ObjectMapper 实例
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String APP_CREDS_KEY_PREFIX = "AppCreds";
  //用于存储恢复状态的数据库的版本号
  private static final StoreVersion CURRENT_VERSION = new StoreVersion(1, 0);

  /**
   * The name of the resource to search for on the classpath to find a shuffle service-specific
   * configuration overlay. If found, this will be parsed as a standard Hadoop
   * {@link Configuration config} file and will override the configs passed from the NodeManager.
   */
  //用于加载额外配置覆盖文件的资源名称
  static final String SHUFFLE_SERVICE_CONF_OVERLAY_RESOURCE_NAME = "spark-shuffle-site.xml";

  // just for integration tests that want to look at this file -- in general not sensible as
  // a static
  @VisibleForTesting
  static YarnShuffleService instance;

  // An entity that manages the shuffle secret per application
  // This is used only if authentication is enabled
  //仅在启用认证时使用，负责为每个应用程序生成、存储和管理用于 Shuffle Fetch 请求的密钥（Secret）
  @VisibleForTesting
  ShuffleSecretManager secretManager;
  //实际的网络服务器，负责监听端口并处理来自客户端的连接和请求
  // The actual server that serves shuffle files
  private TransportServer shuffleServer = null;
  //封装了 Shuffle 服务网络层的配置、认证器和 RPC 处理程序
  private TransportContext transportContext = null;
  //配置信息
  @VisibleForTesting
  Configuration _conf = null;

  // The recovery path used to shuffle service recovery
  //NodeManager 提供的、用于存储服务可恢复状态的本地路径
  @VisibleForTesting
  Path _recoveryPath = null;

  // Handles registering executors and opening shuffle blocks
  //核心业务逻辑处理器，负责处理 Shuffle Fetch 请求，管理执行器注册信息，以及查找和打开本地 Shuffle 数据块
  @VisibleForTesting
  ExternalBlockHandler blockHandler;

  // Handles merged shuffle registration, push blocks and finalization
  //负责处理 Push-Based Shuffle 中的合并块注册、推送和最终化操作。如果未启用 Push-Based Shuffle，则为 NoOpMergedShuffleFileManager
  @VisibleForTesting
  MergedShuffleFileManager shuffleMergeManager;

  // Where to store & reload executor info for recovering state after an NM restart
  @VisibleForTesting
  File registeredExecutorFile;

  // Where to store & reload application secrets for recovering state after an NM restart
  @VisibleForTesting
  File secretsFile;

  // Where to store & reload merge manager info for recovering state after an NM restart
  @VisibleForTesting
  File mergeManagerFile;
  //用于持久化存储应用程序密钥和状态（例如 LevelDB），以便在 NodeManager 重启后恢复状态。
  private DB db;
  //后端存储数据库的枚举值，leveldb 还是 rocksdb
  private DBBackend dbBackend = null;

  public YarnShuffleService() {
    // The name of the auxiliary service configured within the NodeManager
    // (`yarn.nodemanager.aux-services`) is treated as the source-of-truth, so this one can be
    // arbitrary. The NodeManager will log a warning if the configured name doesn't match this name,
    // to inform operators of a potential misconfiguration, but this name is otherwise not used.
    // It is hard-coded instead of using the value of the `spark.shuffle.service.name` configuration
    // because at this point in instantiation there is no Configuration object; it is not passed
    // until `serviceInit` is called, at which point it's too late to adjust the name.
    super("spark_shuffle");
    logger.info("Initializing YARN shuffle service for Spark");
    instance = this;
  }

  /**
   * Return whether authentication is enabled as specified by the configuration.
   * If so, fetch requests will fail unless the appropriate authentication secret
   * for the application is provided.
   */
  private boolean isAuthenticationEnabled() {
    return secretManager != null;
  }

  /**
   * Start the shuffle server with the given configuration.
   */
  //Spark 外部 Shuffle 服务在 YARN NodeManager 进程中执行初始化和启动网络服务的核心逻辑
  @Override
  protected void serviceInit(Configuration externalConf) throws Exception {
    //使用 NodeManager 传入的 Hadoop 配置 (externalConf) 创建一个副本，存储在内部变量 _conf 中。这是后续所有配置的基础
    _conf = new Configuration(externalConf);
    URL confOverlayUrl = Thread.currentThread().getContextClassLoader()
        .getResource(SHUFFLE_SERVICE_CONF_OVERLAY_RESOURCE_NAME);
    if (confOverlayUrl != null) {
      logger.info("Initializing Spark YARN shuffle service with configuration overlay from {}",
          confOverlayUrl);
      //表明将使用该文件中的配置来覆盖默认或 YARN 传入的配置
      _conf.addResource(confOverlayUrl);
    }
    //如果配置了自定义命名空间，则重新创建一个新的 Logger 实例，将日志输出归类到特定的命名空间下
    String logsNamespace = _conf.get(SPARK_SHUFFLE_SERVICE_LOGS_NAMESPACE_KEY, "");
    if (!logsNamespace.isEmpty()) {
      String className = YarnShuffleService.class.getName();
      logger = LoggerFactory.getLogger(className + "." + logsNamespace);
    }

    super.serviceInit(_conf);
    //读取配置，确定如果 Shuffle 服务启动失败，NodeManager 是否应该停止 (stopOnFailure)
    boolean stopOnFailure = _conf.getBoolean(STOP_ON_FAILURE_KEY, DEFAULT_STOP_ON_FAILURE);
    //仅在 NM 未设置恢复路径 (_recoveryPath == null) 且配置了集成测试模式时，创建一个临时目录作为恢复路径。这是为了方便测试
    if (_recoveryPath == null && _conf.getBoolean(INTEGRATION_TESTING, false)) {
      File tempDir = JavaUtils.createDirectory(System.getProperty("java.io.tmpdir"), "spark");
      tempDir.deleteOnExit();
      _recoveryPath = new Path(tempDir.toURI());
    }
    //如果 NodeManager 启用了恢复功能，并且设置了恢复路径 (_recoveryPath 不为 null)
    //从配置中获取持久化存储（如 LevelDB）的后端实现名称，默认使用 LevelDB
    if (_recoveryPath != null) {
      String dbBackendName = _conf.get(Constants.SHUFFLE_SERVICE_DB_BACKEND,
        DBBackend.LEVELDB.name());
      dbBackend = DBBackend.byName(dbBackendName);
      logger.info("Use {} as the implementation of {}",
        dbBackend, Constants.SHUFFLE_SERVICE_DB_BACKEND);
    }

    try {
      // In case this NM was killed while there were running spark applications, we need to restore
      // lost state for the existing executors. We look for an existing file in the NM's local dirs.
      // If we don't find one, then we choose a file to use to save the state next time.  Even if
      // an application was stopped while the NM was down, we expect yarn to call stopApplication()
      // when it comes back
      //如果启用了恢复，调用 initRecoveryDb 方法来确定并处理持久化文件位置，用于存储执行器注册状态和 Merge Shuffle 状态
      if (_recoveryPath != null) {
        registeredExecutorFile = initRecoveryDb(dbBackend.fileName(RECOVERY_FILE_NAME));
        mergeManagerFile =
          initRecoveryDb(dbBackend.fileName(SPARK_SHUFFLE_MERGE_RECOVERY_FILE_NAME));
      }
      //使用服务配置 _conf 创建网络传输配置 (TransportConf) 对象，指定其命名空间为 "shuffle"
      TransportConf transportConf = new TransportConf("shuffle", new HadoopConfigProvider(_conf));
      // Create new MergedShuffleFileManager if shuffleMergeManager is null.
      // This is because in the unit test, a customized MergedShuffleFileManager will
      // be created through setShuffleFileManager method.
      //如果内部变量 shuffleMergeManager 为空（通常在生产环境中为空），则调用 newMergedShuffleFileManagerInstance 方法，根据配置动态加载并实例化 Push-Based Shuffle 的合并管理器
      if (shuffleMergeManager == null) {
        shuffleMergeManager = newMergedShuffleFileManagerInstance(transportConf, mergeManagerFile);
      }
      //Shuffle 服务的核心业务逻辑组件，负责处理所有 Shuffle 数据块的查找和传输，并传入恢复文件和合并管理器
      blockHandler = new ExternalBlockHandler(
        transportConf, registeredExecutorFile, shuffleMergeManager);

      // If authentication is enabled, set up the shuffle server to use a
      // special RPC handler that filters out unauthenticated fetch requests
      //用于存储需要添加到 Shuffle Server 的引导程序（如认证引导程序）
      List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
      boolean authEnabled = _conf.getBoolean(SPARK_AUTHENTICATE_KEY, DEFAULT_SPARK_AUTHENTICATE);
      if (authEnabled) {
        secretManager = new ShuffleSecretManager();
        if (_recoveryPath != null) {
          loadSecretsFromDb();
        }
        bootstraps.add(new AuthServerBootstrap(transportConf, secretManager));
      }
      //从配置中获取 Shuffle Server 的监听端口，默认使用 7337
      int port = _conf.getInt(
        SPARK_SHUFFLE_SERVICE_PORT_KEY, DEFAULT_SPARK_SHUFFLE_SERVICE_PORT);
      transportContext = new TransportContext(transportConf, blockHandler, true);
      //启动实际的 TransportServer，监听配置的端口，并应用所有的引导程序（如认证）
      shuffleServer = transportContext.createServer(port, bootstraps);
      // the port should normally be fixed, but for tests its useful to find an open port
      port = shuffleServer.getPort();
      boundPort = port;
      String authEnabledString = authEnabled ? "enabled" : "not enabled";

      // register metrics on the block handler into the Node Manager's metrics system.
      //将 TransportServer 报告的已注册连接数添加到 blockHandler 的指标集合中
      blockHandler.getAllMetrics().getMetrics().put("numRegisteredConnections",
          shuffleServer.getRegisteredConnections());
      //将 TransportServer 报告的所有网络层指标添加到 blockHandler 的指标集合中。
      blockHandler.getAllMetrics().getMetrics().putAll(shuffleServer.getAllMetrics().getMetrics());
      //获取 Shuffle 服务指标在 Hadoop Metrics2 系统中的命名空间
      String metricsNamespace = _conf.get(SPARK_SHUFFLE_SERVICE_METRICS_NAMESPACE_KEY,
          DEFAULT_SPARK_SHUFFLE_SERVICE_METRICS_NAME);
      //将 blockHandler 的指标包装成一个 YarnShuffleServiceMetrics 对象
      YarnShuffleServiceMetrics serviceMetrics =
          new YarnShuffleServiceMetrics(metricsNamespace, blockHandler.getAllMetrics());
      YarnShuffleServiceMetrics mergeManagerMetrics =
          new YarnShuffleServiceMetrics("mergeManagerMetrics", shuffleMergeManager.getMetrics());

      MetricsSystemImpl metricsSystem = (MetricsSystemImpl) DefaultMetricsSystem.instance();
      metricsSystem.register(
          metricsNamespace, "Metrics on the Spark Shuffle Service", serviceMetrics);
      metricsSystem.register(
          "PushBasedShuffleMergeManager", "Metrics on the push-based shuffle merge manager",
          mergeManagerMetrics);
      logger.info("Registered metrics with Hadoop's DefaultMetricsSystem using namespace '{}'",
          metricsNamespace);

      logger.info("Started YARN shuffle service for Spark on port {}. " +
        "Authentication is {}.  Registered executor file is {}", port, authEnabledString,
        registeredExecutorFile);
    } catch (Exception e) {
      if (stopOnFailure) {
        throw e;
      } else {
        noteFailure(e);
      }
    }
  }

  /**
   * Set the customized MergedShuffleFileManager for unit testing only
   * @param mergeManager
   */
  @VisibleForTesting
  void setShuffleMergeManager(MergedShuffleFileManager mergeManager) {
    this.shuffleMergeManager = mergeManager;
  }

  @VisibleForTesting
  static MergedShuffleFileManager newMergedShuffleFileManagerInstance(
      TransportConf conf, File mergeManagerFile) {
    String mergeManagerImplClassName = conf.mergedShuffleFileManagerImpl();
    try {
      Class<?> mergeManagerImplClazz = Class.forName(
        mergeManagerImplClassName, true, Thread.currentThread().getContextClassLoader());
      Class<? extends MergedShuffleFileManager> mergeManagerSubClazz =
        mergeManagerImplClazz.asSubclass(MergedShuffleFileManager.class);
      // The assumption is that all the custom implementations just like the RemoteBlockPushResolver
      // will also need the transport configuration.
      return mergeManagerSubClazz.getConstructor(TransportConf.class, File.class)
        .newInstance(conf, mergeManagerFile);
    } catch (Exception e) {
      defaultLogger.error("Unable to create an instance of {}", mergeManagerImplClassName);
      return new NoOpMergedShuffleFileManager(conf, mergeManagerFile);
    }
  }

  private void loadSecretsFromDb() throws IOException {
    secretsFile = initRecoveryDb(dbBackend.fileName(SECRETS_RECOVERY_FILE_NAME));

    // Make sure this is protected in case its not in the NM recovery dir
    FileSystem fs = FileSystem.getLocal(_conf);
    fs.mkdirs(new Path(secretsFile.getPath()), new FsPermission((short) 0700));

    db = DBProvider.initDB(dbBackend, secretsFile, CURRENT_VERSION, mapper);
    logger.info("Recovery location is: " + secretsFile.getPath());
    if (db != null) {
      logger.info("Going to reload spark shuffle data");
      try (DBIterator itr = db.iterator()) {
        itr.seek(APP_CREDS_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
        while (itr.hasNext()) {
          Map.Entry<byte[], byte[]> e = itr.next();
          String key = new String(e.getKey(), StandardCharsets.UTF_8);
          if (!key.startsWith(APP_CREDS_KEY_PREFIX)) {
            break;
          }
          String id = parseDbAppKey(key);
          ByteBuffer secret = mapper.readValue(e.getValue(), ByteBuffer.class);
          logger.info("Reloading tokens for app: " + id);
          secretManager.registerApp(id, secret);
        }
      }
    }
  }

  private static String parseDbAppKey(String s) throws IOException {
    if (!s.startsWith(APP_CREDS_KEY_PREFIX)) {
      throw new IllegalArgumentException("expected a string starting with " + APP_CREDS_KEY_PREFIX);
    }
    String json = s.substring(APP_CREDS_KEY_PREFIX.length() + 1);
    AppId parsed = mapper.readValue(json, AppId.class);
    return parsed.appId;
  }

  private static byte[] dbAppKey(AppId appExecId) throws IOException {
    // we stick a common prefix on all the keys so we can find them in the DB
    String appExecJson = mapper.writeValueAsString(appExecId);
    String key = (APP_CREDS_KEY_PREFIX + ";" + appExecJson);
    return key.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {
    String appId = context.getApplicationId().toString();
    try {
      ByteBuffer appServiceData = context.getApplicationDataForService();
      String payload = JavaUtils.bytesToString(appServiceData);
      String shuffleSecret;
      Map<String, Object> metaInfo;
      try {
        metaInfo = mapper.readValue(payload,
            new TypeReference<Map<String, Object>>() {});
        Object metadataStorageVal = metaInfo.get(SPARK_SHUFFLE_SERVER_RECOVERY_DISABLED);
        if (metadataStorageVal != null && (Boolean) metadataStorageVal) {
          AppsWithRecoveryDisabled.disableRecoveryOfApp(appId);
          logger.info("Disabling metadata persistence for application {}", appId);
        }
      } catch (IOException ioe) {
        logger.warn("Unable to parse application data for service: " + payload);
        metaInfo = null;
      }
      if (isAuthenticationEnabled()) {
        if (metaInfo != null) {
          shuffleSecret = (String) metaInfo.get(SECRET_KEY);
        } else {
          shuffleSecret = payload;
        }
        if (db != null && AppsWithRecoveryDisabled.isRecoveryEnabledForApp(appId)) {
          AppId fullId = new AppId(appId);
          byte[] key = dbAppKey(fullId);
          ByteBuffer dbVal = metaInfo != null ?
              JavaUtils.stringToBytes(shuffleSecret) : appServiceData;
          byte[] value = mapper.writeValueAsString(dbVal).getBytes(StandardCharsets.UTF_8);
          db.put(key, value);
        }
        secretManager.registerApp(appId, shuffleSecret);
      }
    } catch (Exception e) {
      logger.error("Exception when initializing application {}", appId, e);
    }
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
    String appId = context.getApplicationId().toString();
    try {
      if (isAuthenticationEnabled()) {
        AppId fullId = new AppId(appId);
        if (db != null && AppsWithRecoveryDisabled.isRecoveryEnabledForApp(appId)) {
          try {
            db.delete(dbAppKey(fullId));
          } catch (IOException e) {
            logger.error("Error deleting {} from executor state db", appId, e);
          }
        }
        secretManager.unregisterApp(appId);
      }
      blockHandler.applicationRemoved(appId, false /* clean up local dirs */);
    } catch (Exception e) {
      logger.error("Exception when stopping application {}", appId, e);
    } finally {
      AppsWithRecoveryDisabled.removeApp(appId);
    }
  }

  @Override
  public void initializeContainer(ContainerInitializationContext context) {
    ContainerId containerId = context.getContainerId();
    logger.info("Initializing container {}", containerId);
  }

  @Override
  public void stopContainer(ContainerTerminationContext context) {
    ContainerId containerId = context.getContainerId();
    logger.info("Stopping container {}", containerId);
  }

  /**
   * Close the shuffle server to clean up any associated state.
   */
  @Override
  protected void serviceStop() {
    try {
      if (shuffleServer != null) {
        shuffleServer.close();
      }
      if (transportContext != null) {
        transportContext.close();
      }
      if (blockHandler != null) {
        blockHandler.close();
      }
      if (db != null) {
        db.close();
      }
    } catch (Exception e) {
      logger.error("Exception when stopping service", e);
    }
  }

  // Not currently used
  @Override
  public ByteBuffer getMetaData() {
    return ByteBuffer.allocate(0);
  }

  /**
   * Set the recovery path for shuffle service recovery when NM is restarted. This will be call
   * by NM if NM recovery is enabled.
   */
  @Override
  public void setRecoveryPath(Path recoveryPath) {
    _recoveryPath = recoveryPath;
  }

  /**
   * Get the path specific to this auxiliary service to use for recovery.
   */
  protected Path getRecoveryPath(String fileName) {
    return _recoveryPath;
  }

  /**
   * Figure out the recovery path and handle moving the DB if YARN NM recovery gets enabled
   * and DB exists in the local dir of NM by old version of shuffle service.
   */
  protected File initRecoveryDb(String dbName) {
    Preconditions.checkNotNull(_recoveryPath,
      "recovery path should not be null if NM recovery is enabled");

    File recoveryFile = new File(_recoveryPath.toUri().getPath(), dbName);
    if (recoveryFile.exists()) {
      return recoveryFile;
    }

    // db doesn't exist in recovery path go check local dirs for it
    String[] localDirs = _conf.getTrimmedStrings("yarn.nodemanager.local-dirs");
    for (String dir : localDirs) {
      File f = new File(new Path(dir).toUri().getPath(), dbName);
      if (f.exists()) {
        // If the recovery path is set then either NM recovery is enabled or another recovery
        // DB has been initialized. If NM recovery is enabled and had set the recovery path
        // make sure to move all DBs to the recovery path from the old NM local dirs.
        // If another DB was initialized first just make sure all the DBs are in the same
        // location.
        Path newLoc = new Path(_recoveryPath, dbName);
        Path copyFrom = new Path(f.toURI());
        if (!newLoc.equals(copyFrom)) {
          logger.info("Moving " + copyFrom + " to: " + newLoc);
          try {
            // The move here needs to handle moving non-empty directories across NFS mounts
            FileSystem fs = FileSystem.getLocal(_conf);
            fs.rename(copyFrom, newLoc);
          } catch (Exception e) {
            // Fail to move recovery file to new path, just continue on with new DB location
            logger.error("Failed to move recovery file {} to the path {}",
              dbName, _recoveryPath.toString(), e);
          }
        }
        return new File(newLoc.toUri().getPath());
      }
    }

    return new File(_recoveryPath.toUri().getPath(), dbName);
  }

  /**
   * Simply encodes an application ID.
   */
  public static class AppId {
    public final String appId;

    @JsonCreator
    public AppId(@JsonProperty("appId") String appId) {
      this.appId = appId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AppId appExecId = (AppId) o;
      return Objects.equals(appId, appExecId.appId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(appId);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("appId", appId)
          .toString();
    }
  }

}
