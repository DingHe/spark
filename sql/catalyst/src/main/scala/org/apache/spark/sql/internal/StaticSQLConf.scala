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

package org.apache.spark.sql.internal

import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.util.Utils


/**
 * Static SQL configuration is a cross-session, immutable Spark configuration. External users can
 * see the static sql configs via `SparkSession.conf`, but can NOT set/unset them.
 */
// StaticSQLConf 定义的是 静态 SQL 配置（Static SQL Configuration）。
// 其核心特性如下：
// 跨会话共享（Cross-session）：与普通的 SQLConf 不同，静态配置在整个 SparkContext 或 SparkSession 生命周期内是全局共享的。
// 不可变性（Immutable）：一旦 Spark 应用启动并初始化了 SparkSession，这些配置就不能再通过 spark.conf.set 进行动态修改。
// 配置方式：外部用户可以通过 SparkSession.conf 查看这些配置，但只能在启动应用时通过 spark-submit --conf 或 SparkConf 对象进行设置。
object StaticSQLConf {

  import SQLConf.buildStaticConf
  // 定义受管数据库（Managed Databases）和表（Tables）的默认存储位置
  // 默认值：当前路径下的 spark-warehouse。
  val WAREHOUSE_PATH = buildStaticConf("spark.sql.warehouse.dir")
    .doc("The default location for managed databases and tables.")
    .version("2.0.0")
    .stringConf
    .createWithDefault(Utils.resolveURI("spark-warehouse").toString)
  // 定义 Session Catalog 的默认数据库名称
  // 默认值：default
  val CATALOG_DEFAULT_DATABASE =
    buildStaticConf(s"spark.sql.catalog.$SESSION_CATALOG_NAME.defaultDatabase")
    .doc("The default database for session catalog.")
    .version("3.4.0")
    .stringConf
    .createWithDefault("default")
  // 决定 Spark 使用哪种 Catalog 实现。
  // 可选值：hive（使用 Hive Metastore）或 in-memory（仅内存存储）。
  val CATALOG_IMPLEMENTATION = buildStaticConf("spark.sql.catalogImplementation")
    .internal()
    .version("2.0.0")
    .stringConf
    .checkValues(Set("hive", "in-memory"))
    .createWithDefault("in-memory")
  // 作用：定义存放“全局临时视图”的系统保留数据库名。
  // 说明：输入会自动转为小写。默认值为 global_temp。
  val GLOBAL_TEMP_DATABASE = buildStaticConf("spark.sql.globalTempDatabase")
    .internal()
    .version("2.1.0")
    .stringConf
    // System preserved database should not exists in metastore. However it's hard to guarantee it
    // for every session, because case-sensitivity differs. Here we always lowercase it to make our
    // life easier.
    .transform(_.toLowerCase(Locale.ROOT))
    .createWithDefault("global_temp")

  // This is used to control when we will split a schema's JSON string to multiple pieces
  // in order to fit the JSON string in metastore's table property (by default, the value has
  // a length restriction of 4000 characters, so do not use a value larger than 4000 as the default
  // value of this property). We will split the JSON string of a schema to its length exceeds the
  // threshold. Note that, this conf is only read in HiveExternalCatalog which is cross-session,
  // that's why this conf has to be a static SQL conf.
  // 作用：Hive Metastore 对表属性有长度限制（通常 4000 字符）。当 Schema 的 JSON 字符串超过此值时，Spark 会将其拆分成多块存储。
  val SCHEMA_STRING_LENGTH_THRESHOLD =
    buildStaticConf("spark.sql.sources.schemaStringLengthThreshold")
      .internal()
      .doc("The maximum length allowed in a single cell when " +
        "storing additional schema information in Hive's metastore.")
      .version("1.3.1")
      .intConf
      .createWithDefault(4000)
  // 作用：文件数据源表名到执行计划关系的缓存大小。增加它可以加快重复查询相同表的解析速度。
  val FILESOURCE_TABLE_RELATION_CACHE_SIZE =
    buildStaticConf("spark.sql.filesourceTableRelationCacheSize")
      .internal()
      .doc("The maximum size of the cache that maps qualified table names to table relation plans.")
      .version("2.2.0")
      .intConf
      .checkValue(cacheSize => cacheSize >= 0, "The maximum size of the cache must not be negative")
      .createWithDefault(1000)
  // 作用：算子和表达式生成的 Java 代码缓存的最大条目数。缓存可以避免重复编译代码带来的 CPU 开销。
  val CODEGEN_CACHE_MAX_ENTRIES = buildStaticConf("spark.sql.codegen.cache.maxEntries")
      .internal()
      .doc("When nonzero, enable caching of generated classes for operators and expressions. " +
        "All jobs share the cache that can use up to the specified number for generated classes.")
      .version("2.4.0")
      .intConf
      .checkValue(maxEntries => maxEntries >= 0, "The maximum must not be negative")
      .createWithDefault(100)
  // 作用：是否在生成的 Java 代码中包含注释。
  // 说明：默认关闭，因为生成大量注释会严重影响复杂查询的编译性能。
  val CODEGEN_COMMENTS = buildStaticConf("spark.sql.codegen.comments")
    .internal()
    .doc("When true, put comment in the generated code. Since computing huge comments " +
      "can be extremely expensive in certain cases, such as deeply-nested expressions which " +
      "operate over inputs with wide schemas, default is false.")
    .version("2.0.0")
    .booleanConf
    .createWithDefault(false)

  // When enabling the debug, Spark SQL internal table properties are not filtered out; however,
  // some related DDL commands (e.g., ANALYZE TABLE and CREATE TABLE LIKE) might not work properly.
  val DEBUG_MODE = buildStaticConf("spark.sql.debug")
    .internal()
    .doc("Only used for internal debugging. Not all functions are supported when it is enabled.")
    .version("2.1.0")
    .booleanConf
    .createWithDefault(false)
  // 作用：Hive Thrift Server 是否运行在单会话模式。开启后，所有连接共享临时视图和配置。
  val HIVE_THRIFT_SERVER_SINGLESESSION =
    buildStaticConf("spark.sql.hive.thriftServer.singleSession")
      .doc("When set to true, Hive Thrift server is running in a single session mode. " +
        "All the JDBC/ODBC connections share the temporary views, function registries, " +
        "SQL configuration and the current database.")
      .version("1.6.0")
      .booleanConf
      .createWithDefault(false)
  // 作用：允许用户注入自定义扩展类（如自定义解析器、优化规则、物理策略）。
  // 说明：这是 Gluten、Iceberg 等项目接入 Spark 的核心入口。
  val SPARK_SESSION_EXTENSIONS = buildStaticConf("spark.sql.extensions")
    .doc("A comma-separated list of classes that implement " +
      "Function1[SparkSessionExtensions, Unit] used to configure Spark Session extensions. The " +
      "classes must have a no-args constructor. If multiple extensions are specified, they are " +
      "applied in the specified order. For the case of rules and planner strategies, they are " +
      "applied in the specified order. For the case of parsers, the last parser is used and each " +
      "parser can delegate to its predecessor. For the case of function name conflicts, the last " +
      "registered function name is used.")
    .version("2.2.0")
    .stringConf
    .toSequence
    .createOptional
  // 作用：定义用于将数据转化为缓存格式的类名。允许替换默认的列式缓存实现。
  val SPARK_CACHE_SERIALIZER = buildStaticConf("spark.sql.cache.serializer")
    .doc("The name of a class that implements " +
      "org.apache.spark.sql.columnar.CachedBatchSerializer. It will be used to " +
      "translate SQL data into a format that can more efficiently be cached. The underlying " +
      "API is subject to change so use with caution. Multiple classes cannot be specified. " +
      "The class must have a no-arg constructor.")
    .version("3.1.0")
    .stringConf
    .createWithDefault("org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer")
  // 作用：查询执行监听器类名列表。用于监控查询的成功或失败。
  val QUERY_EXECUTION_LISTENERS = buildStaticConf("spark.sql.queryExecutionListeners")
    .doc("List of class names implementing QueryExecutionListener that will be automatically " +
      "added to newly created sessions. The classes should have either a no-arg constructor, " +
      "or a constructor that expects a SparkConf argument.")
    .version("2.3.0")
    .stringConf
    .toSequence
    .createOptional

  val STREAMING_QUERY_LISTENERS = buildStaticConf("spark.sql.streaming.streamingQueryListeners")
    .doc("List of class names implementing StreamingQueryListener that will be automatically " +
      "added to newly created sessions. The classes should have either a no-arg constructor, " +
      "or a constructor that expects a SparkConf argument.")
    .version("2.4.0")
    .stringConf
    .toSequence
    .createOptional

  val UI_RETAINED_EXECUTIONS =
    buildStaticConf("spark.sql.ui.retainedExecutions")
      .doc("Number of executions to retain in the Spark UI.")
      .version("1.5.0")
      .intConf
      .createWithDefault(1000)
  // 作用：执行广播（Broadcast）操作时拉取数据的最大并行度。
  // 说明：范围 (0, 128]。如果遇到 OOM，建议调低此值。
  val BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD =
    buildStaticConf("spark.sql.broadcastExchange.maxThreadThreshold")
      .internal()
      .doc("The maximum degree of parallelism to fetch and broadcast the table. " +
        "If we encounter memory issue like frequently full GC or OOM when broadcast table " +
        "we can decrease this number in order to reduce memory usage. " +
        "Notice the number should be carefully chosen since decreasing parallelism might " +
        "cause longer waiting for other broadcasting. Also, increasing parallelism may " +
        "cause memory problem.")
      .version("3.0.0")
      .intConf
      .checkValue(thres => thres > 0 && thres <= 128, "The threshold must be in (0,128].")
      .createWithDefault(128)
  // 作用：执行子查询的最大线程并行度。默认 16。
  val SUBQUERY_MAX_THREAD_THRESHOLD =
    buildStaticConf("spark.sql.subquery.maxThreadThreshold")
      .internal()
      .doc("The maximum degree of parallelism to execute the subquery.")
      .version("2.4.6")
      .intConf
      .checkValue(thres => thres > 0 && thres <= 128, "The threshold must be in (0,128].")
      .createWithDefault(16)
  // 作用：SQL 事件日志中 SQL 文本的截断长度。防止超长 SQL 撑爆日志。
  val SQL_EVENT_TRUNCATE_LENGTH = buildStaticConf("spark.sql.event.truncate.length")
    .doc("Threshold of SQL length beyond which it will be truncated before adding to " +
      "event. Defaults to no truncation. If set to 0, callsite will be logged instead.")
    .version("3.0.0")
    .intConf
    .checkValue(_ >= 0, "Must be set greater or equal to zero")
    .createWithDefault(Int.MaxValue)

  val SQL_LEGACY_SESSION_INIT_WITH_DEFAULTS =
    buildStaticConf("spark.sql.legacy.sessionInitWithConfigDefaults")
      .internal()
      .doc("Flag to revert to legacy behavior where a cloned SparkSession receives SparkConf " +
        "defaults, dropping any overrides in its parent SparkSession.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)
      // 作用：是否注册 Hadoop 的 FsUrlStreamHandlerFactory。这对于支持从 HDFS ADD JAR 非常重要。
  //FsUrlStreamHandlerFactory 是 Hadoop 中的一个类，用于为文件系统（如 HDFS、S3、文件系统等）提供 URL 流处理的支持,
  // 使得 Hadoop 的文件系统（例如 HDFS）可以作为一种协议来处理，这样你就能够使用 hdfs://、file:// 等 URL 协议来引用和访问 Hadoop 文件系统中的文件。
  val DEFAULT_URL_STREAM_HANDLER_FACTORY_ENABLED =
    buildStaticConf("spark.sql.defaultUrlStreamHandlerFactory.enabled")
      .internal()
      .doc(
        "When true, register Hadoop's FsUrlStreamHandlerFactory to support " +
        "ADD JAR against HDFS locations. " +
        "It should be disabled when a different stream protocol handler should be registered " +
        "to support a particular protocol type, or if Hadoop's FsUrlStreamHandlerFactory " +
        "conflicts with other protocol types such as `http` or `https`. See also SPARK-25694 " +
        "and HADOOP-14598.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val STREAMING_UI_ENABLED =
    buildStaticConf("spark.sql.streaming.ui.enabled")
      .doc("Whether to run the Structured Streaming Web UI for the Spark application when the " +
        "Spark Web UI is enabled.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val STREAMING_UI_RETAINED_PROGRESS_UPDATES =
    buildStaticConf("spark.sql.streaming.ui.retainedProgressUpdates")
      .doc("The number of progress updates to retain for a streaming query for Structured " +
        "Streaming UI.")
      .version("3.0.0")
      .intConf
      .createWithDefault(100)

  val STREAMING_UI_RETAINED_QUERIES =
    buildStaticConf("spark.sql.streaming.ui.retainedQueries")
      .doc("The number of inactive queries to retain for Structured Streaming UI.")
      .version("3.0.0")
      .intConf
      .createWithDefault(100)
  // 作用：元数据缓存（如分区文件信息）的生存时间（TTL）。设置为正数时生效。
  val METADATA_CACHE_TTL_SECONDS = buildStaticConf("spark.sql.metadataCacheTTLSeconds")
    .doc("Time-to-live (TTL) value for the metadata caches: partition file metadata cache and " +
      "session catalog cache. This configuration only has an effect when this value having " +
      "a positive value (> 0). It also requires setting " +
      s"'${StaticSQLConf.CATALOG_IMPLEMENTATION.key}' to `hive`, setting " +
      s"'${SQLConf.HIVE_FILESOURCE_PARTITION_FILE_CACHE_SIZE.key}' > 0 and setting " +
      s"'${SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key}' to `true` " +
      "to be applied to the partition file metadata cache.")
    .version("3.1.0")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefault(-1)

  val ENABLED_STREAMING_UI_CUSTOM_METRIC_LIST =
    buildStaticConf("spark.sql.streaming.ui.enabledCustomMetricList")
      .internal()
      .doc("Configures a list of custom metrics on Structured Streaming UI, which are enabled. " +
        "The list contains the name of the custom metrics separated by comma. In aggregation" +
        "only sum used. The list of supported custom metrics is state store provider specific " +
        "and it can be found out for example from query progress log entry.")
      .version("3.1.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)
  // 作用：禁用的 JDBC 连接提供程序列表。
  val DISABLED_JDBC_CONN_PROVIDER_LIST =
    buildStaticConf("spark.sql.sources.disabledJdbcConnProviderList")
      .doc("Configures a list of JDBC connection providers, which are disabled. " +
        "The list contains the name of the JDBC connection providers separated by comma.")
      .version("3.1.0")
      .stringConf
      .createWithDefault("")
}
