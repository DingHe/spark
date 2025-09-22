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

import java.net.URL
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import javax.annotation.concurrent.GuardedBy

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FsUrlStreamHandlerFactory, Path}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.execution.ui.{SQLAppStatusListener, SQLAppStatusStore, SQLTab, StreamingQueryStatusStore}
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.streaming.ui.{StreamingQueryStatusListener, StreamingQueryTab}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.Utils

/**
 * A class that holds all state shared across sessions in a given [[SQLContext]].
 *
 * @param sparkContext The Spark context associated with this SharedState
 * @param initialConfigs The configs from the very first created SparkSession
 */
//跨多个 SQL 会话（SQLContext）共享状态的类，它主要负责管理 Spark 和 Hadoop 的配置信息、流式查询、全局临时视图等
private[sql] class SharedState(
    val sparkContext: SparkContext,
    initialConfigs: scala.collection.Map[String, String])
  extends Logging {
  //是否支持hdfs://访问hdfs文件系统
  SharedState.setFsUrlStreamHandlerFactory(sparkContext.conf, sparkContext.hadoopConfiguration)
  //创建了两个配置对象：conf（SparkConf）和 hadoopConf（Hadoop 配置），用于存储 Spark 和 Hadoop 的配置信息
  private[sql] val (conf, hadoopConf) = {
    //解析数据仓库的路径，如果没有设置spark.sql.warehouse.dir，则默认使用hive的
    val warehousePath = SharedState.resolveWarehousePath(
      sparkContext.conf, sparkContext.hadoopConfiguration, initialConfigs)

    val confClone = sparkContext.conf.clone()
    val hadoopConfClone = new Configuration(sparkContext.hadoopConfiguration)
    // Extract entries from `SparkConf` and put them in the Hadoop conf.
    //将 SparkConf 中的配置信息复制到 Hadoop 配置中
    confClone.getAll.foreach { case (k, v) =>
      if (v ne null) hadoopConfClone.set(k, v)
    }
    // If `SparkSession` is instantiated using an existing `SparkContext` instance and no existing
    // `SharedState`, all `SparkSession` level configurations have higher priority to generate a
    // `SharedState` instance. This will be done only once then shared across `SparkSession`s
    initialConfigs.foreach {
      // We have resolved the warehouse path and should not set warehouse conf here.
      //如果配置项是 WAREHOUSE_PATH.key 或 SharedState.HIVE_WAREHOUSE_CONF_NAME（这两个键与仓库路径相关），则不会在此处进行设置，避免重复设置仓库路径
      case (k, _) if k == WAREHOUSE_PATH.key || k == SharedState.HIVE_WAREHOUSE_CONF_NAME =>
      case (k, v) if SQLConf.isStaticConfigKey(k) =>
        logDebug(s"Applying static initial session options to SparkConf: $k -> $v")
        confClone.set(k, v)
      case (k, v) =>
        logDebug(s"Applying other initial session options to HadoopConf: $k -> $v")
        hadoopConfClone.set(k, v)
    }
    //数据仓库路径补全
    val qualified = try {
      SharedState.qualifyWarehousePath(hadoopConfClone, warehousePath)
    } catch {
      case NonFatal(e) =>
        logWarning("Cannot qualify the warehouse path, leaving it unqualified.", e)
        warehousePath
    }
    // Set warehouse path in the SparkConf and Hadoop conf, so that it's application wide reachable
    // from `SparkContext`.
    SharedState.setWarehousePathConf(sparkContext.conf, sparkContext.hadoopConfiguration, qualified)
    SharedState.setWarehousePathConf(confClone, hadoopConfClone, qualified)
    (confClone, hadoopConfClone)
  }

  /**
   * Class for caching query results reused in future executions.
   */
  val cacheManager: CacheManager = new CacheManager

  /** A global lock for all streaming query lifecycle tracking and management. */
  private[sql] val activeQueriesLock = new Object

  /** 一个 ConcurrentHashMap，用于管理当前所有活跃的流式查询，键是 UUID，值是 StreamExecution 对象
   * A map of active streaming queries to the session specific StreamingQueryManager that manages
   * the lifecycle of that stream.
   */
  @GuardedBy("activeQueriesLock")
  private[sql] val activeStreamingQueries = new ConcurrentHashMap[UUID, StreamExecution]()

  /** 它用于存储 SQL 相关的状态和指标信息，并通过 SQLAppStatusListener 将其与 Spark 的状态队列关联，以便 UI 展示和查询
   * A status store to query SQL status/metrics of this Spark application, based on SQL-specific
   * [[org.apache.spark.scheduler.SparkListenerEvent]]s.
   */
  val statusStore: SQLAppStatusStore = {
    val kvStore = sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new SQLAppStatusListener(conf, kvStore, live = true)
    sparkContext.listenerBus.addToStatusQueue(listener)
    val statusStore = new SQLAppStatusStore(kvStore, Some(listener))
    sparkContext.ui.foreach(new SQLTab(statusStore, _))
    statusStore
  }

  /**
   * A [[StreamingQueryListener]] for structured streaming ui, it contains all streaming query ui
   * data to show.
   */
  lazy val streamingQueryStatusListener: Option[StreamingQueryStatusListener] = {
    sparkContext.ui.flatMap { ui =>
      if (conf.get(STREAMING_UI_ENABLED)) {
        val kvStore = sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
        new StreamingQueryTab(new StreamingQueryStatusStore(kvStore), ui)
        Some(new StreamingQueryStatusListener(conf, kvStore))
      } else {
        None
      }
    }
  }

  /** 用于与外部系统交互的目录，通常是 Hive 元数据库。它确保如果数据库不存在，能够创建一个名为 default 的数据库
   * A catalog that interacts with external systems.
   */
  lazy val externalCatalog: ExternalCatalogWithListener = {
    val externalCatalog = SharedState.reflect[ExternalCatalog, SparkConf, Configuration](
      SharedState.externalCatalogClassName(conf), conf, hadoopConf)

    // Create default database if it doesn't exist
    // If database name not equals 'default', throw exception
    //检查默认数据库是否存在，如果不存在，创建一个名为 default 的数据库
    if (!externalCatalog.databaseExists(SQLConf.get.defaultDatabase)) {
      if (!SessionCatalog.DEFAULT_DATABASE.equalsIgnoreCase(SQLConf.get.defaultDatabase)) {
        throw QueryExecutionErrors.defaultDatabaseNotExistsError(
          SQLConf.get.defaultDatabase
        )
      }
      val defaultDbDefinition = CatalogDatabase(
        SQLConf.get.defaultDatabase,
        "default database",
        CatalogUtils.stringToURI(conf.get(WAREHOUSE_PATH)),
        Map())
      // There may be another Spark application creating default database at the same time, here we
      // set `ignoreIfExists = true` to avoid `DatabaseAlreadyExists` exception.
      externalCatalog.createDatabase(defaultDbDefinition, ignoreIfExists = true)
    }

    // Wrap to provide catalog events
    val wrapped = new ExternalCatalogWithListener(externalCatalog)

    // Make sure we propagate external catalog events to the spark listener bus
    wrapped.addListener((event: ExternalCatalogEvent) => sparkContext.listenerBus.post(event))

    wrapped
  }

  /**
   * A manager for global temporary views.
   */
    //负责管理 Spark 中的全局临时视图
  lazy val globalTempViewManager: GlobalTempViewManager = {
    val globalTempDB = conf.get(GLOBAL_TEMP_DATABASE)
    //获取全局临时数据库的名称
    if (externalCatalog.databaseExists(globalTempDB)) {
      throw QueryExecutionErrors.databaseNameConflictWithSystemPreservedDatabaseError(globalTempDB)
    }
    new GlobalTempViewManager(globalTempDB)
  }

  /**
   * A classloader used to load all user-added jar.
   */
  val jarClassLoader = new NonClosableMutableURLClassLoader(
    org.apache.spark.util.Utils.getContextOrSparkClassLoader)

}

object SharedState extends Logging {
  @volatile private var fsUrlStreamHandlerFactoryInitialized = false

  private def setFsUrlStreamHandlerFactory(conf: SparkConf, hadoopConf: Configuration): Unit = {
    if (!fsUrlStreamHandlerFactoryInitialized &&
        conf.get(DEFAULT_URL_STREAM_HANDLER_FACTORY_ENABLED)) { //支持hdfs://访问hdfs文件
      synchronized {
        if (!fsUrlStreamHandlerFactoryInitialized) {
          try {
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(hadoopConf))
            fsUrlStreamHandlerFactoryInitialized = true
          } catch {
            case NonFatal(_) =>
              logWarning("URL.setURLStreamHandlerFactory failed to set FsUrlStreamHandlerFactory")
          }
        }
      }
    }
  }
  //hive数据仓库元数据访问类
  private val HIVE_EXTERNAL_CATALOG_CLASS_NAME = "org.apache.spark.sql.hive.HiveExternalCatalog"
  //根据参数访问元数据类
  private def externalCatalogClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_EXTERNAL_CATALOG_CLASS_NAME
      case "in-memory" => classOf[InMemoryCatalog].getCanonicalName
    }
  }

  /**
   * Helper method to create an instance of [[T]] using a single-arg constructor that
   * accepts an [[Arg1]] and an [[Arg2]].
   */
    //通过反射创建指定类的实例。该方法接收类名和两个构造函数参数，使用这些参数来反射地调用类的构造函数
  private def reflect[T, Arg1 <: AnyRef, Arg2 <: AnyRef](
      className: String,
      ctorArg1: Arg1,
      ctorArg2: Arg2)(
      implicit ctorArgTag1: ClassTag[Arg1],
      ctorArgTag2: ClassTag[Arg2]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag1.runtimeClass, ctorArgTag2.runtimeClass)
      val args = Array[AnyRef](ctorArg1, ctorArg2)
      ctor.newInstance(args: _*).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  private val HIVE_WAREHOUSE_CONF_NAME = "hive.metastore.warehouse.dir"

  /**
   * Determine the warehouse path using the key `spark.sql.warehouse.dir` in the [[SparkConf]]
   * or the initial options from the very first created SparkSession instance, and
   * `hive.metastore.warehouse.dir` in hadoop [[Configuration]].
   * The priority order is:
   * s.s.w.d in initialConfigs
   *   > s.s.w.d in spark conf (user specified)
   *   > h.m.w.d in hadoop conf (user specified)
   *   > s.s.w.d in spark conf (default)
   *
   * @return the resolved warehouse path.
   */
    //目的是根据一定的优先级规则，确定 Spark 的 spark.sql.warehouse.dir 配置项的路径
  def resolveWarehousePath(
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      initialConfigs: scala.collection.Map[String, String] = Map.empty): String = {
    //尝试从 initialConfigs 或 sparkConf 中获取 spark.sql.warehouse.dir 配置项的值，优先从 initialConfigs 获取
    val sparkWarehouseOption =
      initialConfigs.get(WAREHOUSE_PATH.key).orElse(sparkConf.getOption(WAREHOUSE_PATH.key))
    //不允许设置 hive.metastore.warehouse.dir
    if (initialConfigs.contains(HIVE_WAREHOUSE_CONF_NAME)) {
      logWarning(s"Not allowing to set $HIVE_WAREHOUSE_CONF_NAME in SparkSession's " +
        s"options, please use ${WAREHOUSE_PATH.key} to set statically for cross-session usages")
    }
    // hive.metastore.warehouse.dir only stay in hadoopConf
    sparkConf.remove(HIVE_WAREHOUSE_CONF_NAME)
    // Set the Hive metastore warehouse path to the one we use
    val hiveWarehouseDir = hadoopConf.get(HIVE_WAREHOUSE_CONF_NAME)
    //如果 hive.metastore.warehouse.dir 已设置且 spark.sql.warehouse.dir 没有设置
    if (hiveWarehouseDir != null && sparkWarehouseOption.isEmpty) {
      // If hive.metastore.warehouse.dir is set and spark.sql.warehouse.dir is not set,
      // we will respect the value of hive.metastore.warehouse.dir.
      logInfo(s"${WAREHOUSE_PATH.key} is not set, but $HIVE_WAREHOUSE_CONF_NAME is set. " +
        s"Setting ${WAREHOUSE_PATH.key} to the value of $HIVE_WAREHOUSE_CONF_NAME.")
      hiveWarehouseDir
    } else {
      // If spark.sql.warehouse.dir is set, we will override hive.metastore.warehouse.dir using
      // the value of spark.sql.warehouse.dir.
      // When neither spark.sql.warehouse.dir nor hive.metastore.warehouse.dir is set
      // we will set hive.metastore.warehouse.dir to the default value of spark.sql.warehouse.dir.
      val sparkWarehouseDir = sparkWarehouseOption.getOrElse(WAREHOUSE_PATH.defaultValueString)
      logInfo(s"Setting $HIVE_WAREHOUSE_CONF_NAME ('$hiveWarehouseDir') to the value of " +
        s"${WAREHOUSE_PATH.key}.")
      sparkWarehouseDir
    }
  }
  //数据仓库路径补全
  def qualifyWarehousePath(hadoopConf: Configuration, warehousePath: String): String = {
    val tempPath = new Path(warehousePath)
    val qualified = tempPath.getFileSystem(hadoopConf).makeQualified(tempPath).toString
    logInfo(s"Warehouse path is '$qualified'.")
    qualified
  }
  //分别在spark和hadoop环境设置数据仓库路径
  def setWarehousePathConf(
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      warehousePath: String): Unit = {
    sparkConf.set(WAREHOUSE_PATH.key, warehousePath)
    hadoopConf.set(HIVE_WAREHOUSE_CONF_NAME, warehousePath)
  }
}
