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

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Unstable
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveRulesHolder
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.{DependencyUtils, Utils}

/**
 * A class that holds all session-specific state in a given [[SparkSession]].
 *
 * @param sharedState The state shared across sessions, e.g. global view manager, external catalog.
 * @param conf SQL-specific key-value configurations.
 * @param experimentalMethods Interface to add custom planning strategies and optimizers.
 * @param functionRegistry Internal catalog for managing functions registered by the user.
 * @param udfRegistration Interface exposed to the user for registering user-defined functions.
 * @param udtfRegistration Interface exposed to the user for registering user-defined
 *                         table functions.
 * @param catalogBuilder a function to create an internal catalog for managing table and database
 *                       states.
 * @param sqlParser Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
 * @param analyzerBuilder A function to create the logical query plan analyzer for resolving
 *                        unresolved attributes and relations.
 * @param optimizerBuilder a function to create the logical query plan optimizer.
 * @param planner Planner that converts optimized logical plans to physical plans.
 * @param streamingQueryManagerBuilder A function to create a streaming query manager to
 *                                     start and stop streaming queries.
 * @param listenerManager Interface to register custominternal/SessionState.scala
 *                        [[org.apache.spark.sql.util.QueryExecutionListener]]s.
 * @param resourceLoaderBuilder a function to create a session shared resource loader to load JARs,
 *                              files, etc.
 * @param createQueryExecution Function used to create QueryExecution objects.
 * @param createClone Function used to create clones of the session state.
 */
private[sql] class SessionState(
    sharedState: SharedState,      //表示在多个会话间共享的状态，通常包括全局视图管理器、外部目录等。这个属性有助于多个 SparkSession 实例之间共享一些公共资源
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods,  //提供了一些接口，用于用户自定义查询规划策略和优化器
    val functionRegistry: FunctionRegistry,    //内部函数注册表，用于管理用户注册的函数
    val tableFunctionRegistry: TableFunctionRegistry,   //用于注册用户定义的表函数
    val udfRegistration: UDFRegistration,   //用户自定义函数（UDF）的注册接口，允许用户注册自己的函数
    val udtfRegistration: UDTFRegistration,  //用户自定义表函数（UDTF）的注册接口，类似于 udfRegistration，用于注册可以返回表的自定义函数
    catalogBuilder: () => SessionCatalog, //创建一个内部目录（catalog）管理表和数据库的状态
    val sqlParser: ParserInterface,  //SQL 解析器，用于从 SQL 文本中提取表达式、查询计划、表标识符等信息
    analyzerBuilder: () => Analyzer,  //创建用于解析未解析的属性和关系的逻辑查询分析器
    optimizerBuilder: () => Optimizer,  //创建用于优化逻辑查询计划的优化器
    val planner: SparkPlanner,   //将优化后的逻辑计划转换为物理计划
    val streamingQueryManagerBuilder: () => StreamingQueryManager,  //创建流式查询管理器，用于启动和停止流式查询
    val listenerManager: ExecutionListenerManager,
    resourceLoaderBuilder: () => SessionResourceLoader,  //创建一个共享的资源加载器，用于加载 JAR 文件、外部文件等
    createQueryExecution: (LogicalPlan, CommandExecutionMode.Value) => QueryExecution,  //用于创建一个 QueryExecution 对象，用于执行逻辑查询计划
    createClone: (SparkSession, SessionState) => SessionState,
    val columnarRules: Seq[ColumnarRule],  //列式处理的规则序列，用于优化列式存储格式的执行
    val adaptiveRulesHolder: AdaptiveRulesHolder, //保存自适应查询执行的规则，可能用于在执行时动态调整查询执行计划
    val planNormalizationRules: Seq[Rule[LogicalPlan]]) { //查询计划标准化的规则，用于将查询计划转换为统一的标准形式

  // The following fields are lazy to avoid creating the Hive client when creating SessionState.
  lazy val catalog: SessionCatalog = catalogBuilder()

  lazy val analyzer: Analyzer = analyzerBuilder()

  lazy val optimizer: Optimizer = optimizerBuilder()

  lazy val resourceLoader: SessionResourceLoader = resourceLoaderBuilder()

  // The streamingQueryManager is lazy to avoid creating a StreamingQueryManager for each session
  // when connecting to ThriftServer.
  lazy val streamingQueryManager: StreamingQueryManager = streamingQueryManagerBuilder()

  def catalogManager: CatalogManager = analyzer.catalogManager

  def newHadoopConf(): Configuration = SessionState.newHadoopConf(
    sharedState.sparkContext.hadoopConfiguration,
    conf)

  def newHadoopConfWithOptions(options: Map[String, String]): Configuration = {
    val hadoopConf = newHadoopConf()
    options.foreach { case (k, v) =>
      if ((v ne null) && k != "path" && k != "paths") {
        hadoopConf.set(k, v)
      }
    }
    hadoopConf
  }

  /**
   * Get an identical copy of the `SessionState` and associate it with the given `SparkSession`
   */
  def clone(newSparkSession: SparkSession): SessionState = createClone(newSparkSession, this)

  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------
  //创建QueryExecution对象
  def executePlan(
      plan: LogicalPlan,
      mode: CommandExecutionMode.Value = CommandExecutionMode.ALL): QueryExecution =
    createQueryExecution(plan, mode)   //createQueryExecution实际在BaseSessionStateBuilder里面构建，就是直接new
}

private[sql] object SessionState {
  def newHadoopConf(hadoopConf: Configuration, sqlConf: SQLConf): Configuration = {
    val newHadoopConf = new Configuration(hadoopConf)
    sqlConf.getAllConfs.foreach { case (k, v) => if (v ne null) newHadoopConf.set(k, v) }
    newHadoopConf
  }
}

/**
 * Concrete implementation of a [[BaseSessionStateBuilder]].
 */
@Unstable
class SessionStateBuilder(
    session: SparkSession,
    parentState: Option[SessionState])
  extends BaseSessionStateBuilder(session, parentState) {
  override protected def newBuilder: NewBuilder = new SessionStateBuilder(_, _)
}

/**
 * Session shared [[FunctionResourceLoader]].
 */
@Unstable
class SessionResourceLoader(session: SparkSession) extends FunctionResourceLoader {
  override def loadResource(resource: FunctionResource): Unit = {
    resource.resourceType match {
      case JarResource => addJar(resource.uri)
      case FileResource => session.sparkContext.addFile(resource.uri)
      case ArchiveResource => session.sparkContext.addArchive(resource.uri)
    }
  }

  def resolveJars(path: URI): Seq[String] = {
    path.getScheme match {
      case "ivy" => DependencyUtils.resolveMavenDependencies(path)
      case _ => path.toString :: Nil
    }
  }

  /**
   * Add a jar path to [[SparkContext]] and the classloader.
   *
   * Note: this method seems not access any session state, but a Hive based `SessionState` needs
   * to add the jar to its hive client for the current session. Hence, it still needs to be in
   * [[SessionState]].
   */
  def addJar(path: String): Unit = {
    val uri = Utils.resolveURI(path)
    resolveJars(uri).foreach { p =>
      session.sparkContext.addJar(p)
      val uri = new Path(p).toUri
      val jarURL = if (uri.getScheme == null) {
        // `path` is a local file path without a URL scheme
        new File(p).toURI.toURL
      } else {
        // `path` is a URL with a scheme
        uri.toURL
      }
      session.sharedState.jarClassLoader.addURL(jarURL)
    }
    Thread.currentThread().setContextClassLoader(session.sharedState.jarClassLoader)
  }
}
