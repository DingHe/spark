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
package org.apache.spark.sql.execution.datasources.v2

import java.util

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

/**
 * A base interface for data source v2 implementations of the built-in file-based data sources.
 */
// 所有内置文件类型数据源 V2 实现（如 Parquet V2、ORC V2、JSON V2 等）的基类
// 作用是为 Spark 中所有基于文件的（File-based）数据源 V2 实现提供一套通用的、文件相关的基础设施和逻辑
// 处理回退机制： 提供了 fallbackFileFormat 属性，支持在 V2 实现出现问题时，能够回退到 V1 的 FileFormat 实现
// 支持外部元数据： 默认声明支持外部元数据（supportsExternalMetadata = true），允许 Spark 优先使用用户或 Catalog 提供的 Schema，避免重复推断
trait FileDataSourceV2 extends TableProvider with DataSourceRegister {
  /**
   * Returns a V1 [[FileFormat]] class of the same file data source.
   * This is a solution for the following cases:
   * 1. File datasource V2 implementations cause regression. Users can disable the problematic data
   *    source via SQL configuration and fall back to FileFormat.
   * 2. Catalog support is required, which is still under development for data source V2.
   */
  // V1 回退格式
  // 强制子类实现，返回一个对应的 V1 FileFormat 类。用于支持 V2 禁用时回退到 V1 机制
  def fallbackFileFormat: Class[_ <: FileFormat]

  lazy val sparkSession = SparkSession.active

  //提取文件路径。 从配置选项 map 中提取文件路径
  protected def getPaths(map: CaseInsensitiveStringMap): Seq[String] = {
    val paths = Option(map.get("paths")).map { pathStr =>
      FileDataSourceV2.readPathsToSeq(pathStr)
    }.getOrElse(Seq.empty)
    paths ++ Option(map.get("path")).toSeq
  }
  //从 options 中移除 "path" 和 "paths" 配置项，返回一个不包含这些字段的新配置
  protected def getOptionsWithoutPaths(map: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val withoutPath = map.asCaseSensitiveMap().asScala.filterKeys { k =>
      !k.equalsIgnoreCase("path") && !k.equalsIgnoreCase("paths")
    }
    new CaseInsensitiveStringMap(withoutPath.toMap.asJava)
  }
  // 生成表名称
  // 根据数据源的简称 (shortName()) 和规范化后的文件路径列表，构建一个人类可读的表名。
  // 同时，使用 Utils.redact 机制对路径中的敏感信息进行脱敏处理
  protected def getTableName(map: CaseInsensitiveStringMap, paths: Seq[String]): String = {
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(
      map.asCaseSensitiveMap().asScala.toMap)
    val name = shortName() + " " + paths.map(qualifiedPathName(_, hadoopConf)).mkString(",")
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, name)
  }
  //将给定的文件路径转化为一个完整的、合格的路径
  private def qualifiedPathName(path: String, hadoopConf: Configuration): String = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
  }

  // TODO: To reduce code diff of SPARK-29665, we create stub implementations for file source v2, so
  //       that we don't need to touch all the file source v2 classes. We should remove the stub
  //       implementation and directly implement the TableProvider APIs.
  // 获取 Table 实例 (推断模式)
  // 抽象方法，强制子类实现。用于在 Spark 未提供 Schema 时，根据配置选项推断 Schema 并返回 Table 实例
  protected def getTable(options: CaseInsensitiveStringMap): Table

  //获取 Table 实例 (指定模式)。
  // 默认抛出“不支持”的错误。
  // 这个版本用于 Spark 已提供 Schema 的情况（例如用户使用 .schema(...) 指定）。
  // 子类如果支持用户提供的 Schema，应重写此方法
  protected def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    throw QueryExecutionErrors.unsupportedUserSpecifiedSchemaError()
  }

  override def supportsExternalMetadata(): Boolean = true
  // 缓存 Table 实例。
  // 用于在 inferSchema 期间临时存储推断出的 Table 实例，以便在紧随其后的 getTable 调用中重复使用，避免重复创建或推断
  private var t: Table = null

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    if (t == null) t = getTable(options)
    t.columns.asSchema
  }

  // TODO: implement a light-weight partition inference which only looks at the path of one leaf
  //       file and return partition column names. For now the partition inference happens in
  //       `getTable`, because we don't know the user-specified schema here.
  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] = {
    Array.empty
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    // If the table is already loaded during schema inference, return it directly.
    if (t != null) {
      t
    } else {
      getTable(new CaseInsensitiveStringMap(properties), schema)
    }
  }
}

private object FileDataSourceV2 {
  private lazy val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  // 用于将以 JSON 字符串形式传入的多个文件路径解析成一个 Scala Seq[String] 序列
  private def readPathsToSeq(paths: String): Seq[String] =
    objectMapper.readValue(paths, classOf[Seq[String]])
}
