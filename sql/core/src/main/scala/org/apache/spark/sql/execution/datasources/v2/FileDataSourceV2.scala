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
//属于数据源 v2（DataSource V2）接口的实现，针对文件类型的数据源
trait FileDataSourceV2 extends TableProvider with DataSourceRegister {
  /**
   * Returns a V1 [[FileFormat]] class of the same file data source.
   * This is a solution for the following cases:
   * 1. File datasource V2 implementations cause regression. Users can disable the problematic data
   *    source via SQL configuration and fall back to FileFormat.
   * 2. Catalog support is required, which is still under development for data source V2.
   */
  //返回一个与当前文件数据源相同的 v1 FileFormat 类。这个属性用于处理一些特殊情况
  //1、当前的 v2 文件数据源导致了回归（例如性能下降），用户可以通过 SQL 配置禁用该数据源，并回退到 v1 的 FileFormat 实现
  //2、如果需要支持目录（Catalog），但 v2 还不完全支持该功能
  def fallbackFileFormat: Class[_ <: FileFormat]

  lazy val sparkSession = SparkSession.active
  //从 options 中获取文件路径。这个方法处理输入的配置 map，从中提取出 "paths" 或 "path" 字段并返回路径的序列
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
  //返回一个字符串，表示构建的表名称
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
  //返回一个 Table 实例，表示一个表的数据。这个方法根据给定的选项获取表的元数据并返回表实例
  protected def getTable(options: CaseInsensitiveStringMap): Table
  protected def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    throw QueryExecutionErrors.unsupportedUserSpecifiedSchemaError()
  }

  override def supportsExternalMetadata(): Boolean = true

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
  private def readPathsToSeq(paths: String): Seq[String] =
    objectMapper.readValue(paths, classOf[Seq[String]])
}
