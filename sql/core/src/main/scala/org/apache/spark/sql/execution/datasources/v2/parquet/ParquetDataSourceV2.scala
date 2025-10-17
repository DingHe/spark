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
package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
// ParquetDataSourceV2 类作为 Parquet 文件格式在 Spark SQL V2 架构中的入口点（Entry Point）。
// 核心职责：
// Parquet 注册： 通过实现 shortName() 方法，将自身注册为 Spark SQL 中的 "parquet" 数据源，允许用户通过 spark.read.format("parquet") 来使用它。
// 创建 ParquetTable： 核心作用是根据用户提供的选项（如文件路径、配置参数），创建和返回一个代表 Parquet 数据集逻辑定义的 ParquetTable 实例。
// 这个 ParquetTable 才是实际负责执行 Parquet 文件读取和写入逻辑的对象。
// V1 回退支持： 明确指定了 V1 版本的 Parquet 文件格式类 (ParquetFileFormat)，以支持在 V2 出现问题或被禁用时，能够平稳回退到 V1 版本的实现
class ParquetDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"
  // 获取 Table 实例 (Schema 推断模式)
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    ParquetTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }
  // 获取 Table 实例 (指定 Schema 模式)
  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    ParquetTable(
      tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }
}

