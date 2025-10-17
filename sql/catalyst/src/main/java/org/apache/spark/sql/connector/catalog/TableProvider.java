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

package org.apache.spark.sql.connector.catalog;

import java.util.Map;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The base interface for v2 data sources which don't have a real catalog. Implementations must
 * have a public, 0-arg constructor.
 * <p>
 * Note that, TableProvider can only apply data operations to existing tables, like read, append,
 * delete, and overwrite. It does not support the operations that require metadata changes, like
 * create/drop tables.
 * <p>
 * The major responsibility of this interface is to return a {@link Table} for read/write.
 * </p>
 *
 * @since 3.0.0
 */
// TableProvider 是 Spark DataSource V2 API 中用于实现不具备自身元数据目录（Catalog）的数据源的基础接口。
// 它通常用于读取和写入文件系统数据（如 Parquet、CSV）或 流数据源（如 Kafka），这些数据源的“表”是由一组配置选项（例如文件路径、主题名称）临时定义的。
// 核心职责：
// 模式和分区推断： 负责根据用户提供的选项（如路径）自动推断出数据的 Schema（模式）和 Partitioning（分区信息）
// 创建 Table 实例： 核心目标是根据推断或外部提供的元数据（Schema、Partitioning、Properties）创建一个具体的 Table 实例。这个 Table 实例才是真正执行读取（Read）和写入（Write）等数据操作的对象。
// 限制 DDL 操作： 该接口明确不支持需要更改元数据的操作，例如 CREATE TABLE 或 DROP TABLE。它主要支持针对现有数据的数据操作（DML/DQL），如读取、追加、删除和覆盖

@Evolving
public interface TableProvider {

  /**
   * Infer the schema of the table identified by the given options.
   *
   * @param options an immutable case-insensitive string-to-string map that can identify a table,
   *                e.g. file path, Kafka topic name, etc.
   */
  //推断模式
  // 数据源 Schema 发现逻辑的关键方法。
  // 接收一个 options 映射（通常包含文件路径、格式选项等），并返回该数据源的底层数据的 StructType 模式信息
  StructType inferSchema(CaseInsensitiveStringMap options);

  /**
   * Infer the partitioning of the table identified by the given options.
   * <p>
   * By default this method returns empty partitioning, please override it if this source support
   * partitioning.
   *
   * @param options an immutable case-insensitive string-to-string map that can identify a table,
   *                e.g. file path, Kafka topic name, etc.
   */
  //推断分区
  // 默认返回一个空的 Transform 数组（即 new Transform[0]），表示不推断分区
  default Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return new Transform[0];
  }

  /**
   * Return a {@link Table} instance with the specified table schema, partitioning and properties
   * to do read/write. The returned table should report the same schema and partitioning with the
   * specified ones, or Spark may fail the operation.
   *
   * @param schema The specified table schema.
   * @param partitioning The specified table partitioning.
   * @param properties The specified table properties. It's case preserving (contains exactly what
   *                   users specified) and implementations are free to use it case sensitively or
   *                   insensitively. It should be able to identify a table, e.g. file path, Kafka
   *                   topic name, etc.
   */
  // 获取 Table 实例（核心）
  // 接收确定的 Schema、Partitioning 和 Properties，并基于这些信息创建一个具体的 Table 实例。
  // 这个返回的 Table 对象是执行实际的 Read 或 Write 操作的 V2 API 句柄
  Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties);

  /**
   * Returns true if the source has the ability of accepting external table metadata when getting
   * tables. The external table metadata includes:
   * <ol>
   *   <li>For table reader: user-specified schema from {@code DataFrameReader}/{@code
   *   DataStreamReader} and schema/partitioning stored in Spark catalog.</li>
   *   <li>For table writer: the schema of the input {@code Dataframe} of
   *   {@code DataframeWriter}/{@code DataStreamWriter}.</li>
   * </ol>
   * <p>
   * By default this method returns false, which means the schema and partitioning passed to
   * {@link #getTable(StructType, Transform[], Map)} are from the infer methods. Please override it
   * if this source has expensive schema/partitioning inference and wants external table metadata
   * to avoid inference.
   */
  // 支持外部元数据。
  // 这是一个 default 方法，默认返回 false。
  // 如果返回 true，则表示该数据源能够接受外部提供的表元数据（例如用户在 DataFrameReader 中指定的 Schema 或 Spark Catalog 中存储的 Schema/Partitioning），
  // 从而避免执行昂贵的 inferSchema 和 inferPartitioning 推断过程
  default boolean supportsExternalMetadata() {
    return false;
  }
}
