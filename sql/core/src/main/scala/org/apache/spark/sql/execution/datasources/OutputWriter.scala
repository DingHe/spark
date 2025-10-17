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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType


/**
 * A factory that produces [[OutputWriter]]s.  A new [[OutputWriterFactory]] is created on driver
 * side for each write job issued when writing to a [[HadoopFsRelation]], and then gets serialized
 * to executor side to create actual [[OutputWriter]]s on the fly.
 */
//Spark SQL 在将 DataFrame 数据写入到文件系统（尤其是基于 HadoopFsRelation 的数据源，如 Parquet、ORC、CSV 等）时使用的核心机制
//核心作用可以概括为：
//负责实例化写入器： 它的主要职责是在执行器（Executor）端为每个写入任务（Task）创建一个具体的 OutputWriter 实例。OutputWriter 才是真正执行 I/O、将数据行写入文件的对象。
//实现序列化： OutputWriterFactory 本身是在 Driver 端创建的，然后需要被序列化并发送到各个 Executor 上。这使得每个 Task 都能独立地使用这个工厂来创建自己的写入器，确保写入过程在分布式环境中正确地进行。
//提供文件信息： 它还负责提供写入文件所需的基本元数据，例如最终文件的扩展名。
abstract class OutputWriterFactory extends Serializable {

  /** Returns the file extension to be used when writing files out. */
  // 获取写入文件的扩展名
  // 返回一个字符串，表示写入文件时应使用的文件扩展名（例如：Parquet 格式返回 ".parquet"，CSV 格式返回 ".csv"）。
  // 该扩展名会被 Spark 用于构造最终的文件名。
  // 传入 TaskAttemptContext 允许扩展名依赖于某些运行时配置或任务信息。
  def getFileExtension(context: TaskAttemptContext): String

  /**
   * When writing to a [[HadoopFsRelation]], this method gets called by each task on executor side
   * to instantiate new [[OutputWriter]]s.
   *
   * @param path Path to write the file.
   * @param dataSchema Schema of the rows to be written. Partition columns are not included in the
   *        schema if the relation being written is partitioned.
   * @param context The Hadoop MapReduce task context.
   */
  // 在执行器上创建新的写入器实例。
  //在每个写入任务（Task）启动时被调用，以实例化一个具体的 OutputWriter 对象，该对象将负责写入该任务分配到的数据。
  // 参数说明： 1. path：文件写入路径，通常是一个临时路径或最终的文件路径。
  // 2. dataSchema：待写入数据的 Schema。值得注意的是，如果写入的是分区表，该 Schema 不包含分区列，只包含实际写入到文件内部的数据列。
  // 3. context：Hadoop 任务上下文 (TaskAttemptContext)。
  // 提供必要的运行时信息，如配置、任务 ID 等，供 OutputWriter 在初始化和写入文件时使用
  def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter
}


/**
 * [[OutputWriter]] is used together with [[HadoopFsRelation]] for persisting rows to the
 * underlying file system.  Subclasses of [[OutputWriter]] must provide a zero-argument constructor.
 * An [[OutputWriter]] instance is created and initialized when a new output file is opened on
 * executor side.  This instance is used to persist rows to this single output file.
 */
// OutputWriter（输出写入器）是 Spark SQL 在将 DataFrame 数据写入到文件系统时，在执行器（Executor）端工作的具体数据写入对象
// 核心作用可以概括为：
// 具体 I/O 执行者： OutputWriter 的子类（例如 ParquetOutputWriter、OrcOutputWriter 等）封装了特定文件格式的写入逻辑。它负责将 Spark 内部的行格式 (InternalRow) 转换为该文件格式所需的物理存储结构，并写入到单个文件中
// 单文件生命周期管理： 每个 OutputWriter 实例都只负责写入一个输出文件。它的生命周期从文件被打开时开始，到所有行写入完毕并调用 close() 时结束
// 支持分区写入： 它与 HadoopFsRelation 配合使用，在写入动态分区表时，它接收到的行数据 (InternalRow) 中不包含动态分区列的值，因为它只写入文件内部的数据
abstract class OutputWriter {
  /**
   * Persists a single row. Invoked on the executor side. When writing to dynamically partitioned
   * tables, dynamic partition columns are not included in rows to be written.
   */
  // 写入单行数据
  // 这是写入器的核心方法。它在 Executor 端被反复调用，以持久化处理结果的每一行数据。它接收一个 InternalRow 对象，这是 Spark 内部高效的行表示格式。
  // 具体的实现类会将这个 InternalRow 转换为目标文件格式（如 Parquet 记录、ORC 结构等）并写入缓冲区或磁盘
  def write(row: InternalRow): Unit

  /**
   * Closes the [[OutputWriter]]. Invoked on the executor side after all rows are persisted, before
   * the task output is committed.
   */
  // 关闭写入器并完成文件写入。
  def close(): Unit

  /**
   * The file path to write. Invoked on the executor side.
   */
  // 获取当前写入器正在写入的文件路径。
  def path(): String
}
