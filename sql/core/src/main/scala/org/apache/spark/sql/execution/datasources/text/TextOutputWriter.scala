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
package org.apache.spark.sql.execution.datasources.text

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType
// 核心作用是将 DataFrame 的数据行以纯文本（Plain Text）格式写入到 Hadoop 文件系统（HDFS 或兼容的文件系统，如 S3）
// 设计遵循以下关键原则：
// 单列限制： Spark SQL 的文本数据源通常要求写入的 DataFrame 只有一列（该列通常为 StringType），因为文本文件本身不支持复杂的结构化数据。TextOutputWriter 假设它只接收一个字段（索引为 0）进行写入。
// 数据转换与写入： 它负责将 Spark 内部高效的 InternalRow 格式中的第一个字段（UTF8String）提取出来，并将其字节直接写入底层的输出流。
// 分隔符处理： 写入每行数据后，它会追加一个换行符（lineSeparator），从而实现标准的行分隔文本文件格式。
class TextOutputWriter(
    val path: String, // 文件写入路径。
    dataSchema: StructType, // 数据 Schema ，待写入数据的结构信息。在 TextOutputWriter 的实现中，虽然接收了这个参数，但其内部写入逻辑（基于单列）并未直接使用完整的 Schema
    lineSeparator: Array[Byte], // 行分隔符字节数组。定义了写入每行数据后追加的分隔符（例如 \n 或 \r\n）的字节表示。这是从 Spark 配置或用户选项中获取的
    context: TaskAttemptContext)
  extends OutputWriter {
  // 底层的输出流。
  // 这是实际进行 I/O 操作的 Java/Hadoop 输出流。它通过 CodecStreams.createOutputStream 创建，该方法会根据 context 中的配置（例如是否启用压缩）智能地返回一个普通的输出流或一个压缩流（如 Gzip, Snappy）
  private val writer = CodecStreams.createOutputStream(context, new Path(path))

  override def write(row: InternalRow): Unit = {
    if (!row.isNullAt(0)) {
      val utf8string = row.getUTF8String(0)
      utf8string.writeTo(writer)
    }
    writer.write(lineSeparator)
  }

  override def close(): Unit = {
    writer.close()
  }
}
