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
package org.apache.spark.sql.execution.datasources.json

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType
// 核心作用是在 Spark 执行器（Executor）端，将 DataFrame 的数据行以 JSON 格式写入到目标文件系统
// 设计专注于以下方面：
// JSON 格式化： 它不直接操作字节，而是依赖 JacksonGenerator (基于 Jackson 库) 来处理复杂的 JSON 格式化和转义逻辑，确保 InternalRow 被正确地转换成 JSON 对象字符串
// 编码处理： 它严格遵守用户在 options 中指定的字符编码（如 UTF-8），确保写入的 JSON 文件编码正确。
// 单行记录（默认）： 默认情况下，它将每个 InternalRow 写入文件的一行，构成一个 JSON Lines (JSONL) 文件，这也是 Spark 默认的 JSON 读写模式，便于并行处理。

class JsonOutputWriter(
    val path: String, // 文件写入路径。
    options: JSONOptions, // JSON 配置选项 ， 用户为 JSON 数据源提供的配置选项，例如编码 (encoding)、日期格式、时间戳格式等。这些选项决定了 JSON 字符串的具体格式
    dataSchema: StructType, // 数据 Schema ， 待写入数据的结构信息。JsonOutputWriter 必须使用这个 Schema 来知道如何正确地将 InternalRow 的字段映射到 JSON 对象的键和类型
    context: TaskAttemptContext)
  extends OutputWriter with Logging {
  // 编码方式
  private val encoding = options.encoding match {
    case Some(charsetName) => Charset.forName(charsetName)
    case None => StandardCharsets.UTF_8
  }

  if (JSONOptionsInRead.denyList.contains(encoding)) {
    logWarning(s"The JSON file ($path) was written in the encoding ${encoding.displayName()}" +
      " which can be read back by Spark only if multiLine is enabled.")
  }

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path), encoding)

  // create the Generator without separator inserted between 2 records
  private[this] val gen = new JacksonGenerator(dataSchema, writer, options)

  override def write(row: InternalRow): Unit = {
    gen.write(row)
    gen.writeLineEnding()
  }

  override def close(): Unit = {
    gen.close()
    writer.close()
  }
}
