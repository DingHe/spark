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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.util.SerializableConfiguration

/**  主要用于从文本文件中读取数据和将数据写入文本文件
 * A data source for reading text files. The text files must be encoded as UTF-8.
 */
class TextFileFormat extends TextBasedFileFormat with DataSourceRegister {
  //返回数据源的简短名称
  override def shortName(): String = "text"

  override def toString: String = "Text"
  //验证模式是否符合要求。由于文本文件每行数据只能表示一个值
  private def verifySchema(schema: StructType): Unit = {
    if (schema.size != 1) {
      throw QueryCompilationErrors.textDataSourceWithMultiColumnsError(schema)
    }
  }
  //如果是“wholeText”模式，则不可拆分
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    val textOptions = new TextOptions(options)
    super.isSplitable(sparkSession, options, path) && !textOptions.wholeText
  }
  //推断输入文件的模式。文本文件通常只有一列，这里返回一个模式，包含一个名为 "value" 的列，类型为 StringType
  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(new StructType().add("value", StringType))
  //为写操作做准备
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    verifySchema(dataSchema)  //检查模式是否符合要求（必须只有一列）

    val textOptions = new TextOptions(options)
    val conf = job.getConfiguration

    textOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new TextOutputWriter(path, dataSchema, textOptions.lineSeparatorInWrite, context)
      }
      //返回 .txt 后缀的文件扩展名，支持压缩扩展名
      override def getFileExtension(context: TaskAttemptContext): String = {
        ".txt" + CodecStreams.getCompressionExtension(context)
      }
    }
  }
  //创建读取器，用于从文本文件读取数据
  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    assert(
      requiredSchema.length <= 1,
      "Text data source only produces a single data column named \"value\".")
    val textOptions = new TextOptions(options)
    val broadcastedHadoopConf =  //将 Hadoop 配置广播到各个任务节点
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    readToUnsafeMem(broadcastedHadoopConf, requiredSchema, textOptions)
  }
  //根据文件选择合适的读取方式，读取文件中的文本行，并将其转换为 UnsafeRow 对象
  private def readToUnsafeMem(
      conf: Broadcast[SerializableConfiguration],
      requiredSchema: StructType,
      textOptions: TextOptions): (PartitionedFile) => Iterator[UnsafeRow] = {

    (file: PartitionedFile) => {
      val confValue = conf.value.value
      val reader = if (!textOptions.wholeText) {  //根据 wholeText 设置，选择逐行读取还是整个文件读取
        new HadoopFileLinesReader(file, textOptions.lineSeparatorInRead, confValue)
      } else {
        new HadoopFileWholeTextReader(file, confValue)
      }
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => reader.close()))
      if (requiredSchema.isEmpty) {  //将读取的文本行转换为 UnsafeRow 对象
        val emptyUnsafeRow = new UnsafeRow(0)
        reader.map(_ => emptyUnsafeRow)
      } else {
        val unsafeRowWriter = new UnsafeRowWriter(1)

        reader.map { line =>
          // Writes to an UnsafeRow directly
          unsafeRowWriter.reset()
          unsafeRowWriter.write(0, line.getBytes, 0, line.getLength)
          unsafeRowWriter.getRow()
        }
      }
    }
  }

  override def supportDataType(dataType: DataType): Boolean =
    dataType == StringType
}

