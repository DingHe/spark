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

import java.io.{InputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.TaskContext
//主要用于处理文件的压缩和解压缩
object CodecStreams {
  //根据 Hadoop 的 Configuration 和文件路径 file，判断该文件是否使用了压缩编码，若使用了则返回对应的 CompressionCodec 对象，否则返回 None
  //config: Configuration：Hadoop 配置对象，包含文件系统和压缩格式相关的配置
  //file: Path：文件的路径，用于根据文件的扩展名匹配相应的压缩编码
  private def getDecompressionCodec(config: Configuration, file: Path): Option[CompressionCodec] = {
    val compressionCodecs = new CompressionCodecFactory(config)
    Option(compressionCodecs.getCodec(file))
  }
  //根据文件路径，自动检测是否有压缩编码，并返回一个相应的 InputStream 对象，如果有压缩则返回解压缩流，否则返回普通文件输入流
  def createInputStream(config: Configuration, file: Path): InputStream = {
    val fs = file.getFileSystem(config)
    val inputStream: InputStream = fs.open(file)

    getDecompressionCodec(config, file)
      .map(codec => codec.createInputStream(inputStream))
      .getOrElse(inputStream)
  }

  /**
   * Creates an input stream from the given path and add a closure for the input stream to be
   * closed on task completion.
   */
    //创建一个输入流，并在 Spark 任务完成后自动关闭该输入流，避免资源泄露
  def createInputStreamWithCloseResource(config: Configuration, path: Path): InputStream = {
    val inputStream = createInputStream(config, path)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => inputStream.close()))
    inputStream
  }
  //根据 JobContext 中的配置或文件扩展名，获取输出使用的压缩编码。
  private def getCompressionCodec(
      context: JobContext,
      file: Option[Path] = None): Option[CompressionCodec] = {
    if (FileOutputFormat.getCompressOutput(context)) {
      val compressorClass = FileOutputFormat.getOutputCompressorClass(
        context,
        classOf[GzipCodec])

      Some(ReflectionUtils.newInstance(compressorClass, context.getConfiguration))
    } else {
      file.flatMap { path =>
        val compressionCodecs = new CompressionCodecFactory(context.getConfiguration)
        Option(compressionCodecs.getCodec(path))
      }
    }
  }

  /**
   * Create a new file and open it for writing.
   * If compression is enabled in the [[JobContext]] the stream will write compressed data to disk.
   * An exception will be thrown if the file already exists.
   */
  //创建一个输出流，如果在 JobContext 中开启了压缩，会对输出流进行压缩包装
  def createOutputStream(context: JobContext, file: Path): OutputStream = {
    val fs = file.getFileSystem(context.getConfiguration)
    val outputStream: OutputStream = fs.create(file, false)

    getCompressionCodec(context, Some(file))
      .map(codec => codec.createOutputStream(outputStream))
      .getOrElse(outputStream)
  }
  //基于输出流创建一个字符流 OutputStreamWriter，用于写入文本数据
  def createOutputStreamWriter(
      context: JobContext,
      file: Path,
      charset: Charset = StandardCharsets.UTF_8): OutputStreamWriter = {
    new OutputStreamWriter(createOutputStream(context, file), charset)
  }

  /** Returns the compression codec extension to be used in a file name, e.g. ".gzip"). */
  //获取当前输出压缩编码的默认文件扩展名（如 .gzip、.snappy）
  def getCompressionExtension(context: JobContext): String = {
    getCompressionCodec(context)
      .map(_.getDefaultExtension)
      .getOrElse("")
  }
}
