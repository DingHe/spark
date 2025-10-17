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

package org.apache.spark.sql.hive.execution

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.ql.plan.FileSinkDesc
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{JobConf, Reporter}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SPECULATION_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.hive.{HiveInspectors, HiveTableUtil}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableJobConf

/**
 * `FileFormat` for writing Hive tables.
 *
 * TODO: implement the read logic.
 */
//HiveFileFormat 是 Spark SQL 集成 Hive 的重要组成部分，它的核心作用是作为 适配器，允许 Spark SQL 使用 Hive 自身的 SerDe（序列化/反序列化）和文件格式机制来写入数据。
//统一写入接口： 它实现了 Spark 的 FileFormat 特质，将 Hive 的写入逻辑（基于 HiveOutputFormat 和 SerDe）适配到 Spark 的写入框架中。
//配置 Hive 写入： 在写入准备阶段，它负责从 Hive 的元数据配置对象（FileSinkDesc）中提取信息（如 SerDe 类名、输出格式类名），并将其设置到 Hadoop 的 JobConf 中，指导 Hive 写入过程。
//创建 Hive 写入器： 它创建了一个 OutputWriterFactory，用于在执行器端实例化具体的 HiveOutputWriter，以执行行数据的格式转换和文件 I/O。
// fileSinkConf Hive 写入配置
class HiveFileFormat(fileSinkConf: FileSinkDesc)
  extends FileFormat with DataSourceRegister with Logging {

  def this() = this(null)

  override def shortName(): String = "hive"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    throw QueryExecutionErrors.inferSchemaUnsupportedForHiveError()
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    val conf = job.getConfiguration
    val tableDesc = fileSinkConf.getTableInfo
    conf.set("mapred.output.format.class", tableDesc.getOutputFileFormatClassName)

    // When speculation is on and output committer class name contains "Direct", we should warn
    // users that they may loss data if they are using a direct output committer.
    val speculationEnabled = sparkSession.sparkContext.conf.get(SPECULATION_ENABLED)
    val outputCommitterClass = conf.get("mapred.output.committer.class", "")
    if (speculationEnabled && outputCommitterClass.contains("Direct")) {
      val warningMessage =
        s"$outputCommitterClass may be an output committer that writes data directly to " +
          "the final location. Because speculation is enabled, this output committer may " +
          "cause data loss (see the case in SPARK-10063). If possible, please use an output " +
          "committer that does not have this behavior (e.g. FileOutputCommitter)."
      logWarning(warningMessage)
    }

    // Add table properties from storage handler to hadoopConf, so any custom storage
    // handler settings can be set to hadoopConf
    HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, conf, false)
    Utilities.copyTableJobPropertiesToConf(tableDesc, conf)

    // Avoid referencing the outer object.
    val fileSinkConfSer = fileSinkConf
    new OutputWriterFactory {
      private val jobConf = new SerializableJobConf(new JobConf(conf))
      @transient private lazy val outputFormat =
        jobConf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

      override def getFileExtension(context: TaskAttemptContext): String = {
        Utilities.getFileExtension(jobConf.value, fileSinkConfSer.getCompressed, outputFormat)
      }

      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new HiveOutputWriter(path, fileSinkConfSer, jobConf.value, dataSchema)
      }
    }
  }

  override def supportFieldName(name: String): Boolean = {
    fileSinkConf.getTableInfo.getOutputFileFormatClassName match {
      case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" =>
        !name.matches(".*[ ,;{}()\n\t=].*")
      case "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat" =>
        try {
          TypeInfoUtils.getTypeInfoFromTypeString(s"struct<$name:int>")
          true
        } catch {
          case _: IllegalArgumentException => false
        }
      case _ => true
    }
  }
}
// HiveOutputWriter 是 Spark Hive 集成模块的一部分，它实现了 OutputWriter 抽象类，
// 专门负责利用 Hive SerDe（序列化/反序列化）框架和 Hadoop RecordWriter 机制将 Spark 的内部行数据 (InternalRow) 写入到 Hive 表所定义的底层文件格式中（如 TextFile, SequenceFile, 或自定义的 SerDe 格式）
//核心职责是：
//数据桥接： 将 Spark SQL 的内部数据结构（InternalRow 和 Catalyst 类型）转换为 Hive SerDe 所需的 Java 对象或 Writable 对象
// 格式适配： 利用 FileSinkDesc 和 TableInfo 中定义的 Hive SerDe 和文件格式信息，调用底层的 Hive 写入逻辑 (HiveFileFormatUtils.getHiveRecordWriter) 来执行实际的文件 I/O
// 类型检查： 继承 HiveInspectors 特质，使用 Hive 的 ObjectInspector 体系来指导数据转换和序列化。
// 简而言之，它允许 Spark 像 Hive 一样，以 Hive Catalog 中定义的原生格式来写入数据

class HiveOutputWriter(
    val path: String, // 文件写入路径
    fileSinkConf: FileSinkDesc, // Hive 文件写入配置，包含 Hive SerDe 的配置信息、输出格式类 (OutputFormat) 以及目标表的元数据信息（通过 TableInfo 获取）
    jobConf: JobConf, // Hadoop Job 配置。
    dataSchema: StructType)  // Spark 要写入的行的结构信息。用于指导 Spark Catalyst 类型到 Hive 类型的转换
  extends OutputWriter with HiveInspectors {
  // 从 fileSinkConf 中获取 Hive 表的元数据信息 (TableInfo)，包含了 SerDe 类名、属性等
  private def tableDesc = fileSinkConf.getTableInfo
  //Hive 序列化器实例。
  //根据 tableDesc 定义的 SerDe 类名反射创建的 Hive 序列化器。它负责将行对象（Java Object 数组）转换为 Hive 底层文件所需的可写对象 (Writable)。
  private val serializer = {
    val serializer = tableDesc.getDeserializerClass.getConstructor().
      newInstance().asInstanceOf[Serializer]
    serializer.initialize(jobConf, tableDesc.getProperties)
    serializer
  }

  //Hive 记录写入器。
  //调用 HiveFileFormatUtils.getHiveRecordWriter 创建的底层写入机制。它结合 jobConf、tableDesc 和 serializer，负责将序列化后的数据对象实际写入文件系统。
  private val hiveWriter = HiveFileFormatUtils.getHiveRecordWriter(
    jobConf,
    tableDesc,
    serializer.getSerializedClass,
    fileSinkConf,
    new Path(path),
    Reporter.NULL)

  /**
   * Since SPARK-30201 ObjectInspectorCopyOption.JAVA change to ObjectInspectorCopyOption.DEFAULT.
   * The reason is DEFAULT option can convert `UTF8String` to `Text` with bytes and
   * we can compatible with non UTF-8 code bytes during write.
   */
  //标准化结构体对象检查器。
  //在 Hive 的运行时数据模型中，基本数据类型可以有两种常见的存储形式：
  //Java 原生对象（JAVA）： 如 java.lang.Integer, java.lang.String 等，通常用于 Java UDFs 或直接的内存操作
  //Hadoop Writable 对象（WRITABLE）： 如 IntWritable, Text 等，它们是 Hadoop/MapReduce 框架中用于序列化和网络传输的标准格式。
  // 默认选项。 当选择 DEFAULT 时，Hive 运行时环境会根据当前的上下文（例如操作符类型、配置等）来自动选择最有效率的内部表示方式，可能是 JAVA，也可能是 WRITABLE。它将性能和效率放在首位。
  private val standardOI = ObjectInspectorUtils
    .getStandardObjectInspector(
      tableDesc.getDeserializer(jobConf).getObjectInspector,
      ObjectInspectorCopyOption.DEFAULT)
    .asInstanceOf[StructObjectInspector]
  //字段对象检查器数组。
  //提取 standardOI 中的每个字段（列）对应的 ObjectInspector。这些检查器指导 Spark 如何将对应列的 Catalyst 数据转换为 Hive 兼容数据
  private val fieldOIs =
    standardOI.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
  //Catalyst 数据类型数组
  private val dataTypes = dataSchema.map(_.dataType).toArray
  //这是一个函数数组，由 fieldOIs 和 dataTypes 映射生成。每个函数 (wrapperFor) 负责将一个 Catalyst 值（如 UTF8String）封装成 Hive SerDe 期望的 Java/Writable 对象（例如 Text 或 String）
  private val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }
  //用于在 write 方法中存储封装后的 Hive 兼容数据对象（Java/Writable），然后将整个数组传递给 serializer.serialize
  private val outputData = new Array[Any](fieldOIs.length)

  override def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fieldOIs.length) {
      outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row.get(i, dataTypes(i)))
      i += 1
    }
    hiveWriter.write(serializer.serialize(outputData, standardOI))
  }

  override def close(): Unit = {
    // Seems the boolean value passed into close does not matter.
    hiveWriter.close(false)
  }
}
