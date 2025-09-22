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
package org.apache.spark.sql.execution.datasources.v2.json

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.json.JsonDataSource
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
//用于支持 Spark 读取和写入 JSON 格式的数据。
// 它封装了 JSON 文件的 Schema 解析、扫描、写入等逻辑，属于 Spark 数据源 V2（Data Source V2）框架的一部分
case class JsonTable(
    name: String,  //表名称
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,  //选项参数（如 multiline、mode 等 JSON 读取选项）
    paths: Seq[String],   //需要读取的 JSON 文件路径
    userSpecifiedSchema: Option[StructType],  //用户指定的 Schema（如果用户未提供，将通过 inferSchema 进行推断）
    fallbackFileFormat: Class[_ <: FileFormat])  //允许回退到 V1 FileFormat（如 JsonFileFormat）
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {
  override def newScanBuilder(options: CaseInsensitiveStringMap): JsonScanBuilder = {
    //JsonScanBuilder 负责执行扫描逻辑
    new JsonScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
  }

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = new JSONOptionsInRead(
      options.asScala.toMap,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    JsonDataSource(parsedOptions).inferSchema(
      sparkSession, files, parsedOptions)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write = JsonWrite(paths, formatName, supportsDataType, info)
    }

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsDataType(keyType) && supportsDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _: NullType => true

    case _ => false
  }

  override def formatName: String = "JSON"
}
