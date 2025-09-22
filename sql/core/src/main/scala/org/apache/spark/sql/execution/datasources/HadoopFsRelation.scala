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

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.types.{StructField, StructType}


/**
 * Acts as a container for all of the metadata required to read from a datasource. All discovery,
 * resolution and merging logic for schemas and partitions has been removed.
 *
 * @param location A [[FileIndex]] that can enumerate the locations of all the files that
 *                 comprise this relation.
 * @param partitionSchema The schema of the columns (if any) that are used to partition the relation
 * @param dataSchema The schema of any remaining columns.  Note that if any partition columns are
 *                   present in the actual data files as well, they are preserved.
 * @param bucketSpec Describes the bucketing (hash-partitioning of the files by some column values).
 * @param fileFormat A file format that can be used to read and write the data in files.
 * @param options Configuration used when reading / writing data.
 */
//表示 基于 Hadoop 文件系统（HDFS、S3、Azure Blob 等）数据源 的核心类
//具备 Spark 读取和操作文件数据源的能力。该类包含了读取和写入文件所需的所有元数据，如文件路径、数据模式（Schema）、分区信息、存储格式等
case class HadoopFsRelation(
    location: FileIndex, //文件索引对象，包含此关系对应的所有文件的路径及元数据，负责枚举文件的实际存储位置
    partitionSchema: StructType, //分区列的 Schema，用于描述此关系中的分区字段，通常指基于目录路径的分区结构（如 /year=2023）
    // The top-level columns in `dataSchema` should match the actual physical file schema, otherwise
    // the ORC data source may not work with the by-ordinal mode.
    dataSchema: StructType, //数据列的 Schema，描述文件中存储的实际数据列的结构，不包含分区列
    bucketSpec: Option[BucketSpec], //分桶信息，如果存在，说明该数据源按某些列进行了 Hash 分桶，Spark 在分桶表中进行更高效的查询
    fileFormat: FileFormat,//文件格式，用于描述数据的物理存储格式（如 Parquet、ORC、CSV、JSON 等）
    options: Map[String, String])(val sparkSession: SparkSession)//读取/写入配置选项，如分隔符、压缩方式、是否多行解析等，通过 key-value 形式传递
  extends BaseRelation with FileRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  // When data and partition schemas have overlapping columns, the output
  // schema respects the order of the data schema for the overlapping columns, and it
  // respects the data types of the partition schema.
  //schema  完整的 Schema，由 dataSchema 和 partitionSchema 合并而成，保持数据列顺序和分区列类型
  //overlappedPartCols 重叠的分区列映射，如果数据文件中存在与分区列相同的字段，存储这些字段的映射信息
  val (schema: StructType, overlappedPartCols: Map[String, StructField]) =
    PartitioningUtils.mergeDataAndPartitionSchema(dataSchema,
      partitionSchema, sparkSession.sessionState.conf.caseSensitiveAnalysis)

  override def toString: String = {
    fileFormat match {
      case source: DataSourceRegister => source.shortName()
      case _ => "HadoopFiles"
    }
  }

  override def sizeInBytes: Long = {
    val compressionFactor = sqlContext.conf.fileCompressionFactor
    (location.sizeInBytes * compressionFactor).toLong
  }


  override def inputFiles: Array[String] = location.inputFiles
}
