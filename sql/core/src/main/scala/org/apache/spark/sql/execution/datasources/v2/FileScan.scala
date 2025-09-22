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
package org.apache.spark.sql.execution.datasources.v2

import java.util.{Locale, OptionalLong}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, ExpressionSet}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.read.{Batch, InputPartition, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
//Batch：表示批量读取数据的能力

trait FileScan extends Scan
  with Batch with SupportsReportStatistics with SupportsMetadata with Logging {
  /**
   * Returns whether a file with `path` could be split or not.
   */
    //判断文件是否可被拆分（默认不可拆分）
    //一些格式（如 Parquet）支持拆分，而一些（如 gzip 压缩的文本文件）不支持拆分
  def isSplitable(path: Path): Boolean = {
    false
  }

  def sparkSession: SparkSession

  def fileIndex: PartitioningAwareFileIndex

  def dataSchema: StructType

  /**
   * Returns the required data schema
   */
  def readDataSchema: StructType

  /**
   * Returns the required partition schema
   */
  def readPartitionSchema: StructType

  /**
   * Returns the filters that can be use for partition pruning
   */
  def partitionFilters: Seq[Expression]

  /**
   * Returns the data filters that can be use for file listing
   */
  def dataFilters: Seq[Expression]

  /**
   * If a file with `path` is unsplittable, return the unsplittable reason,
   * otherwise return `None`.
   */
    //如果文件不可拆分，则返回原因（默认值 "undefined"）
  def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    "undefined"
  }

  protected def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")

  private lazy val (normalizedPartitionFilters, normalizedDataFilters) = {
    val partitionFilterAttributes = AttributeSet(partitionFilters).map(a => a.name -> a).toMap
    val normalizedPartitionFilters = ExpressionSet(partitionFilters.map(
      QueryPlan.normalizeExpressions(_, toAttributes(fileIndex.partitionSchema)
        .map(a => partitionFilterAttributes.getOrElse(a.name, a)))))
    val dataFiltersAttributes = AttributeSet(dataFilters).map(a => a.name -> a).toMap
    val normalizedDataFilters = ExpressionSet(dataFilters.map(
      QueryPlan.normalizeExpressions(_, toAttributes(dataSchema)
        .map(a => dataFiltersAttributes.getOrElse(a.name, a)))))
    (normalizedPartitionFilters, normalizedDataFilters)
  }

  override def equals(obj: Any): Boolean = obj match {
    case f: FileScan =>
      fileIndex == f.fileIndex && readSchema == f.readSchema &&
        normalizedPartitionFilters == f.normalizedPartitionFilters &&
        normalizedDataFilters == f.normalizedDataFilters

    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()
  //获取元数据字符串的最大长度，用于日志输出时避免过长的字符串影响可读性
  val maxMetadataValueLength = sparkSession.sessionState.conf.maxMetadataStringLength
  //生成当前 FileScan 的描述信息
  override def description(): String = {
    val metadataStr = getMetaData().toSeq.sorted.map {
      case (key, value) =>
        val redactedValue =
          Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, value)
        key + ": " + StringUtils.abbreviate(redactedValue, maxMetadataValueLength)
    }.mkString(", ")
    s"${this.getClass.getSimpleName} $metadataStr"
  }
  //返回 FileScan 相关的元数据信息
  override def getMetaData(): Map[String, String] = {
    val locationDesc =
      fileIndex.getClass.getSimpleName +
        Utils.buildLocationMetadata(fileIndex.rootPaths, maxMetadataValueLength)
    Map(
      "Format" -> s"${this.getClass.getSimpleName.replace("Scan", "").toLowerCase(Locale.ROOT)}",
      "ReadSchema" -> readDataSchema.catalogString,
      "PartitionFilters" -> seqToString(partitionFilters),
      "DataFilters" -> seqToString(dataFilters),
      "Location" -> locationDesc)
  }
  //生成文件的 分区信息
  protected def partitions: Seq[FilePartition] = {
    //获取选择的分区
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    //计算出每个文件分区允许的最大字节数（即最大拆分大小）
    val maxSplitBytes = FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
    val partitionAttributes = toAttributes(fileIndex.partitionSchema)
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    //获取需要读取的分区字段，并且确保每个字段在 attributeMap 中存在。如果某个字段没有找到，则抛出异常
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(normalizeName(readField.name),
        throw QueryCompilationErrors.cannotFindPartitionColumnInPartitionSchemaError(
          readField, fileIndex.partitionSchema)
      )
    }
    //生成投影函数
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    //拆分文件
    val splitFiles = selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }
      partition.files.flatMap { file =>
        PartitionedFileUtil.splitFiles(
          sparkSession = sparkSession,
          file = file,
          isSplitable = isSplitable(file.getPath),
          maxSplitBytes = maxSplitBytes,
          partitionValues = partitionValues
        )
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
    //如果拆分后的文件块数量只有一个，并且该文件是不可拆分的（即 isSplitable 为 false），
    // 并且文件大小超过了配置阈值（IO_WARNING_LARGEFILETHRESHOLD），则会发出警告，表示这是一个大文件，且无法拆分
    if (splitFiles.length == 1) {
      val path = splitFiles(0).toPath
      if (!isSplitable(path) && splitFiles(0).length >
        sparkSession.sparkContext.getConf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
        logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
          s"partition, the reason is: ${getFileUnSplittableReason(path)}")
      }
    }

    FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
  }
  //算输入的分区信息，并返回 InputPartition 数组
  override def planInputPartitions(): Array[InputPartition] = {
    partitions.toArray
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val compressionFactor = sparkSession.sessionState.conf.fileCompressionFactor
        val size = (compressionFactor * fileIndex.sizeInBytes /
          (dataSchema.defaultSize + fileIndex.partitionSchema.defaultSize) *
          (readDataSchema.defaultSize + readPartitionSchema.defaultSize)).toLong

        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
  }

  override def toBatch: Batch = this
  //读取扫描数据的 Schema，由数据字段 (readDataSchema) 和分区字段 (readPartitionSchema) 组成
  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields)

  // Returns whether the two given arrays of [[Filter]]s are equivalent.
  protected def equivalentFilters(a: Array[Filter], b: Array[Filter]): Boolean = {
    a.sortBy(_.hashCode()).sameElements(b.sortBy(_.hashCode()))
  }
  //指定当前 Spark 任务是否区分大小写，影响字段匹配的行为
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }
}
