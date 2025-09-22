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

import scala.collection.mutable

import org.apache.spark.sql.{sources, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, DataSourceUtils, PartitioningAwareFileIndex, PartitioningUtils}
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
//处理文件格式的数据源时用于构建 Scan 的抽象类
abstract class FileScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType)
  extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownCatalystFilters {
  private val partitionSchema = fileIndex.partitionSchema  //存储文件索引 (fileIndex) 中的分区模式（Schema
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis  //指示 Spark 是否区分大小写
  protected val supportsNestedSchemaPruning = false  //表示该 FileScanBuilder 是否支持嵌套字段的裁剪（默认不支持）
  protected var requiredSchema = StructType(dataSchema.fields ++ partitionSchema.fields)  //存储查询所需的字段，包括数据字段 (dataSchema.fields) 和分区字段
  protected var partitionFilters = Seq.empty[Expression]  //存储针对分区列的过滤条件（由 Catalyst 表达式表示）
  protected var dataFilters = Seq.empty[Expression]  //存储针对数据列的过滤条件（Catalyst 表达式）
  protected var pushedDataFilters = Array.empty[Filter] //存储成功下推到文件数据源的过滤条件（转换为 V1 Filter）
  //裁剪查询所需的列，以减少数据扫描量
  override def pruneColumns(requiredSchema: StructType): Unit = {
    // [SPARK-30107] While `requiredSchema` might have pruned nested columns,
    // the actual data schema of this scan is determined in `readDataSchema`.
    // File formats that don't support nested schema pruning,
    // use `requiredSchema` as a reference and prune only top-level columns.
    this.requiredSchema = requiredSchema
  }
  //确定 Scan 需要读取的实际数据列
  protected def readDataSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val schema = if (supportsNestedSchemaPruning) requiredSchema else dataSchema
    val fields = schema.fields.filter { field =>
      //仅保留 requiredNameSet 里存在的字段，且不能是 partitionNameSet 里的字段（因为分区字段存储在路径中，不需要读取数据）
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName) && !partitionNameSet.contains(colName)
    }
    StructType(fields)
  }
  //确定 Scan 需要的分区字段
  def readPartitionSchema(): StructType = {
    val requiredNameSet = createRequiredNameSet()
    val fields = partitionSchema.fields.filter { field =>
      //过滤 partitionSchema 中的字段，仅保留 requiredSchema 里需要的字段
      val colName = PartitioningUtils.getColName(field, isCaseSensitive)
      requiredNameSet.contains(colName)
    }
    StructType(fields)
  }
  //解析 Catalyst 过滤条件，将其拆分为
  //分区过滤器 (partitionFilters)：直接用于查询优化，不需要下推到数据源
  //数据过滤器 (dataFilters)：可能会被下推到数据源以减少数据读取量
  override def pushFilters(filters: Seq[Expression]): Seq[Expression] = {
    val (deterministicFilters, nonDeterminsticFilters) = filters.partition(_.deterministic)
    val (partitionFilters, dataFilters) =
      DataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, deterministicFilters)
    this.partitionFilters = partitionFilters.filter { f =>
      // Python UDFs might exist because this rule is applied before ``ExtractPythonUDFs``.
      !SubqueryExpression.hasSubquery(f) && !f.exists(_.isInstanceOf[PythonUDF])
    }
    this.dataFilters = dataFilters
    val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
    for (filterExpr <- dataFilters) {
      val translated = DataSourceStrategy.translateFilter(filterExpr, true)
      if (translated.nonEmpty) {
        translatedFilters += translated.get
      }
    }
    pushedDataFilters = pushDataFilters(translatedFilters.toArray)
    dataFilters ++ nonDeterminsticFilters
  }

  override def pushedFilters: Array[Predicate] = pushedDataFilters.map(_.toV2)

  /*
   * Push down data filters to the file source, so the data filters can be evaluated there to
   * reduce the size of the data to be read. By default, data filters are not pushed down.
   * File source needs to implement this method to push down data filters.
   */
  protected def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = Array.empty[Filter]
  //返回需要的列，并根据是否区分大小写做转换
  private def createRequiredNameSet(): Set[String] =
    requiredSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet

  val partitionNameSet: Set[String] =
    partitionSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet
}
