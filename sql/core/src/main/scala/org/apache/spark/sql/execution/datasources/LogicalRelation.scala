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

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{ExposesMetadataColumns, LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.{truncatedString, CharVarcharUtils}
import org.apache.spark.sql.sources.BaseRelation

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
 */
//用于将 BaseRelation（基础关系）链接到逻辑查询计划的一个类。它继承自 LeafNode，并实现了多个特性接口，如 MultiInstanceRelation 和 ExposesMetadataColumns
case class LogicalRelation(
    relation: BaseRelation, //底层的 BaseRelation 实现，代表了数据源的具体实现，比如 HadoopFsRelation（HDFS 关系）
    output: Seq[AttributeReference], //查询计划中输出的列
    catalogTable: Option[CatalogTable], //表示数据源对应的 CatalogTable
    override val isStreaming: Boolean) //标识是否为流式数据源
  extends LeafNode with MultiInstanceRelation with ExposesMetadataColumns {

  // Only care about relation when canonicalizing.  对 output 中的表达式进行标准化
  override def doCanonicalize(): LogicalPlan = copy(
    output = output.map(QueryPlan.normalizeExpressions(_, output)),
    catalogTable = None)
  //首先尝试从 catalogTable 获取表的统计信息。如果有，使用它；否则，使用 relation 提供的字节大小（sizeInBytes）来创建默认的统计信息
  override def computeStats(): Statistics = {
    catalogTable
      .flatMap(_.stats.map(_.toPlanStats(output, conf.cboEnabled || conf.planStatsEnabled)))
      .getOrElse(Statistics(sizeInBytes = relation.sizeInBytes))
  }

  /** Used to lookup original attribute capitalization 查找输出属性的原始大小写*/
  val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))

  /**
   * Returns a new instance of this LogicalRelation. According to the semantics of
   * MultiInstanceRelation, this method returns a copy of this object with
   * unique expression ids. We respect the `expectedOutputAttributes` and create
   * new instances of attributes in it.
   */
  override def newInstance(): LogicalRelation = {
    this.copy(output = output.map(_.newInstance()))
  }
  //刷新数据源的元数据或文件系统位置
  override def refresh(): Unit = relation match {
    case fs: HadoopFsRelation => fs.location.refresh()  //只有hdfs需要刷新
    case _ =>  // Do nothing.
  }
  //化的字符串表示，通常用于日志或调试
  override def simpleString(maxFields: Int): String = {
    s"Relation ${catalogTable.map(_.identifier.unquotedString).getOrElse("")}" +
      s"[${truncatedString(output, ",", maxFields)}] $relation"
  }
  //用于输出元数据列。对于 HadoopFsRelation，它返回文件元数据列；否则返回空集合
  override lazy val metadataOutput: Seq[AttributeReference] = relation match {
    case relation: HadoopFsRelation =>
      metadataOutputWithOutConflicts(Seq(relation.fileFormat.createFileMetadataCol))
    case _ => Nil
  }

  override def withMetadataColumns(): LogicalRelation = {
    val newMetadata = metadataOutput.filterNot(outputSet.contains)
    if (newMetadata.nonEmpty) {
      val newRelation = this.copy(output = output ++ newMetadata)
      newRelation.copyTagsFrom(this)
      newRelation
    } else {
      this
    }
  }
}

object LogicalRelation {
  def apply(relation: BaseRelation, isStreaming: Boolean = false): LogicalRelation = {
    // The v1 source may return schema containing char/varchar type. We replace char/varchar
    // with "annotated" string type here as the query engine doesn't support char/varchar yet.
    val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(relation.schema)
    LogicalRelation(relation, toAttributes(schema), None, isStreaming)
  }

  def apply(relation: BaseRelation, table: CatalogTable): LogicalRelation = {
    // The v1 source may return schema containing char/varchar type. We replace char/varchar
    // with "annotated" string type here as the query engine doesn't support char/varchar yet.
    val schema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(relation.schema)
    LogicalRelation(relation, toAttributes(schema), Some(table), false)
  }
}
