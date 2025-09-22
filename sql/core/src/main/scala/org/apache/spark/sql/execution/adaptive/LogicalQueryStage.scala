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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, RepartitionOperation, Statistics}
import org.apache.spark.sql.catalyst.trees.TreePattern.{LOGICAL_QUERY_STAGE, REPARTITION_OPERATION, TreePattern}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec

/**
 * The LogicalPlan wrapper for a [[QueryStageExec]], or a snippet of physical plan containing
 * a [[QueryStageExec]], in which all ancestor nodes of the [[QueryStageExec]] are linked to
 * the same logical node.
 *
 * For example, a logical Aggregate can be transformed into FinalAgg - Shuffle - PartialAgg, in
 * which the Shuffle will be wrapped into a [[QueryStageExec]], thus the [[LogicalQueryStage]]
 * will have FinalAgg - QueryStageExec as its physical plan.
 */ //与 QueryStageExec 相关的一个逻辑计划封装类，它将一个物理查询阶段（QueryStageExec）或包含 QueryStageExec 的物理查询片段包装为一个逻辑查询阶段
// TODO we can potentially include only [[QueryStageExec]] in this class if we make the aggregation
// planning aware of partitioning.
case class LogicalQueryStage(
    logicalPlan: LogicalPlan, //表示当前 LogicalQueryStage 的逻辑查询计划，通常是 Spark 解析的查询树的一个部分。它通常是经过优化后的逻辑计划，但未经过物理执行规划
    physicalPlan: SparkPlan) extends LeafNode { //表示当前 LogicalQueryStage 的物理查询计划，它是查询优化后生成的最终执行计划的一部分，通常包含一个或多个 QueryStageExec 类型的执行计划

  override def output: Seq[Attribute] = logicalPlan.output //返回逻辑计划的输出属性（即查询结果中的列）
  override val isStreaming: Boolean = logicalPlan.isStreaming
  override val outputOrdering: Seq[SortOrder] = physicalPlan.outputOrdering
  override protected val nodePatterns: Seq[TreePattern] = {
    // Repartition is a special node that it represents a shuffle exchange,
    // then in AQE the repartition will be always wrapped into `LogicalQueryStage`
    val repartitionPattern = logicalPlan match {
      case _: RepartitionOperation => Some(REPARTITION_OPERATION)
      case _ => None
    }
    Seq(LOGICAL_QUERY_STAGE) ++ repartitionPattern
  }
  //计算当前查询阶段的统计信息
  override def computeStats(): Statistics = {
    // TODO this is not accurate when there is other physical nodes above QueryStageExec.
    val physicalStats = physicalPlan.collectFirst {
      case a: BaseAggregateExec if a.groupingExpressions.isEmpty =>
        a.collectFirst {
          case s: QueryStageExec => s.computeStats()
        }.flatten.map { stat =>
          if (stat.rowCount.contains(0)) stat.copy(rowCount = Some(1)) else stat
        }
      case s: QueryStageExec => s.computeStats()
    }.flatten
    if (physicalStats.isDefined) {
      logDebug(s"Physical stats available as ${physicalStats.get} for plan: $physicalPlan")
    } else {
      logDebug(s"Physical stats not available for plan: $physicalPlan")
    }
    physicalStats.getOrElse(logicalPlan.stats)
  }

  override def maxRows: Option[Long] = stats.rowCount.map(_.min(Long.MaxValue).toLong)
}
