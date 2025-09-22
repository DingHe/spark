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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final, PartialMerge}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{ExplainUtils, PartitioningPreservingUnaryExecNode, UnaryExecNode}
import org.apache.spark.sql.execution.streaming.StatefulOperatorPartitioning

/**
 * Holds common logic for aggregate operators
 */
//聚合操作的抽象类，主要用于描述聚合操作的共通逻辑
trait BaseAggregateExec extends UnaryExecNode with PartitioningPreservingUnaryExecNode {
  //表示子节点所需的分布式表达式，用于指定子节点数据的分布方式。如果该值为 Some，则表示有具体的分布要求
  def requiredChildDistributionExpressions: Option[Seq[Expression]]
  //表示当前聚合操作是否是在流式处理模式下执行。如果为 true，表示该聚合是在流式查询中执行的，反之则是批处理模式
  def isStreaming: Boolean
  //表示聚合操作可能会进行数据洗牌（shuffle）时的分区数
  def numShufflePartitions: Option[Int]
  //表示聚合操作中的分组表达式（即聚合的键）
  def groupingExpressions: Seq[NamedExpression]
  //表示聚合函数的表达式序列。每个聚合函数都是一个 AggregateExpression，可以是例如 SUM、COUNT 等
  def aggregateExpressions: Seq[AggregateExpression]
  //聚合表达式的输出属性，用于在执行计划中标识聚合结果
  def aggregateAttributes: Seq[Attribute]
  //表示输入缓冲区的初始偏移量。它通常与流式操作中的状态管理有关，帮助定位数据的处理状态
  def initialInputBufferOffset: Int
  //聚合操作的结果表达式。它包含了最终输出的字段，这些字段由聚合表达式的计算结果生成
  def resultExpressions: Seq[NamedExpression]

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("Keys", groupingExpressions)}
       |${ExplainUtils.generateFieldString("Functions", aggregateExpressions)}
       |${ExplainUtils.generateFieldString("Aggregate Attributes", aggregateAttributes)}
       |${ExplainUtils.generateFieldString("Results", resultExpressions)}
       |""".stripMargin
  }
  //用于获取聚合操作的输入属性。具体来说，
  // 首先检查聚合表达式的模式，如果是 Final 或 PartialMerge，则会返回子节点的输出去掉聚合函数的属性，
  // 并将聚合函数的缓冲区属性追加到返回值中。否则，直接返回子节点的输出属性
  protected def inputAttributes: Seq[Attribute] = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.contains(Final) || modes.contains(PartialMerge)) {
      // SPARK-31620: when planning aggregates, the partial aggregate uses aggregate function's
      // `inputAggBufferAttributes` as its output. And Final and PartialMerge aggregate rely on the
      // output to bind references for `DeclarativeAggregate.mergeExpressions`. But if we copy the
      // aggregate function somehow after aggregate planning, like `PlanSubqueries`, the
      // `DeclarativeAggregate` will be replaced by a new instance with new
      // `inputAggBufferAttributes` and `mergeExpressions`. Then Final and PartialMerge aggregate
      // can't bind the `mergeExpressions` with the output of the partial aggregate, as they use
      // the `inputAggBufferAttributes` of the original `DeclarativeAggregate` before copy. Instead,
      // we shall use `inputAggBufferAttributes` after copy to match the new `mergeExpressions`.
      val aggAttrs = inputAggBufferAttributes
      child.output.dropRight(aggAttrs.length) ++ aggAttrs
    } else {
      child.output
    }
  }

  private val inputAggBufferAttributes: Seq[Attribute] = {
    aggregateExpressions
      // there're exactly four cases needs `inputAggBufferAttributes` from child according to the
      // agg planning in `AggUtils`: Partial -> Final, PartialMerge -> Final,
      // Partial -> PartialMerge, PartialMerge -> PartialMerge.
      .filter(a => a.mode == Final || a.mode == PartialMerge)
      .flatMap(_.aggregateFunction.inputAggBufferAttributes)
  }

  protected val aggregateBufferAttributes: Seq[AttributeReference] = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
    AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
    AttributeSet(aggregateBufferAttributes) ++
    // it's not empty when the inputAggBufferAttributes is not equal to the aggregate buffer
    // attributes of the child Aggregate, when the child Aggregate contains the subquery in
    // AggregateFunction. See SPARK-31620 for more details.
    AttributeSet(inputAggBufferAttributes.filterNot(child.output.contains))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) =>
        if (isStreaming) {
          numShufflePartitions match {
            case Some(parts) =>
              StatefulOperatorPartitioning.getCompatibleDistribution(
                exprs, parts, conf) :: Nil

            case _ =>
              throw new IllegalStateException("Expected to set the number of partitions before " +
                "constructing required child distribution!")
          }
        } else {
          ClusteredDistribution(exprs) :: Nil
        }
      case None => UnspecifiedDistribution :: Nil
    }
  }

  /**
   * The corresponding [[SortAggregateExec]] to get same result as this node.
   */
  def toSortAggregate: SortAggregateExec = {
    SortAggregateExec(
      requiredChildDistributionExpressions, isStreaming, numShufflePartitions, groupingExpressions,
      aggregateExpressions, aggregateAttributes, initialInputBufferOffset, resultExpressions,
      child)
  }
}
