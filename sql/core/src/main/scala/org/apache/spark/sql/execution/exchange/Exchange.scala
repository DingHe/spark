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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Base class for operators that exchange data among multiple threads or processes.
 *
 * Exchanges are the key class of operators that enable parallelism. Although the implementation
 * differs significantly, the concept is similar to the exchange operator described in
 * "Volcano -- An Extensible and Parallel Query Evaluation System" by Goetz Graefe.
 */
// Apache Spark 物理执行计划中一个至关重要的基类，它标志着分布式计算和数据重组的边界
// Exchange 类在 Spark 物理查询计划（PhysicalPlan）中扮演的角色是数据交换操作的基类。
// 它的核心作用是实现 Spark 架构中的并行化和容错性，以及在不同 Stage 之间或不同执行器之间传输和重组数据
// 数据重分区 (Re-partitioning)： Exchange 操作通常伴随着 Shuffle 过程，用于将数据按照特定的分区策略（如哈希、范围、随机）重新分布到集群的各个执行器上
// Stage 边界： 在 Spark 的 DAGScheduler 中，Exchange 是划分 Stage 的主要边界。每当数据需要跨网络、跨执行器进行大规模重组时，就会插入一个 Exchange 节点，将查询计划划分为两个或更多个 Stage
// 基类抽象： 它是所有具体数据交换策略（如 ShuffleExchangeExec、BroadcastExchangeExec 等）的抽象父类，提供了它们共有的基本结构和属性
abstract class Exchange extends UnaryExecNode {
  //  重要。 它定义了 Exchange 节点完成数据交换后，其输出数据集的列结构（Schema）。
  //  对于 Exchange 而言，它只是重组数据，而不改变列的结构。因此，它的输出属性直接继承自其子节点（child.output）
  override def output: Seq[Attribute] = child.output  //表示子节点（child）的输出属性
  // 用于标识该节点的类型。它返回一个包含 EXCHANGE 枚举值的序列，允许 Spark 的规则和优化器快速识别和匹配所有 Exchange 类型的节点
  final override val nodePatterns: Seq[TreePattern] = Seq(EXCHANGE)
  // 用于在 Spark 打印执行计划（如 explain()）时，生成更详细的字符串表示。
  // 它在父类的参数基础上，额外添加了该节点的唯一 ID（plan_id=$id），方便调试时追踪具体的物理操作实例
  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[plan_id=$id]")
}

/**
 * A wrapper for reused exchange to have different output, because two exchanges which produce
 * logically identical output will have distinct sets of output attribute ids, so we need to
 * preserve the original ids because they're what downstream operators are expecting.
 */
// Spark 物理查询计划中的一个包装器（Wrapper）节点
// 核心作用是实现子查询或公共表表达式 (CTE) 结果的重用，从而避免重复计算和重复数据交换（Shuffle 或 Broadcast）
// 在 Spark SQL 的优化阶段（特别是 Subquery Elimination 和 Common Expression Elimination 规则），如果发现查询计划中多个部分依赖于相同的 Exchange 操作结果（即它们对同一份数据进行相同方式的 Shuffle 或 Broadcast），优化器就会：
// 只执行一次该 Exchange 操作（由其 child 节点表示）
// 在其他需要相同结果的地方，插入一个 ReusedExchangeExec 节点，指向已执行的 Exchange
// 为什么需要包装器？
// 尽管两个操作逻辑上产生相同的数据，但下游操作符期望的列属性 ID（ExprId） 集合可能不同
// child (被重用的 Exchange) 的输出有一套 ExprId
// ReusedExchangeExec 的下游操作期望另一套 ExprId
case class ReusedExchangeExec(override val output: Seq[Attribute], child: Exchange)
  extends LeafExecNode {
  // 直接委托给其子节点 child.supportsColumnar。如果被重用的操作支持列式执行（例如 Apache Arrow 或 Parquet），则该包装器也支持
  override def supportsColumnar: Boolean = child.supportsColumnar

  // Ignore this wrapper for canonicalizing.
  // 覆写规范化逻辑。在规范化（用于判断两个计划是否等价）时，ReusedExchangeExec 包装器本身应该被忽略。
  // 因此，它返回子节点 child 的规范化结果
  override def doCanonicalize(): SparkPlan = child.canonicalized
  // 实际的执行逻辑。它直接调用并返回 child.execute() 的结果。这实现了重用：不执行新的 Shuffle，而是获取已完成 Shuffle 的结果。
  def doExecute(): RDD[InternalRow] = {
    child.execute()
  }
  // 列式执行逻辑。它直接调用并返回 child.executeColumnar() 的结果
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar()
  }
  // 广播执行逻辑。它直接调用并返回 child.executeBroadcast() 的结果。用于重用 BroadcastExchangeExec 的广播变量
  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  // `ReusedExchangeExec` can have distinct set of output attribute ids from its child, we need
  // to update the attribute ids in `outputPartitioning` and `outputOrdering`.
  //定义了将 child.output 中的旧 ExprId 映射到 ReusedExchangeExec.output 中的新 ExprId 的逻辑。
  // 它通过一个 AttributeMap 存储映射关系，并提供一个 transform 函数，将表达式中的所有 Attribute 引用更新为下游期望的 ID
  private[sql] lazy val updateAttr: Expression => Expression = {
    val originalAttrToNewAttr = AttributeMap(child.output.zip(output))
    e => e.transform {
      case attr: Attribute => originalAttrToNewAttr.getOrElse(attr, attr)
    }
  }
  // 承 child 的分区策略，但需要更新其中的 Attribute ID。如果 child.outputPartitioning 是一个基于表达式的分区策略（如 HashPartitioning），
  // 则使用 updateAttr 函数更新该表达式中的所有 ExprId
  override def outputPartitioning: Partitioning = child.outputPartitioning match {
    case e: Expression => updateAttr(e).asInstanceOf[Partitioning]
    case other => other
  }
  // 继承 child 的排序策略，并使用 updateAttr 函数更新排序键中的所有 Attribute ID，以匹配 ReusedExchangeExec 的输出
  override def outputOrdering: Seq[SortOrder] = {
    child.outputOrdering.map(updateAttr(_).asInstanceOf[SortOrder])
  }

  override def verboseStringWithOperatorId(): String = {
    val reuse_op_str = ExplainUtils.getOpId(child)
    s"""
       |$formattedNodeName [Reuses operator id: $reuse_op_str]
       |${ExplainUtils.generateFieldString("Output", output)}
       |""".stripMargin
  }
}
