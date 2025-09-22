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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.trees.UnaryLike

trait DynamicPruning extends Predicate

/**
 * The DynamicPruningSubquery expression is only used in join operations to prune one side of the
 * join with a filter from the other side of the join. It is inserted in cases where partition
 * pruning can be applied.
 *
 * @param pruningKey the filtering key of the plan to be pruned.
 * @param buildQuery the build side of the join.
 * @param buildKeys the join keys corresponding to the build side of the join
 * @param onlyInBroadcast when set to false it indicates that the pruning filter is likely to be
 *  beneficial and so it should be executed even if it cannot reuse the results of the
 *  broadcast through ReuseExchange; otherwise, it will use the filter only if it
 *  can reuse the results of the broadcast through ReuseExchange
 * @param broadcastKeyIndex the index of the filtering key collected from the broadcast
 */
case class DynamicPruningSubquery(
    pruningKey: Expression, //表示用于过滤的关键字，它是一个表达式，用于根据某些条件从查询中剔除不必要的数据。在动态分区裁剪（Dynamic Partition Pruning）中，它是用来决定如何裁剪数据的关键
    buildQuery: LogicalPlan, //JOIN操作中“build”侧的逻辑查询计划。在动态裁剪中，“build”侧是通过过滤键（pruningKey）来选择的部分数据，通常是广播表的查询计划
    buildKeys: Seq[Expression], //用于连接操作的“build”侧的连接键。它们与buildQuery中的数据字段相关联，用于连接操作和过滤的依据
    broadcastKeyIndex: Int, //是buildKeys中的索引，用于标识在广播表中哪个列（或字段）会被用作裁剪的关键字
    onlyInBroadcast: Boolean, //裁剪过滤器的应用场景。如果为false，表示裁剪过滤器可能对查询优化有利，因此即使无法重用广播的结果，也会执行该过滤器；如果为true，则只有在可以重用广播结果的情况下，才会执行裁剪过滤器
    exprId: ExprId = NamedExpression.newExprId,
    hint: Option[HintInfo] = None)
  extends SubqueryExpression(buildQuery, Seq(pruningKey), exprId, Seq.empty, hint)
  with DynamicPruning
  with Unevaluable
  with UnaryLike[Expression] {

  override def child: Expression = pruningKey  //子查询中用于动态裁剪的表达式

  override def plan: LogicalPlan = buildQuery //用于动态裁剪的“build”侧查询计划

  override def nullable: Boolean = false

  override def withNewPlan(plan: LogicalPlan): DynamicPruningSubquery = copy(buildQuery = plan)

  override def withNewHint(hint: Option[HintInfo]): SubqueryExpression = copy(hint = hint)

  override lazy val resolved: Boolean = {
    pruningKey.resolved &&
      buildQuery.resolved &&
      buildKeys.nonEmpty &&
      buildKeys.forall(_.resolved) &&
      broadcastKeyIndex >= 0 &&
      broadcastKeyIndex < buildKeys.size &&
      buildKeys.forall(_.references.subsetOf(buildQuery.outputSet)) &&
      pruningKey.dataType == buildKeys(broadcastKeyIndex).dataType
  }

  final override def nodePatternsInternal: Seq[TreePattern] = Seq(DYNAMIC_PRUNING_SUBQUERY)

  override def toString: String = s"dynamicpruning#${exprId.id} $conditionString"

  override lazy val canonicalized: DynamicPruning = {
    copy(
      pruningKey = pruningKey.canonicalized,
      buildQuery = buildQuery.canonicalized,
      buildKeys = buildKeys.map(_.canonicalized),
      exprId = ExprId(0))
  }

  override protected def withNewChildInternal(newChild: Expression): DynamicPruningSubquery =
    copy(pruningKey = newChild)
}

/**
 * Marker for a planned [[DynamicPruning]] expression.
 * The expression is created during planning, and it defers to its child for evaluation.
 *
 * @param child underlying predicate.
 */
//封装DynamicPruning表达式中的实际谓词（predicate）。child表示一个表达式，通常是用于判断过滤条件的子表达式
case class DynamicPruningExpression(child: Expression)
  extends UnaryExpression
  with DynamicPruning {
  override def eval(input: InternalRow): Any = child.eval(input)
  final override val nodePatterns: Seq[TreePattern] = Seq(DYNAMIC_PRUNING_EXPRESSION)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }

  override protected def withNewChildInternal(newChild: Expression): DynamicPruningExpression =
    copy(child = newChild)
}
