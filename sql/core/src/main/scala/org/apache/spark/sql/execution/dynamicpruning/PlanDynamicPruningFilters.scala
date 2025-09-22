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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSeq, BindReferences, DynamicPruningExpression, DynamicPruningSubquery, Expression, ListQuery, Literal}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DYNAMIC_PRUNING_SUBQUERY
import org.apache.spark.sql.execution.{InSubqueryExec, QueryExecution, SparkPlan, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins._

/**
 * This planner rule aims at rewriting dynamic pruning predicates in order to reuse the
 * results of broadcast. For joins that are not planned as broadcast hash joins we keep
 * the fallback mechanism with subquery duplicate.
*/
//将通过重写动态修剪表达式，使得可以重用广播连接的结果，从而优化查询性能
case class PlanDynamicPruningFilters(sparkSession: SparkSession) extends Rule[SparkPlan] {

  /** HashedRelationBroadcastMode，表示这些键将通过哈希广播的方式进行处理
   * Identify the shape in which keys of a given plan are broadcasted.
   */
  private def broadcastMode(keys: Seq[Expression], output: AttributeSeq): BroadcastMode = {
    val packedKeys = BindReferences.bindReferences(HashJoin.rewriteKeyExpr(keys), output)
    HashedRelationBroadcastMode(packedKeys)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.dynamicPartitionPruningEnabled) {
      return plan
    }

    plan.transformAllExpressionsWithPruning(_.containsPattern(DYNAMIC_PRUNING_SUBQUERY)) {
      //value：需要进行动态修剪的分区列的值（主表中的过滤条件）
      //buildPlan：构建侧（如 Fact 表）的子查询执行计划，用于生成动态修剪所需的值。
      //buildKeys：构建侧的分区列键（与 value 进行匹配的键）
      //broadcastKeyIndex：需要广播的键的索引，指向 buildKeys 中的特定列。
      //onlyInBroadcast：标志位，表示此动态修剪是否只在广播情况下有效。
      //
      case DynamicPruningSubquery(
          value, buildPlan, buildKeys, broadcastKeyIndex, onlyInBroadcast, exprId, _) =>
        //将逻辑计划 buildPlan 转换为物理执行计划
        val sparkPlan = QueryExecution.createSparkPlan(
          sparkSession, sparkSession.sessionState.planner, buildPlan)
        // Using `sparkPlan` is a little hacky as it is based on the assumption that this rule is
        // the first to be applied (apart from `InsertAdaptiveSparkPlan`).
        //广播交换复用功能必须开启

        val canReuseExchange = conf.exchangeReuseEnabled && buildKeys.nonEmpty &&
          plan.exists {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _, _) =>
              left.sameResult(sparkPlan)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right, _) =>
              right.sameResult(sparkPlan)
            case _ => false
          }

        if (canReuseExchange) {
          val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, sparkPlan)
          val mode = broadcastMode(buildKeys, executedPlan.output)
          // plan a broadcast exchange of the build side of the join
          val exchange = BroadcastExchangeExec(mode, executedPlan)
          val name = s"dynamicpruning#${exprId.id}"
          // place the broadcast adaptor for reusing the broadcast results on the probe side
          val broadcastValues =
            SubqueryBroadcastExec(name, broadcastKeyIndex, buildKeys, exchange)
          DynamicPruningExpression(InSubqueryExec(value, broadcastValues, exprId))
        } else if (onlyInBroadcast) { //如果该动态修剪仅限于广播，但无法复用，则直接返回 Literal.TrueLiteral，表示无需执行动态修剪
          // it is not worthwhile to execute the query, so we fall-back to a true literal
          DynamicPruningExpression(Literal.TrueLiteral)
        } else {
          // we need to apply an aggregate on the buildPlan in order to be column pruned
          val alias = Alias(buildKeys(broadcastKeyIndex), buildKeys(broadcastKeyIndex).toString)()
          val aggregate = Aggregate(Seq(alias), Seq(alias), buildPlan)
          DynamicPruningExpression(expressions.InSubquery(
            Seq(value), ListQuery(aggregate, numCols = aggregate.output.length)))
        }
    }
  }
}
