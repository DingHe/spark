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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningSubquery, ListQuery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.V1WriteCommand
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule wraps the query plan with an [[AdaptiveSparkPlanExec]], which executes the query plan
 * and re-optimize the plan during execution based on runtime data statistics.
 *
 * Note that this rule is stateful and thus should not be reused across query executions.
 */
case class InsertAdaptiveSparkPlan(
    adaptiveExecutionContext: AdaptiveExecutionContext) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = applyInternal(plan, false)

  private def applyInternal(plan: SparkPlan, isSubquery: Boolean): SparkPlan = plan match {
    case _ if !conf.adaptiveExecutionEnabled => plan  //如果没有开启自适应执行，直接返回原计划
    // 其他一些特殊情况处理（例如命令执行）
    case _: ExecutedCommandExec => plan
    case _: CommandResultExec => plan
    case c: V2CommandExec => c.withNewChildren(c.children.map(apply))
    case c: DataWritingCommandExec
        if !c.cmd.isInstanceOf[V1WriteCommand] || !conf.plannedWriteEnabled =>
      c.copy(child = apply(c.child))
    // 如果计划支持自适应执行，并且符合 AQE 的应用条件
    case _ if shouldApplyAQE(plan, isSubquery) =>
      if (supportAdaptive(plan)) {
        try {
          // Plan sub-queries recursively and pass in the shared stage cache for exchange reuse.
          // Fall back to non-AQE mode if AQE is not supported in any of the sub-queries.
          val subqueryMap = buildSubqueryMap(plan)  //构建子查询的map
          val planSubqueriesRule = PlanAdaptiveSubqueries(subqueryMap)  //子查询逻辑计划转为物理计划的规则
          val preprocessingRules = Seq(
            planSubqueriesRule)
          // Run pre-processing rules.
          val newPlan = AdaptiveSparkPlanExec.applyPhysicalRules(plan, preprocessingRules)  //应用物理规则
          logDebug(s"Adaptive execution enabled for plan: $plan")
          AdaptiveSparkPlanExec(newPlan, adaptiveExecutionContext, preprocessingRules, isSubquery) //利用物理计划构建AdaptiveSparkPlanExec
        } catch {
          case SubqueryAdaptiveNotSupportedException(subquery) =>
            logWarning(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is enabled " +
              s"but is not supported for sub-query: $subquery.")
            plan
        }
      } else {
        logDebug(s"${SQLConf.ADAPTIVE_EXECUTION_ENABLED.key} is enabled " +
          s"but is not supported for query: $plan.")
        plan
      }

    case _ => plan
  }

  // AQE is only useful when the query has exchanges, sub-queries or table caches. This method
  // returns true if one of the following conditions is satisfied:
  //   - The config ADAPTIVE_EXECUTION_FORCE_APPLY is true.
  //   - The input query is from a sub-query. When this happens, it means we've already decided to
  //     apply AQE for the main query and we must continue to do it.
  //   - The query contains exchanges.
  //   - The query may need to add exchanges. It's an overkill to run `EnsureRequirements` here, so
  //     we just check `SparkPlan.requiredChildDistribution` and see if it's possible that the
  //     the query needs to add exchanges later.
  //   - The query contains nested `AdaptiveSparkPlanExec`.
  //   - The query contains `InMemoryTableScanExec`.
  //   - The query contains sub-query.

  //用于判断是否应该为当前查询计划（plan）启用自适应查询执行（AQE）
  //isSubquery: Boolean：指示当前计划是否为子查询，如果当前查询是子查询（由外部查询调用的查询），则 AQE 将自动应用于外部查询。也就是说，如果是子查询，AQE 会继续应用在外部查询中
  private def shouldApplyAQE(plan: SparkPlan, isSubquery: Boolean): Boolean = {
    //检查是否强制启用 AQE
    conf.getConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY) || isSubquery || {
      plan.exists {
        case _: Exchange => true  //如果查询计划中包含 Exchange 操作（例如 Shuffle 操作）
        case p if !p.requiredChildDistribution.forall(_ == UnspecifiedDistribution) => true  //如果查询的子节点（子查询）需要特定的分布方式（不是 UnspecifiedDistribution），则 AQE 可能需要插入额外的交换操作，以便根据实际数据情况优化查询
        // AQE framework has a different way to update the query plan in the UI: it updates the plan
        // at the end of execution, while non-AQE updates the plan before execution. If the cached
        // plan is already AQEed, the current plan must be AQEed as well so that the UI can get plan
        // update correctly.
        case i: InMemoryTableScanExec
            if i.relation.cachedPlan.isInstanceOf[AdaptiveSparkPlanExec] => true  //已经应用了 AQE 的缓存表（cachedPlan 是 AdaptiveSparkPlanExec 类型），则说明该查询计划可以启用 AQE
        case _: InMemoryTableScanExec
            if conf.getConf(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING) => true  //如果查询计划中包含 InMemoryTableScanExec，并且配置允许更改缓存表的输出分区（SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING 配置为 true），则 AQE 可以应用于这个查询
        case p => p.expressions.exists(_.exists {
          case _: SubqueryExpression => true  //如果查询计划中包含子查询表达式（如 ScalarSubquery 或 DynamicPruningSubquery），则 AQE 应该启用
          case _ => false
        })
      }
    }
  }

  private def supportAdaptive(plan: SparkPlan): Boolean = {
    sanityCheck(plan) &&
      !plan.logicalLink.exists(_.isStreaming) && //查询是否为流式查询，如果是，则不支持 AQE
    plan.children.forall(supportAdaptive)
  }
  //一个有效的 logicalLink 是判断当前计划是否完整和有效的标志
  private def sanityCheck(plan: SparkPlan): Boolean =
    plan.logicalLink.isDefined

  /**
   * Returns an expression-id-to-execution-plan map for all the sub-queries.
   * For each sub-query, generate the adaptive execution plan for each sub-query by applying this
   * rule.
   * The returned subquery map holds executed plan, then the [[PlanAdaptiveSubqueries]] can take
   * them and create a new subquery.
   */
  //通过递归查找查询计划中的所有子查询，并为每个子查询生成相应的执行计划，最后返回一个包含所有子查询执行计划的映射
  private def buildSubqueryMap(plan: SparkPlan): Map[Long, SparkPlan] = {
    val subqueryMap = mutable.HashMap.empty[Long, SparkPlan]
    //// 如果计划中不包含标记为子查询的表达式，直接返回空的 Map
    if (!plan.containsAnyPattern(SCALAR_SUBQUERY, IN_SUBQUERY, DYNAMIC_PRUNING_SUBQUERY)) {
      return subqueryMap.toMap
    }
    //遍历查询计划中的所有表达式，查找符合子查询模式的表达式（例如标记为 PLAN_EXPRESSION），PLAN_EXPRESSION表示包含查询计划的表达式
    plan.foreach(_.expressions.filter(_.containsPattern(PLAN_EXPRESSION)).foreach(_.foreach {
      case e @ (_: expressions.ScalarSubquery | _: ListQuery | _: DynamicPruningSubquery) =>
        val subquery = e.asInstanceOf[SubqueryExpression]
        if (!subqueryMap.contains(subquery.exprId.id)) {
          val executedPlan = compileSubquery(subquery.plan)  // 编译子查询的执行计划
          verifyAdaptivePlan(executedPlan, subquery.plan)
          subqueryMap.put(subquery.exprId.id, executedPlan)
        }
      case _ =>
    }))

    subqueryMap.toMap
  }
  //为子查询生成适当的物理执行计划
  def compileSubquery(plan: LogicalPlan): SparkPlan = {
    // Apply the same instance of this rule to sub-queries so that sub-queries all share the
    // same `stageCache` for Exchange reuse.
    this.applyInternal(
      QueryExecution.createSparkPlan(adaptiveExecutionContext.session,  //把逻辑计划转为物理计划就是编译
        adaptiveExecutionContext.session.sessionState.planner, plan.clone()), true)
  }

  private def verifyAdaptivePlan(plan: SparkPlan, logicalPlan: LogicalPlan): Unit = {
    if (!plan.isInstanceOf[AdaptiveSparkPlanExec]) {
      throw SubqueryAdaptiveNotSupportedException(logicalPlan)
    }
  }
}

private case class SubqueryAdaptiveNotSupportedException(plan: LogicalPlan) extends Exception {}
