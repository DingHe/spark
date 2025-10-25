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

import org.apache.spark.sql.catalyst.analysis.UpdateAttributeNullability
import org.apache.spark.sql.catalyst.optimizer.{ConvertToLocalRelation, EliminateLimits, OptimizeOneRowPlan}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LogicalPlanIntegrity}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * The optimizer for re-optimizing the logical plan used by AdaptiveSparkPlanExec.
 */
// Spark 自适应查询执行 (Adaptive Query Execution, AQE) 框架中使用的运行时逻辑计划优化器
// 在传统的 Spark 执行模式中，查询优化在执行前完成一次。
// 而在 AQE 中，Spark 允许在查询执行期间，根据 Shuffle 统计信息等运行时数据，重新优化（或称再优化）逻辑查询计划。
// AQEOptimizer 就是负责执行这些运行时优化的规则集

class AQEOptimizer(conf: SQLConf, extendedRuntimeOptimizerRules: Seq[Rule[LogicalPlan]])
  extends RuleExecutor[LogicalPlan] {

  private def fixedPoint =
    FixedPoint(
      conf.optimizerMaxIterations,
      maxIterationsSetting = SQLConf.OPTIMIZER_MAX_ITERATIONS.key)

  // 默认规则批次序列
  private val defaultBatches = Seq(
    Batch("Propagate Empty Relations", fixedPoint,
      AQEPropagateEmptyRelation,
      ConvertToLocalRelation,
      UpdateAttributeNullability),
    Batch("Dynamic Join Selection", Once, DynamicJoinSelection), // 根据运行时统计信息动态选择最佳 Join 策略（执行 一次）
    Batch("Eliminate Limits", fixedPoint, EliminateLimits),
    Batch("Optimize One Row Plan", fixedPoint, OptimizeOneRowPlan)) :+
    Batch("User Provided Runtime Optimizers", fixedPoint, extendedRuntimeOptimizerRules: _*)
  // 获取实际执行的规则批次（核心）
  final override protected def batches: Seq[Batch] = {
    val excludedRules = conf.getConf(SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES)
      .toSeq.flatMap(Utils.stringToSeq)
    defaultBatches.flatMap { batch =>
      val filteredRules = batch.rules.filter { rule =>
        val exclude = excludedRules.contains(rule.ruleName)
        if (exclude) {
          logInfo(s"Optimization rule '${rule.ruleName}' is excluded from the optimizer.")
        }
        !exclude
      }
      if (batch.rules == filteredRules) {
        Some(batch)
      } else if (filteredRules.nonEmpty) {
        Some(Batch(batch.name, batch.strategy, filteredRules: _*))
      } else {
        logInfo(s"Optimization batch '${batch.name}' is excluded from the optimizer " +
          s"as all enclosed rules have been excluded.")
        None
      }
    }
  }
  // 验证逻辑计划的变更
  override protected def validatePlanChanges(
      previousPlan: LogicalPlan,
      currentPlan: LogicalPlan): Option[String] = {
    LogicalPlanIntegrity.validateOptimizedPlan(previousPlan, currentPlan)
  }
}
