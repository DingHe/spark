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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * Given a [[LogicalPlan]], returns a list of `PhysicalPlan`s that can
 * be used for execution. If this strategy does not apply to the given logical operation then an
 * empty list should be returned.
 */
// 用于策略模式的一个抽象类，用于将逻辑计划转换为物理计划。
// 物理计划是 Spark 查询优化过程中重要的一步，它指定了实际执行查询所需的操作和步骤。
// 这个类是所有物理执行策略的基类
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]] extends Logging {

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  //用于为给定的逻辑计划返回一个占位符物理计划。占位符通常表示一个尚未完全展开的执行计划，在后续的优化过程中会被其他策略填充成具体的物理执行计划
  protected def planLater(plan: LogicalPlan): PhysicalPlan
  //用于应用当前的策略，将一个逻辑计划转换为一个或多个物理计划。如果该策略不适用于给定的逻辑计划，它应该返回一个空的物理计划列表
  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}

/**
 * Abstract class for transforming [[LogicalPlan]]s into physical plans.
 * Child classes are responsible for specifying a list of [[GenericStrategy]] objects that
 * each of which can return a list of possible physical plan options.
 * If a given strategy is unable to plan all of the remaining operators in the tree,
 * it can call [[GenericStrategy#planLater planLater]], which returns a placeholder
 * object that will be [[collectPlaceholders collected]] and filled in
 * using other available strategies.
 *
 * TODO: RIGHT NOW ONLY ONE PLAN IS RETURNED EVER...
 *       PLAN SPACE EXPLORATION WILL BE IMPLEMENTED LATER.
 *
 * @tparam PhysicalPlan The type of physical plan produced by this [[QueryPlanner]]
 */
//查询规划器类 QueryPlanner 的定义，主要用于将逻辑计划（LogicalPlan）转换为物理计划（PhysicalPlan）。
// 物理计划是执行查询的实际步骤和顺序
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]  //表示所有的执行策略

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    //遍历所有策略，调用每个策略来生成物理计划的候选方案
    // Collect physical plan candidates.
    val candidates = strategies.iterator.flatMap(_(plan))

    // The candidates may contain placeholders marked as [[planLater]],
    // so try to replace them by their child plans.
    val plans = candidates.flatMap { candidate =>
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        //如果当前候选计划没有占位符，则直接返回该计划作为有效计划
        Iterator(candidate)
      } else {
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            //递归地为占位符的逻辑计划生成物理计划
            val childPlans = this.plan(logicalPlan)

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {  //将递归生成的物理计划替换到原计划中的占位符位置
                  case p if p.eq(placeholder) => childPlan
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }

  /**
   * Collects placeholders marked using [[GenericStrategy#planLater planLater]]
   * by [[strategies]].
   */
  //收集物理计划中的占位符，标记了哪些部分的计划需要后续进一步的规划（即那些标记为 planLater 的部分）
  //返回一个包含占位符和其对应逻辑计划（LogicalPlan）的元组列表
  protected def collectPlaceholders(plan: PhysicalPlan): Seq[(PhysicalPlan, LogicalPlan)]

  /** Prunes bad plans to prevent combinatorial explosion. */
  //修剪无效的物理计划
  protected def prunePlans(plans: Iterator[PhysicalPlan]): Iterator[PhysicalPlan]
}
