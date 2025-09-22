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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, PartialMerge}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Remove redundant ProjectExec node from the spark plan. A ProjectExec node is redundant when
 * - It has the same output attributes and orders as its child's output and the ordering of
 *   the attributes is required.
 * - It has the same output attributes as its child's output when attribute output ordering
 *   is not required.
 * This rule needs to be a physical rule because project nodes are useful during logical
 * optimization to prune data. During physical planning, redundant project nodes can be removed
 * to simplify the query plan.
 */
//主要功能是移除物理执行计划中多余的 ProjectExec 节点，简化查询执行计划，提高执行效率
object RemoveRedundantProjects extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.REMOVE_REDUNDANT_PROJECTS_ENABLED)) { //控制是否执行该优化规则
      plan
    } else {
      removeProject(plan, true)
    }
  }
  //requireOrdering: Boolean：是否需要保持输出列的顺序。
  private def removeProject(plan: SparkPlan, requireOrdering: Boolean): SparkPlan = {
    plan match {
      case p @ ProjectExec(_, child) =>
        if (isRedundant(p, child, requireOrdering) && canRemove(p, child)) {
          //如果可以移除，递归移除子节点中的多余 ProjectExec
          val newPlan = removeProject(child, requireOrdering)
          // The `newPlan` should retain the logical plan link already. We call `setLogicalLink`
          // here to make sure the `newPlan` sets the `LOGICAL_PLAN_TAG` tag.
          newPlan.setLogicalLink(child.logicalLink.get)
          newPlan
        } else { //如果不能移除，继续递归处理其子节点，requireOrdering 设置为 false
          p.mapChildren(removeProject(_, false))
        }
      case op: TakeOrderedAndProjectExec =>
        // The planner turns Limit + Sort into TakeOrderedAndProjectExec which adds an additional
        // Project that does not exist in the logical plan. We shouldn't use this additional Project
        // to optimize out other Projects, otherwise when AQE turns physical plan back to
        // logical plan, we lose the Project and may mess up the output column order. So column
        // ordering is required if AQE is enabled and projectList is the same as child output.
        //如果 AQE（Adaptive Query Execution）开启，必须保留列的顺序
        val requireColOrdering = conf.adaptiveExecutionEnabled && op.projectList == op.child.output
        op.mapChildren(removeProject(_, requireColOrdering))
      case a: BaseAggregateExec =>
        // BaseAggregateExec require specific column ordering when mode is Final or PartialMerge.
        // See comments in BaseAggregateExec inputAttributes method.
        //BaseAggregateExec 节点在 Final 和 PartialMerge 聚合模式下，依赖于特定列的排序，不能移除 ProjectExec
        val keepOrdering = a.aggregateExpressions
          .exists(ae => ae.mode.equals(Final) || ae.mode.equals(PartialMerge))
        a.mapChildren(removeProject(_, keepOrdering))
      case o =>
        val required = if (canPassThrough(o)) requireOrdering else true
        o.mapChildren(removeProject(_, requireOrdering = required))
    }
  }

  /**
   * Check if the given node can pass the ordering requirement from its parent.
   */
  private def canPassThrough(plan: SparkPlan): Boolean = plan match {
    case _: FilterExec => true
    // JoinExec ordering requirement should inherit from its parent. If there is no ProjectExec in
    // its ancestors, JoinExec should require output columns to be ordered, and vice versa.
    case _: BaseJoinExec => true
    case _: WindowExec => true
    case _: ExpandExec => true
    case _ => false
  }

  /**
   * Check if the nullability change is positive. It catches the case when the project output
   * attribute is not nullable, but the child output attribute is nullable.
   */
  private def checkNullability(output: Seq[Attribute], childOutput: Seq[Attribute]): Boolean =
    output.zip(childOutput).forall { case (attr1, attr2) => attr1.nullable || !attr2.nullable }

  //project: ProjectExec  需要判断是否多余的 ProjectExec 节点
  //child: SparkPlan ProjectExec 节点的子节点，即其输出来源
  private def isRedundant(
      project: ProjectExec,
      child: SparkPlan,
      requireOrdering: Boolean): Boolean = {
    child match {
      // If a DataSourceV2ScanExec node does not support columnar, a ProjectExec node is required
      // to convert the rows to UnsafeRow. See DataSourceV2Strategy for more details.
      //如果子节点是 DataSourceV2ScanExecBase，且不支持列式存储，不能移除 ProjectExec
      //如果数据源不支持列式读取，Spark 需要依靠 ProjectExec 将数据转换为 UnsafeRow 类型，保证后续执行计划的兼容性
      case d: DataSourceV2ScanExecBase if !d.supportsColumnar => false
      //如果子节点是带有 FilterExec（过滤操作）的 DataSourceV2ScanExecBase，且不支持列式存储，同样不能移除
      case FilterExec(_, d: DataSourceV2ScanExecBase) if !d.supportsColumnar => false
      case _ =>
        if (requireOrdering) {
          //如果输出列的 ID 和 顺序 与子节点完全一致，且空值属性符合要求，则该 ProjectExec 是多余的，可以移除
          project.output.map(_.exprId.id) == child.output.map(_.exprId.id) &&
            checkNullability(project.output, child.output)
        } else {
          //如果输出列的 ID 在 忽略顺序 后一致，且空值属性符合要求，则 ProjectExec 是多余的
          val orderedProjectOutput = project.output.sortBy(_.exprId.id)
          val orderedChildOutput = child.output.sortBy(_.exprId.id)
          orderedProjectOutput.map(_.exprId.id) == orderedChildOutput.map(_.exprId.id) &&
            checkNullability(orderedProjectOutput, orderedChildOutput)
        }
    }
  }

  // SPARK-36020: Currently a project can only be removed if (1) its logical link is empty or (2)
  // its logical link is the same as the child's logical link. This is to ensure the physical
  // plan node can correctly map to its logical plan node in AQE.
  private def canRemove(project: ProjectExec, child: SparkPlan): Boolean = {
    project.logicalLink.isEmpty || project.logicalLink.exists(child.logicalLink.contains)
  }
}
