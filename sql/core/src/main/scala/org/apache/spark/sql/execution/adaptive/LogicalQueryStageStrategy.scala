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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.{joins, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

/**
 * Strategy for plans containing [[LogicalQueryStage]] nodes:
 * 1. Transforms [[LogicalQueryStage]] to its corresponding physical plan that is either being
 *    executed or has already completed execution.
 * 2. Transforms [[Join]] which has one child relation already planned and executed as a
 *    [[BroadcastQueryStageExec]]. This is to prevent reversing a broadcast stage into a shuffle
 *    stage in case of the larger join child relation finishes before the smaller relation. Note
 *    that this rule needs to be applied before regular join strategies.
 */ //用于将包含 LogicalQueryStage 节点的逻辑查询计划转换为相应的物理执行计划
object LogicalQueryStageStrategy extends Strategy {
  //判断某个查询计划是否为 BroadcastQueryStage 阶段。BroadcastQueryStage 是一个逻辑查询阶段，它标志着某个查询计划已经变成了广播阶段
  private def isBroadcastStage(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, _: BroadcastQueryStageExec) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    //等值连接，如果左节点或者有节点为广播节点，则进行广播hash join
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, otherCondition, _,
          left, right, hint)
        if isBroadcastStage(left) || isBroadcastStage(right) =>
      val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
      Seq(BroadcastHashJoinExec(
        leftKeys, rightKeys, joinType, buildSide, otherCondition, planLater(left),
        planLater(right)))
    //单列非空反连接，转为BroadcastHashJoinExec
    case j @ ExtractSingleColumnNullAwareAntiJoin(leftKeys, rightKeys)
        if isBroadcastStage(j.right) =>
      Seq(joins.BroadcastHashJoinExec(leftKeys, rightKeys, LeftAnti, BuildRight,
        None, planLater(j.left), planLater(j.right), isNullAwareAntiJoin = true))
    //其他join，其中一边是广播，则使用BroadcastNestedLoopJoinExec
    case j @ Join(left, right, joinType, condition, _)
        if isBroadcastStage(left) || isBroadcastStage(right) =>
      val buildSide = if (isBroadcastStage(left)) BuildLeft else BuildRight
      BroadcastNestedLoopJoinExec(
        planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

    case q: LogicalQueryStage =>
      q.physicalPlan :: Nil

    case _ => Nil
  }
}
