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

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.joins.ShuffledJoin

/**
 * A simple implementation of [[Cost]], which takes a number of [[Long]] as the cost value.
 */
// 简单成本表示，使用一个LONG类型的数字表示
case class SimpleCost(value: Long) extends Cost {

  override def compare(that: Cost): Int = that match {
    case SimpleCost(thatValue) =>
      if (value < thatValue) -1 else if (value > thatValue) 1 else 0
    case _ =>
      throw QueryExecutionErrors.cannotCompareCostWithTargetCostError(that.toString)
  }
}

/**
 * A skew join aware implementation of [[CostEvaluator]], which counts the number of
 * [[ShuffleExchangeLike]] nodes and skew join nodes in the plan.
 */
// 主要用于 Spark 自适应查询执行 (AQE) 过程中，对不同的物理执行计划进行启发式的成本比较
// 目标不是像传统优化器那样对查询的 CPU 或 I/O 做出精确估计，而是通过计数计划中的关键操作符来评估计划的相对“复杂性”或“劣势”：
// Shuffle 交换次数： Shuffle 往往是 Spark 执行中最昂贵的操作之一。
// 倾斜 Join 数量： 存在数据倾斜的 Join 会导致任务执行严重不平衡，是性能瓶颈
// 强制优化倾斜 Join 的标志 是否将倾斜 Join 的数量纳入成本计算
case class SimpleCostEvaluator(forceOptimizeSkewedJoin: Boolean) extends CostEvaluator {
  override def evaluateCost(plan: SparkPlan): Cost = {
    // 计算 Shuffle 数量
    val numShuffles = plan.collect {
      case s: ShuffleExchangeLike => s
    }.size

    if (forceOptimizeSkewedJoin) {
      val numSkewJoins = plan.collect {
        case j: ShuffledJoin if j.isSkewJoin => j
      }.size
      // We put `-numSkewJoins` in the first 32 bits of the long value, so that it's compared first
      // when comparing the cost, and larger `numSkewJoins` means lower cost.
      // 将其放置在 Long 值的高 32 位。
      // 由于是负数，在数值比较时，numSkewJoins 越大的计划，其成本值（整个 Long 值）越小，从而优先被选择（即解决倾斜的价值最高）
      SimpleCost(-numSkewJoins.toLong << 32 | numShuffles)
    } else {
      SimpleCost(numShuffles)
    }
  }
}
