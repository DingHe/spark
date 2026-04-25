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
// 这个类的核心作用是对物理计划（SparkPlan）的执行代价进行量化评分。
// 在 AQE 运行期间，Spark 可能会尝试多种不同的执行策略（例如不同的 Join 算法）。SimpleCostEvaluator 通过计算计划中包含的 Shuffle 数量 和 数据倾斜 Join 的优化情况，将复杂的物理计划转换成一个可以比较的数字（Cost）。
// forceOptimizeSkewedJoin 作用：这是一个开关参数，决定评估器是否将“倾斜 Join 的优化”作为最高优先级的考量因素。
// 如果为 true：评估器会极力推荐那些处理了数据倾斜的计划。
// 如果为 false：评估器仅关注 Shuffle 的数量。
case class SimpleCostEvaluator(forceOptimizeSkewedJoin: Boolean) extends CostEvaluator {
  override def evaluateCost(plan: SparkPlan): Cost = {
    // 计算 Shuffle 数量
    // 逻辑：遍历整个 SparkPlan 树，找出所有属于 ShuffleExchangeLike 类型的算子（如 ShuffleExchangeExec）。
    // 目的：Shuffle 是 Spark SQL 中最昂贵的操作（涉及磁盘 I/O 和网络传输），因此减少 Shuffle 数量是降低成本的首要目标。
    val numShuffles = plan.collect {
      case s: ShuffleExchangeLike => s
    }.size

    if (forceOptimizeSkewedJoin) {
      // 统计 numSkewJoins：计算计划中已经标记为 isSkewJoin（倾斜 Join）的 ShuffledJoin 算子数量。
      val numSkewJoins = plan.collect {
        case j: ShuffledJoin if j.isSkewJoin => j
      }.size
      // We put `-numSkewJoins` in the first 32 bits of the long value, so that it's compared first
      // when comparing the cost, and larger `numSkewJoins` means lower cost.
      // 将其放置在 Long 值的高 32 位。
      // 由于是负数，在数值比较时，numSkewJoins 越大的计划，其成本值（整个 Long 值）越小，从而优先被选择（即解决倾斜的价值最高）
      SimpleCost(-numSkewJoins.toLong << 32 | numShuffles)
    } else {
      // 逻辑：成本完全等同于 Shuffle 的个数。Shuffle 越少，计划越优。
      SimpleCost(numShuffles)
    }
  }
}
