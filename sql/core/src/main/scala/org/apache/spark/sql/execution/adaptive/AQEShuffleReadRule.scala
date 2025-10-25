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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeLike, ShuffleOrigin}

/**
 * A rule that may create [[AQEShuffleReadExec]] on top of [[ShuffleQueryStageExec]] and change the
 * plan output partitioning. The AQE framework will skip the rule if it leads to extra shuffles.
 */
// 抽象出所有运行时作用于已完成 Shuffle 阶段（ShuffleQueryStageExec）之上，并可能插入 AQEShuffleReadExec 节点的优化规则
// 这类规则负责在 Shuffle 阶段完成后，根据收集到的运行时统计信息（如分区大小），动态地改变下游数据的读取方式
trait AQEShuffleReadRule extends Rule[SparkPlan] {
  /**
   * Returns the list of [[ShuffleOrigin]]s supported by this rule.
   */
  // 受支持的 Shuffle 来源（抽象）
  // 要求所有继承此特质的具体优化规则必须实现它，以返回一个 ShuffleOrigin 列表。ShuffleOrigin 标记了 Shuffle 操作在逻辑计划中的来源（例如 JOIN, REPARTITION, AGGREGATE 等）
  protected def supportedShuffleOrigins: Seq[ShuffleOrigin]
  // 检查 Shuffle 是否受支持
  protected def isSupported(shuffle: ShuffleExchangeLike): Boolean = {
    supportedShuffleOrigins.contains(shuffle.shuffleOrigin)
  }
}
