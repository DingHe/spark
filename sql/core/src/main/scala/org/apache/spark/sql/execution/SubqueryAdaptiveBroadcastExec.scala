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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * Similar to [[SubqueryBroadcastExec]], this node is used to store the
 * initial physical plan of DPP subquery filters when enabling both AQE and DPP.
 * It is intermediate physical plan and not executable.
 * After the build side is executed, this node will be replaced with the
 * [[SubqueryBroadcastExec]] and the child will be optimized with the ReusedExchange
 * from the build side.
 */
//表示启用自适应查询执行（AQE）和动态分区剪裁（DPP）的广播子查询的中间节点
case class SubqueryAdaptiveBroadcastExec(
    name: String, //表示该物理执行计划的名称
    index: Int, //用于标识该节点在父级执行计划中的位置
    onlyInBroadcast: Boolean, //标识是否仅在广播时使用该执行计划
    @transient buildPlan: LogicalPlan, //表示构建表的逻辑计划
    buildKeys: Seq[Expression], //构建子查询时用于连接的列或表达式，通常是子查询与主查询之间的连接条件
    child: SparkPlan) extends BaseSubqueryExec with UnaryExecNode {

  protected override def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.executeCodePathUnsupportedError("SubqueryAdaptiveBroadcastExec")
  }

  protected override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    copy(name = "dpp", buildKeys = keys, child = child.canonicalized)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SubqueryAdaptiveBroadcastExec =
    copy(child = newChild)
}
