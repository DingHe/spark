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

import java.util.concurrent.{Future => JFuture}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.joins.{HashedRelation, HashJoin, LongHashedRelation}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.ThreadUtils

/**
 * Physical plan for a custom subquery that collects and transforms the broadcast key values.
 * This subquery retrieves the partition key from the broadcast results based on the type of
 * [[HashedRelation]] returned. If the key is packed inside a Long, we extract it through
 * bitwise operations, otherwise we return it from the appropriate index of the [[UnsafeRow]].
 *
 * @param index the index of the join key in the list of keys from the build side
 * @param buildKeys the join keys from the build side of the join used
 * @param child the BroadcastExchange or the AdaptiveSparkPlan with BroadcastQueryStageExec
 *              from the build side of the join
 */
//用于执行广播子查询的物理计划操作节点。这个类涉及到子查询的广播执行，主要用于查询优化阶段通过广播小表以提高查询效率，尤其是在执行涉及子查询的连接操作时
case class SubqueryBroadcastExec(
    name: String, //自定义的名称属性，用于为 SubqueryBroadcastExec 实例提供标识。此名称在日志记录和错误跟踪时很有用
    index: Int,  //buildKeys 列表中子查询对应的键的索引位置
    buildKeys: Seq[Expression],  //用于连接操作中“构建侧”（build side）的一组连接键（即连接条件中的表达式）
    child: SparkPlan)    //执行结果将作为广播数据传递给执行计划中的其他部分。child 可以是一个 BroadcastExchange，也可以是一个 AdaptiveSparkPlan
  extends BaseSubqueryExec with UnaryExecNode {

  // `SubqueryBroadcastExec` is only used with `InSubqueryExec`. No one would reference this output,
  // so the exprId doesn't matter here. But it's important to correctly report the output length, so
  // that `InSubqueryExec` can know it's the single-column execution mode, not multi-column.
  //执行节点的输出列，它定义了从这个物理节点输出的数据列。这里的输出是一个包含构建侧连接键的 Seq[Attribute]，用于支持查询的广播执行
  override def output: Seq[Attribute] = {
    val key = buildKeys(index)
    val name = key match {
      case n: NamedExpression => n.name
      case Cast(n: NamedExpression, _, _, _) => n.name
      case _ => "key"
    }
    Seq(AttributeReference(name, key.dataType, key.nullable)())
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"))
  //规范化之后，它返回一个新的 SubqueryBroadcastExec 实例，更新了 buildKeys 和 child（子节点）的状态
  override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    SubqueryBroadcastExec("dpp", index, keys, child.canonicalized)
  }

  @transient
  private lazy val relationFuture: JFuture[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY) //执行ID
    SQLExecution.withThreadLocalCaptured[Array[InternalRow]](
      session, SubqueryBroadcastExec.executionContext) {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(session, executionId) {
        val beforeCollect = System.nanoTime()
        //执行广播查询
        val broadcastRelation = child.executeBroadcast[HashedRelation]().value  //触发广播查询的执行
        val (iter, expr) = if (broadcastRelation.isInstanceOf[LongHashedRelation]) {
          (broadcastRelation.keys(), HashJoin.extractKeyExprAt(buildKeys, index))
        } else {
          (broadcastRelation.keys(),
            BoundReference(index, buildKeys(index).dataType, buildKeys(index).nullable))
        }

        val proj = UnsafeProjection.create(expr)
        val keyIter = iter.map(proj).map(_.copy())

        val rows = if (broadcastRelation.keyIsUnique) {
          keyIter.toArray[InternalRow]
        } else {
          keyIter.toArray[InternalRow].distinct
        }
        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += (beforeBuild - beforeCollect) / 1000000
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes).sum
        longMetric("numOutputRows") += rows.length
        longMetric("dataSize") += dataSize
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

        rows  //返回收集到的所有数据行（InternalRow 数组），这些数据将在广播连接中使用
      }
    }
  }
  //执行过程中启动 relationFuture，即启动子查询的广播数据收集。该方法实际上是启动异步执行的第一步
  protected override def doPrepare(): Unit = {
    relationFuture
  }
  //因为 SubqueryBroadcastExec 的执行路径并不直接生成 RDD。它依赖于广播查询的结果而不是直接的计算，因而这里的代码路径不支持直接的执行
  protected override def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.executeCodePathUnsupportedError("SubqueryBroadcastExec")
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[id=#$id]")

  override protected def withNewChildInternal(newChild: SparkPlan): SubqueryBroadcastExec =
    copy(child = newChild)
}

object SubqueryBroadcastExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("dynamicpruning",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
