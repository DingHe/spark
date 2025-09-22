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

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future

import org.apache.spark.{FutureAction, MapOutputStatistics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanLike
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A query stage is an independent subgraph of the query plan. AQE framework will materialize its
 * output before proceeding with further operators of the query plan. The data statistics of the
 * materialized output can be used to optimize the rest of the query plan.
 *///查询阶段是查询计划的独立子图，主要涉及到查询计划的执行与物化操作
abstract class QueryStageExec extends LeafExecNode {

  /**
   * An id of this query stage which is unique in the entire query plan.
   */
  val id: Int  //表示当前查询阶段的唯一标识符

  /**
   * The sub-tree of the query plan that belongs to this query stage.
   */
  val plan: SparkPlan  //当前查询阶段关联的查询计划子树

  /**
   * Materialize this query stage, to prepare for the execution, like submitting map stages,
   * broadcasting data, etc. The caller side can use the returned [[Future]] to wait until this
   * stage is ready.
   *///物化当前查询阶段，它会触发对查询阶段的实际执行（如提交 Map 阶段、广播数据等）
  final def materialize(): Future[Any] = {
    logDebug(s"Materialize query stage ${this.getClass.getSimpleName}: $id")
    doMaterialize()
  }
  //子类重写，负责实际的物化逻辑
  protected def doMaterialize(): Future[Any]

  /**
   * Returns the runtime statistics after stage materialization.
   */ //获取当前查询阶段物化后的运行时统计信息
  def getRuntimeStatistics: Statistics

  /**
   * Compute the statistics of the query stage if executed, otherwise None.
   *///查询阶段已经物化，则计算并返回该阶段的统计信息。如果尚未物化，返回 None
  def computeStats(): Option[Statistics] = if (isMaterialized) {
    val runtimeStats = getRuntimeStatistics
    val dataSize = runtimeStats.sizeInBytes.max(0)
    val numOutputRows = runtimeStats.rowCount.map(_.max(0))
    val attributeStats = runtimeStats.attributeStats
    Some(Statistics(dataSize, numOutputRows, attributeStats, isRuntime = true))
  } else {
    None
  }

  @transient
  @volatile  //存储查询阶段的结果
  protected var _resultOption = new AtomicReference[Option[Any]](None)

  private[adaptive] def resultOption: AtomicReference[Option[Any]] = _resultOption
  final def isMaterialized: Boolean = resultOption.get().isDefined //判断当前查询阶段是否已经物化，如果 _resultOption 中有值，说明已经物化

  @transient
  @volatile
  protected var _error = new AtomicReference[Option[Throwable]](None)

  def error: AtomicReference[Option[Throwable]] = _error
  final def hasFailed: Boolean = _error.get().isDefined

  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering
  override def executeCollect(): Array[InternalRow] = plan.executeCollect()
  override def executeTake(n: Int): Array[InternalRow] = plan.executeTake(n)
  override def executeTail(n: Int): Array[InternalRow] = plan.executeTail(n)
  override def executeToIterator(): Iterator[InternalRow] = plan.executeToIterator()

  protected override def doExecute(): RDD[InternalRow] = plan.execute()
  override def supportsRowBased: Boolean = plan.supportsRowBased
  override def supportsColumnar: Boolean = plan.supportsColumnar
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = plan.executeColumnar()
  override def doExecuteBroadcast[T](): Broadcast[T] = plan.executeBroadcast()

  protected override def stringArgs: Iterator[Any] = Iterator.single(id)

  override def simpleStringWithNodeId(): String = {
    super.simpleStringWithNodeId() + computeStats().map(", " + _.toString).getOrElse("")
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: java.util.ArrayList[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    super.generateTreeString(depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix,
      maxFields,
      printNodeId,
      indent)
    lastChildren.add(true)
    plan.generateTreeString(
      depth + 1, lastChildren, append, verbose, "", false, maxFields, printNodeId, indent)
    lastChildren.remove(lastChildren.size() - 1)
  }

  override protected[sql] def cleanupResources(): Unit = {
    plan.cleanupResources()
    super.cleanupResources()
  }
}

/**
 * There are 2 kinds of exchange query stages:
 *   1. Shuffle query stage. This stage materializes its output to shuffle files, and Spark launches
 *      another job to execute the further operators.
 *   2. Broadcast query stage. This stage materializes its output to an array in driver JVM. Spark
 *      broadcasts the array before executing the further operators.
 */ //有两种交换查询阶段，shuffle查询阶段和广播查询阶段
abstract class ExchangeQueryStageExec extends QueryStageExec {

  /**
   * Cancel the stage materialization if in progress; otherwise do nothing.
   */
  def cancel(): Unit

  /**
   * The canonicalized plan before applying query stage optimizer rules.
   */
  val _canonicalized: SparkPlan

  override def doCanonicalize(): SparkPlan = _canonicalized

  def newReuseInstance(newStageId: Int, newOutput: Seq[Attribute]): ExchangeQueryStageExec
}

/**
 * A shuffle query stage whose child is a [[ShuffleExchangeLike]] or [[ReusedExchangeExec]].
 *
 * @param id the query stage id.
 * @param plan the underlying plan.
 * @param _canonicalized the canonicalized plan before applying query stage optimizer rules.
 */ //处理 shuffle 查询阶段的一个执行计划，通常用于处理涉及到 ShuffleExchangeLike 或 ReusedExchangeExec 类型的子查询
case class ShuffleQueryStageExec(
    override val id: Int,
    override val plan: SparkPlan,
    override val _canonicalized: SparkPlan) extends ExchangeQueryStageExec {

  @transient val shuffle = plan match {  //表示当前查询阶段的 shuffle 操作
    case s: ShuffleExchangeLike => s
    case ReusedExchangeExec(_, s: ShuffleExchangeLike) => s
    case _ =>
      throw new IllegalStateException(s"wrong plan for shuffle stage:\n ${plan.treeString}")
  }

  def advisoryPartitionSize: Option[Long] = shuffle.advisoryPartitionSize

  @transient private lazy val shuffleFuture = shuffle.submitShuffleJob

  override protected def doMaterialize(): Future[Any] = shuffleFuture

  override def newReuseInstance(
      newStageId: Int, newOutput: Seq[Attribute]): ExchangeQueryStageExec = {
    val reuse = ShuffleQueryStageExec(
      newStageId,
      ReusedExchangeExec(newOutput, shuffle),
      _canonicalized)
    reuse._resultOption = this._resultOption
    reuse._error = this._error
    reuse
  }

  override def cancel(): Unit = shuffleFuture match {
    case action: FutureAction[MapOutputStatistics] if !action.isCompleted =>
      action.cancel()
    case _ =>
  }

  /**
   * Returns the Option[MapOutputStatistics]. If the shuffle map stage has no partition,
   * this method returns None, as there is no map statistics.
   */
  def mapStats: Option[MapOutputStatistics] = {
    assert(resultOption.get().isDefined, s"${getClass.getSimpleName} should already be ready")
    val stats = resultOption.get().get.asInstanceOf[MapOutputStatistics]
    Option(stats)
  }

  override def getRuntimeStatistics: Statistics = shuffle.runtimeStatistics
}

/**
 * A broadcast query stage whose child is a [[BroadcastExchangeLike]] or [[ReusedExchangeExec]].
 *
 * @param id the query stage id.
 * @param plan the underlying plan.
 * @param _canonicalized the canonicalized plan before applying query stage optimizer rules.
 */ //用于处理广播查询。它的子查询是一个 BroadcastExchangeLike 或 ReusedExchangeExec
case class BroadcastQueryStageExec(
    override val id: Int,
    override val plan: SparkPlan,
    override val _canonicalized: SparkPlan) extends ExchangeQueryStageExec {

  @transient val broadcast = plan match { //示当前查询阶段的广播交换
    case b: BroadcastExchangeLike => b
    case ReusedExchangeExec(_, b: BroadcastExchangeLike) => b
    case _ =>
      throw new IllegalStateException(s"wrong plan for broadcast stage:\n ${plan.treeString}")
  }
  //物化当前查询阶段
  override protected def doMaterialize(): Future[Any] = {
    broadcast.submitBroadcastJob  //提交广播作业
  }
  //创建当前查询阶段的一个新的复用实例。在某些情况下，查询计划中的广播查询阶段可能会被复用
  override def newReuseInstance(
      newStageId: Int, newOutput: Seq[Attribute]): ExchangeQueryStageExec = {
    val reuse = BroadcastQueryStageExec(
      newStageId,
      ReusedExchangeExec(newOutput, broadcast),
      _canonicalized)
    reuse._resultOption = this._resultOption
    reuse._error = this._error
    reuse
  }
  //取消当前查询阶段的广播作业
  override def cancel(): Unit = {
    if (!broadcast.relationFuture.isDone) {  //没有完成，则取消该作业
      sparkContext.cancelJobsWithTag(broadcast.jobTag)
      broadcast.relationFuture.cancel(true)
    }
  }

  override def getRuntimeStatistics: Statistics = broadcast.runtimeStatistics
}

/**
 * A table cache query stage whose child is a [[InMemoryTableScanLike]].
 *
 * @param id the query stage id.
 * @param plan the underlying plan.
 */ //处理与内存表缓存（In-Memory Table Cache）相关的查询阶段的执行类
case class TableCacheQueryStageExec(
    override val id: Int,
    override val plan: SparkPlan) extends QueryStageExec {
  //表示当前查询阶段的内存表扫描操作
  @transient val inMemoryTableScan = plan match {
    case i: InMemoryTableScanLike => i
    case _ =>
      throw new IllegalStateException(s"wrong plan for table cache stage:\n ${plan.treeString}")
  }

  @transient
  private lazy val future: Future[Unit] = {
    if (inMemoryTableScan.isMaterialized) { //如果该表已经被物化（即缓存数据已经准备好），则返回一个已经完成的 Future.successful(())
      Future.successful(())
    } else {
      val rdd = inMemoryTableScan.baseCacheRDD() //如果表尚未物化
      sparkContext.submitJob(
        rdd,
        (_: Iterator[CachedBatch]) => (),
        (0 until rdd.getNumPartitions).toSeq,
        (_: Int, _: Unit) => (),
        ()
      )
    }
  }

  override protected def doMaterialize(): Future[Any] = future

  override def getRuntimeStatistics: Statistics = inMemoryTableScan.runtimeStatistics
}
