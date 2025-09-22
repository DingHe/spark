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

package org.apache.spark.sql.execution.exchange

import java.util.UUID
import java.util.concurrent._

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.NANOSECONDS
import scala.util.control.NonFatal

import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.{HashedRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.{SparkFatalException, ThreadUtils}

/**
 * Common trait for all broadcast exchange implementations to facilitate pattern matching.
 */
//用于广播交换（Broadcast Exchange）实现的公共特质（trait），目的是为各种广播实现提供统一的结构，方便进行模式匹配（pattern matching）。
// 广播交换通常用于小表的广播式分发，以加速 Broadcast Join 操作。
trait BroadcastExchangeLike extends Exchange {

  /**
   * The broadcast run ID in job tag
   */
  val runId: UUID = UUID.randomUUID

  /**
   * The broadcast job tag
   */
    //用于标识广播作业的标签
  def jobTag: String = s"broadcast exchange (runId ${runId.toString})"

  /**
   * The asynchronous job that prepares the broadcast relation.
   */
  //该方法返回一个异步任务（Future），用于准备广播数据（Broadcast[Any]）
  def relationFuture: Future[broadcast.Broadcast[Any]]

  /**
   * The asynchronous job that materializes the broadcast. It's used for registering callbacks on
   * `relationFuture`. Note that calling this method may not start the execution of broadcast job.
   * It also does the preparations work, such as waiting for the subqueries.
   */
    //提交广播任务，返回一个 Future 对象，代表广播数据的计算结果
  final def submitBroadcastJob: scala.concurrent.Future[broadcast.Broadcast[Any]] = executeQuery {
    completionFuture
  }
  //具体的广播执行逻辑由子类提供，BroadcastExchangeLike 只定义了通用接口
  protected def completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]]

  /**
   * Returns the runtime statistics after broadcast materialization.
   */
  def runtimeStatistics: Statistics
}

/**
 * A [[BroadcastExchangeExec]] collects, transforms and finally broadcasts the result of
 * a transformed SparkPlan.
 */
//将转换后的数据以广播变量的形式发送给每个执行器，从而提高小型表与大型表进行 Join 操作的效率
case class BroadcastExchangeExec(
    mode: BroadcastMode, //决定广播数据的格式，支持 IdentityBroadcastMode 和 HashedRelationBroadcastMode
    child: SparkPlan) extends BroadcastExchangeLike { //子计划，表示需要广播的数据的计算逻辑
  import BroadcastExchangeExec._

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode) //广播分区策略，确保广播数据的分区与 BroadcastMode 匹配
  //用于 Spark 计划的规范化，移除不影响语义的属性，确保计划可以被正确比较或缓存
  override def doCanonicalize(): SparkPlan = {
    BroadcastExchangeExec(mode.canonicalized, child.canonicalized)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics("numOutputRows").value
    Statistics(dataSize, Some(rowCount))
  }
  //保存异步广播结果，确保广播数据完成后可供其他操作使用。
  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  override lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    promise.future

  @transient
  private val timeout: Long = conf.broadcastTimeout
  //广播表的最大行数，超过此值将抛出异常，防止广播过大的数据
  @transient
  private lazy val maxBroadcastRows = mode match {
    case HashedRelationBroadcastMode(key, _)
      // NOTE: LongHashedRelation is used for single key with LongType. This should be kept
      // consistent with HashedRelation.apply.
      if !(key.length == 1 && key.head.dataType == LongType) =>
      // Since the maximum number of keys that BytesToBytesMap supports is 1 << 29,
      // and only 70% of the slots can be used before growing in UnsafeHashedRelation,
      // here the limitation should not be over 341 million.
      (BytesToBytesMap.MAX_CAPACITY / 1.5).toLong
    case _ => 512000000
  }
  //执行广播的核心逻辑
  @transient
  override lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      session, BroadcastExchangeExec.executionContext) {
          try {
            // Setup a job tag here so later it may get cancelled by tag if necessary.
            sparkContext.addJobTag(jobTag)
            sparkContext.setInterruptOnCancel(true)
            val beforeCollect = System.nanoTime()
            // Use executeCollect/executeCollectIterator to avoid conversion to Scala types
            val (numRows, input) = child.executeCollectIterator() //从 child 计划中执行计算，获取数据
            longMetric("numOutputRows") += numRows
            if (numRows >= maxBroadcastRows) {
              throw QueryExecutionErrors.cannotBroadcastTableOverMaxTableRowsError(
                maxBroadcastRows, numRows)
            }

            val beforeBuild = System.nanoTime()
            longMetric("collectTime") += NANOSECONDS.toMillis(beforeBuild - beforeCollect)

            // Construct the relation.
            val relation = mode.transform(input, Some(numRows))  //根据 BroadcastMode 转换数据（如 HashedRelation 或原始数据）

            val dataSize = relation match {
              case map: HashedRelation =>
                map.estimatedSize
              case arr: Array[InternalRow] =>
                arr.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
              case _ =>
                throw new SparkException("[BUG] BroadcastMode.transform returned unexpected " +
                  s"type: ${relation.getClass.getName}")
            }

            longMetric("dataSize") += dataSize
            if (dataSize >= MAX_BROADCAST_TABLE_BYTES) {
              throw QueryExecutionErrors.cannotBroadcastTableOverMaxTableBytesError(
                MAX_BROADCAST_TABLE_BYTES, dataSize)
            }

            val beforeBroadcast = System.nanoTime()
            longMetric("buildTime") += NANOSECONDS.toMillis(beforeBroadcast - beforeBuild)

            // SPARK-39983 - Broadcast the relation without caching the unserialized object.
            //使用 sparkContext.broadcastInternal 进行广播，避免序列化对象被缓存
            val broadcasted = sparkContext.broadcastInternal(relation, serializedOnly = true)
            longMetric("broadcastTime") += NANOSECONDS.toMillis(
              System.nanoTime() - beforeBroadcast)
            val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
            SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
            promise.trySuccess(broadcasted)
            broadcasted
          } catch {
            // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
            // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
            // will catch this exception and re-throw the wrapped fatal throwable.
            case oe: OutOfMemoryError =>
              val tables = child.collect { case f: FileSourceScanExec => f.tableIdentifier }.flatten
              val ex = new SparkFatalException(
                QueryExecutionErrors.notEnoughMemoryToBuildAndBroadcastTableError(oe, tables))
              promise.tryFailure(ex)
              throw ex
            case e if !NonFatal(e) =>
              val ex = new SparkFatalException(e)
              promise.tryFailure(ex)
              throw ex
            case e: Throwable =>
              promise.tryFailure(e)
              throw e
          }
    }
  }
  //提前触发广播任务，确保在执行阶段数据已准备就绪
  override protected def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }
 //BroadcastExchangeExec 仅支持广播，不支持直接输出 RDD，因此直接抛出异常。
  override protected def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.executeCodePathUnsupportedError("BroadcastExchange")
  }
  //获取广播结果，若未完成等待指定超时时间
  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobsWithTag(jobTag)
          relationFuture.cancel(true)
        }
        throw QueryExecutionErrors.executeBroadcastTimeoutError(timeout, Some(ex))
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): BroadcastExchangeExec =
    copy(child = newChild)
}

object BroadcastExchangeExec {
  val MAX_BROADCAST_TABLE_BYTES = 8L << 30

  private[execution] val executionContext = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("broadcast-exchange",
        SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
