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

import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Future => JFuture}
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{ErrorMessageFormat, JobArtifactSet, SparkContext, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.internal.config.{SPARK_DRIVER_PREFIX, SPARK_EXECUTOR_PREFIX}
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.internal.StaticSQLConf.SQL_EVENT_TRUNCATE_LENGTH
import org.apache.spark.util.Utils

object SQLExecution {

  val EXECUTION_ID_KEY = "spark.sql.execution.id"
  val EXECUTION_ROOT_ID_KEY = "spark.sql.execution.root.id"

  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()

  def getQueryExecution(executionId: Long): QueryExecution = {
    executionIdToQueryExecution.get(executionId)
  }

  private val testing = sys.props.contains(IS_TESTING.key)

  private[sql] def checkSQLExecutionId(sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext
    // only throw an exception during tests. a missing execution ID should not fail a job.
    if (testing && sc.getLocalProperty(EXECUTION_ID_KEY) == null) {
      // Attention testers: when a test fails with this exception, it means that the action that
      // started execution of a query didn't call withNewExecutionId. The execution ID should be
      // set by calling withNewExecutionId in the action that begins execution, like
      // Dataset.collect or DataFrameWriter.insertInto.
      throw new IllegalStateException("Execution ID should be set")
    }
  }

  /**
   * Wrap an action that will execute "queryExecution" to track all Spark jobs in the body so that
   * we can connect them with an execution.
   */
  //包装一个执行动作body并跟踪 Spark 作业。这个方法的主要目的是生成并管理执行 ID（executionId），记录执行上下文，
  // 发布执行开始和结束的事件，以及处理 Spark 作业的生命周期
  def withNewExecutionId[T](
      queryExecution: QueryExecution,
      name: Option[String] = None)(body: => T): T = queryExecution.sparkSession.withActive {
    val sparkSession = queryExecution.sparkSession
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(EXECUTION_ID_KEY) //保存当前的执行 ID，稍后恢复
    val executionId = SQLExecution.nextExecutionId
    sc.setLocalProperty(EXECUTION_ID_KEY, executionId.toString)  //设置当前执行的 executionId
    // Track the "root" SQL Execution Id for nested/sub queries. The current execution is the
    // root execution if the root execution ID is null.
    // And for the root execution, rootExecutionId == executionId.
    if (sc.getLocalProperty(EXECUTION_ROOT_ID_KEY) == null) { //在嵌套查询中，根执行 ID 是整个执行链的起点
      sc.setLocalProperty(EXECUTION_ROOT_ID_KEY, executionId.toString)
    }
    val rootExecutionId = sc.getLocalProperty(EXECUTION_ROOT_ID_KEY).toLong
    executionIdToQueryExecution.put(executionId, queryExecution)
    try {
      // sparkContext.getCallSite() would first try to pick up any call site that was previously
      // set, then fall back to Utils.getCallSite(); call Utils.getCallSite() directly on
      // streaming queries would give us call site like "run at <unknown>:0"
      val callSite = sc.getCallSite()

      val truncateLength = sc.conf.get(SQL_EVENT_TRUNCATE_LENGTH)

      val desc = Option(sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION))
        .filter(_ => truncateLength > 0)
        .map { sqlStr =>
          val redactedStr = Utils
            .redact(sparkSession.sessionState.conf.stringRedactionPattern, sqlStr)
          redactedStr.substring(0, Math.min(truncateLength, redactedStr.length))
        }.getOrElse(callSite.shortForm)

      val planDescriptionMode =
        ExplainMode.fromString(sparkSession.sessionState.conf.uiExplainMode)
      //处理配置项和敏感信息
      val globalConfigs = sparkSession.sharedState.conf.getAll.toMap
      val modifiedConfigs = sparkSession.sessionState.conf.getAllConfs
        .filterNot { case (key, value) =>
          key.startsWith(SPARK_DRIVER_PREFIX) ||
            key.startsWith(SPARK_EXECUTOR_PREFIX) ||
            globalConfigs.get(key).contains(value)
        }
      val redactedConfigs = sparkSession.sessionState.conf.redactOptions(modifiedConfigs)

      withSQLConfPropagated(sparkSession) {
        var ex: Option[Throwable] = None
        val startTime = System.nanoTime()
        try {
          sc.listenerBus.post(SparkListenerSQLExecutionStart(
            executionId = executionId,
            rootExecutionId = Some(rootExecutionId),
            description = desc,
            details = callSite.longForm,
            physicalPlanDescription = queryExecution.explainString(planDescriptionMode),
            // `queryExecution.executedPlan` triggers query planning. If it fails, the exception
            // will be caught and reported in the `SparkListenerSQLExecutionEnd`
            sparkPlanInfo = SparkPlanInfo.fromSparkPlan(queryExecution.executedPlan),
            time = System.currentTimeMillis(),
            modifiedConfigs = redactedConfigs,
            jobTags = sc.getJobTags()
          ))
          body    //真正执行的代码逻辑在这里执行
        } catch {
          case e: Throwable =>
            ex = Some(e)
            throw e
        } finally {
          val endTime = System.nanoTime()
          val errorMessage = ex.map {
            case e: SparkThrowable =>
              SparkThrowableHelper.getMessage(e, ErrorMessageFormat.PRETTY)
            case e =>
              Utils.exceptionString(e)
          }
          val event = SparkListenerSQLExecutionEnd(
            executionId,
            System.currentTimeMillis(),
            // Use empty string to indicate no error, as None may mean events generated by old
            // versions of Spark.
            errorMessage.orElse(Some("")))
          // Currently only `Dataset.withAction` and `DataFrameWriter.runCommand` specify the `name`
          // parameter. The `ExecutionListenerManager` only watches SQL executions with name. We
          // can specify the execution name in more places in the future, so that
          // `QueryExecutionListener` can track more cases.
          event.executionName = name
          event.duration = endTime - startTime
          event.qe = queryExecution
          event.executionFailure = ex
          sc.listenerBus.post(event)
        }
      }
    } finally {
      executionIdToQueryExecution.remove(executionId)
      sc.setLocalProperty(EXECUTION_ID_KEY, oldExecutionId)
      // Unset the "root" SQL Execution Id once the "root" SQL execution completes.
      // The current execution is the root execution if rootExecutionId == executionId.
      if (sc.getLocalProperty(EXECUTION_ROOT_ID_KEY) == executionId.toString) {
        sc.setLocalProperty(EXECUTION_ROOT_ID_KEY, null)
      }
    }
  }

  /**
   * Wrap an action with a known executionId. When running a different action in a different
   * thread from the original one, this method can be used to connect the Spark jobs in this action
   * with the known executionId, e.g., `BroadcastExchangeExec.relationFuture`.
   */
  def withExecutionId[T](sparkSession: SparkSession, executionId: String)(body: => T): T = {
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    withSQLConfPropagated(sparkSession) {
      try {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId)
        body
      } finally {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, oldExecutionId)
      }
    }
  }

  /**
   * Wrap an action with specified SQL configs. These configs will be propagated to the executor
   * side via job local properties.
   */
  def withSQLConfPropagated[T](sparkSession: SparkSession)(body: => T): T = {
    val sc = sparkSession.sparkContext
    // Set all the specified SQL configs to local properties, so that they can be available at
    // the executor side.
    val allConfigs = sparkSession.sessionState.conf.getAllConfs
    val originalLocalProps = allConfigs.collect {
      case (key, value) if key.startsWith("spark") =>
        val originalValue = sc.getLocalProperty(key)
        sc.setLocalProperty(key, value)
        (key, originalValue)
    }

    try {
      body
    } finally {
      for ((key, value) <- originalLocalProps) {
        sc.setLocalProperty(key, value)
      }
    }
  }

  /**
   * Wrap passed function to ensure necessary thread-local variables like
   * SparkContext local properties are forwarded to execution thread
   */
    //主要作用是确保 Spark 上下文和本地属性能够在执行任务的线程中被正确传递和恢复
  def withThreadLocalCaptured[T](
      sparkSession: SparkSession, exec: ExecutorService) (body: => T): JFuture[T] = {
    val activeSession = sparkSession
    val sc = sparkSession.sparkContext
    val localProps = Utils.cloneProperties(sc.getLocalProperties)
    val artifactState = JobArtifactSet.getCurrentJobArtifactState.orNull  //获取当前作业的工件状态
    exec.submit(() => JobArtifactSet.withActiveJobArtifactState(artifactState) {
      val originalSession = SparkSession.getActiveSession
      val originalLocalProps = sc.getLocalProperties
      SparkSession.setActiveSession(activeSession)
      sc.setLocalProperties(localProps)
      val res = body   //执行传入的body代码
      // reset active session and local props.
      sc.setLocalProperties(originalLocalProps)
      if (originalSession.nonEmpty) {
        SparkSession.setActiveSession(originalSession.get)
      } else {
        SparkSession.clearActiveSession()
      }
      res
    })
  }
}
