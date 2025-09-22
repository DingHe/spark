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

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.expressions.codegen.ByteCodeStats
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Command, CommandResult, CreateTableAsSelect, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceTableAsSelect, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.adaptive.{AdaptiveExecutionContext, InsertAdaptiveSparkPlan}
import org.apache.spark.sql.execution.bucketing.{CoalesceBucketsInJoin, DisableUnnecessaryBucketedScan}
import org.apache.spark.sql.execution.dynamicpruning.PlanDynamicPruningFilters
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, OffsetSeqMetadata, WatermarkPropagator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.Utils

/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
//主要用于执行SQL查询的工作流。它管理着从SQL查询的解析到执行计划生成的各个阶段
class QueryExecution(
    val sparkSession: SparkSession,
    val logical: LogicalPlan,  //SQL 查询的逻辑计划
    val tracker: QueryPlanningTracker = new QueryPlanningTracker,  //追踪 SQL 查询执行过程中的各个阶段，帮助调试和性能分析
    val mode: CommandExecutionMode.Value = CommandExecutionMode.ALL) extends Logging {

  val id: Long = QueryExecution.nextExecutionId


  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner  //将优化后的逻辑计划转换为物理计划

  def assertAnalyzed(): Unit = analyzed   //分析阶段，使用Analyzer
  //检查当前查询是否被支持，特别是在批处理模式下
  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }
  //查询已经经过分析阶段后的逻辑计划,懒加载，后面提交才执行
  lazy val analyzed: LogicalPlan = {
    val plan = executePhase(QueryPlanningTracker.ANALYSIS) {  //分析阶段
      // We can't clone `logical` here, which will reset the `_analyzed` flag.
      sparkSession.sessionState.analyzer.executeAndCheck(logical, tracker)  //使用Analyzer执行分析，并标志该逻辑计划已经分析
    }
    tracker.setAnalyzed(plan)
    plan
  }
  //根据执行模式 (mode) 确定是否要“急切执行”命令。如果模式是 NON_ROOT，则会在遍历查询时执行命令；如果是 ALL，则会执行所有命令
  lazy val commandExecuted: LogicalPlan = mode match {
    case CommandExecutionMode.NON_ROOT => analyzed.mapChildren(eagerlyExecuteCommands)
    case CommandExecutionMode.ALL => eagerlyExecuteCommands(analyzed)
    case CommandExecutionMode.SKIP => analyzed
  }
 //根据传入的命令类型返回命令的名称
  private def commandExecutionName(command: Command): String = command match {
    case _: CreateTableAsSelect => "create"
    case _: ReplaceTableAsSelect => "replace"
    case _: AppendData => "append"
    case _: OverwriteByExpression => "overwrite"
    case _: OverwritePartitionsDynamic => "overwritePartitions"
    case _ => "command"
  }
  //遍历查询计划中的命令节点，执行这些命令并返回 CommandResult。执行结果会被缓存在查询的执行计划中
  private def eagerlyExecuteCommands(p: LogicalPlan) = p transformDown {
    case c: Command =>
      // Since Command execution will eagerly take place here,
      // and in most cases be the bulk of time and effort,
      // with the rest of processing of the root plan being just outputting command results,
      // for eagerly executed commands we mark this place as beginning of execution.
      tracker.setReadyForExecution()
      //创建QueryExecution对象，此时的LogicalPlan已经分析过
      val qe = sparkSession.sessionState.executePlan(c, CommandExecutionMode.NON_ROOT)
      //commandExecutionName(c)返回当前命令的名称
      val result = SQLExecution.withNewExecutionId(qe, Some(commandExecutionName(c))) {
        qe.executedPlan.executeCollect()   //真正的执行逻辑在这里
      }
      CommandResult(
        qe.analyzed.output,
        qe.commandExecuted,
        qe.executedPlan,
        result)
    case other => other   //非命令节点，直接返回
  }
  //经过规范化的逻辑计划。规范化通常是为了优化缓存命中率。通过应用 planNormalizationRules 来生成
  // The plan that has been normalized by custom rules, so that it's more likely to hit cache.
  lazy val normalized: LogicalPlan = {
    val normalizationRules = sparkSession.sessionState.planNormalizationRules
    if (normalizationRules.isEmpty) {
      commandExecuted
    } else {
      val planChangeLogger = new PlanChangeLogger[LogicalPlan]()
      val normalized = normalizationRules.foldLeft(commandExecuted) { (p, rule) =>
        val result = rule.apply(p)
        planChangeLogger.logRule(rule.ruleName, p, result)
        result
      }
      planChangeLogger.logBatch("Plan Normalization", commandExecuted, normalized)
      normalized
    }
  }
  //使用缓存的数据生成新的逻辑计划。它会确保缓存数据的有效性。
  lazy val withCachedData: LogicalPlan = sparkSession.withActive {
    assertAnalyzed()
    assertSupported()
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    sparkSession.sharedState.cacheManager.useCachedData(normalized.clone())
  }

  def assertCommandExecuted(): Unit = commandExecuted
  //LogicalPlan 类型的属性，表示优化后的查询计划
  lazy val optimizedPlan: LogicalPlan = {
    // We need to materialize the commandExecuted here because optimizedPlan is also tracked under
    // the optimizing phase
    assertCommandExecuted()   //代表已经分析过
    executePhase(QueryPlanningTracker.OPTIMIZATION) {
      // clone the plan to avoid sharing the plan instance between different stages like analyzing,
      // optimizing and planning.
      val plan =
        sparkSession.sessionState.optimizer.executeAndTrack(withCachedData.clone(), tracker)
      // We do not want optimized plans to be re-analyzed as literals that have been constant
      // folded and such can cause issues during analysis. While `clone` should maintain the
      // `analyzed` state of the LogicalPlan, we set the plan as analyzed here as well out of
      // paranoia.
      plan.setAnalyzed()
      plan
    }
  }

  def assertOptimized(): Unit = optimizedPlan
  //生成物理计划 (SparkPlan) 的属性。物理计划是执行查询的最终计划，包含了具体的执行细节
  lazy val sparkPlan: SparkPlan = {
    // We need to materialize the optimizedPlan here because sparkPlan is also tracked under
    // the planning phase
    assertOptimized()
    executePhase(QueryPlanningTracker.PLANNING) {
      // Clone the logical plan here, in case the planner rules change the states of the logical
      // plan.
      //使用planner把逻辑计划转为物理计划，返回第一个物理计划
      QueryExecution.createSparkPlan(sparkSession, planner, optimizedPlan.clone())
    }
  }

  def assertSparkPlanPrepared(): Unit = sparkPlan

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  //表示已经准备好执行的物理计划
  lazy val executedPlan: SparkPlan = {
    // We need to materialize the optimizedPlan here, before tracking the planning phase, to ensure
    // that the optimization time is not counted as part of the planning phase.
    assertOptimized()
    val plan = executePhase(QueryPlanningTracker.PLANNING) {
      // clone the plan to avoid sharing the plan instance between different stages like analyzing,
      // optimizing and planning.
      //把物理计划执行前应用的规则应用到物理计划上，返回规则应用后的物理计划
      QueryExecution.prepareForExecution(preparations, sparkPlan.clone())
    }
    // Note: For eagerly executed command it might have already been called in
    // `eagerlyExecutedCommand` and is a noop here.
    tracker.setReadyForExecution()
    plan
  }

  def assertExecutedPlanPrepared(): Unit = executedPlan

  /**
   * Internal version of the RDD. Avoids copies and has no schema.
   * Note for callers: Spark may apply various optimization including reusing object: this means
   * the row is valid only for the iteration it is retrieved. You should avoid storing row and
   * accessing after iteration. (Calling `collect()` is one of known bad usage.)
   * If you want to store these rows into collection, please apply some converter or copy row
   * which produces new object per iteration.
   * Given QueryExecution is not a public class, end users are discouraged to use this: please
   * use `Dataset.rdd` instead where conversion will be applied.
   */
    //返回一个 RDD[InternalRow]，表示查询的执行结果。注意这个 RDD 没有包含模式信息，且不推荐直接存储
  lazy val toRdd: RDD[InternalRow] = new SQLExecutionRDD(
    executedPlan.execute(), sparkSession.sessionState.conf)

  /** Get the metrics observed during the execution of the query plan. */
  def observedMetrics: Map[String, Row] = CollectMetricsExec.collect(executedPlan)
  //物理计划执行前应用的规则
  protected def preparations: Seq[Rule[SparkPlan]] = {
    QueryExecution.preparations(sparkSession,
      Option(InsertAdaptiveSparkPlan(AdaptiveExecutionContext(sparkSession, this))), false)
  }
  //执行查询的某个阶段，并通过 tracker.measurePhase 来跟踪时间和性能
  protected def executePhase[T](phase: String)(block: => T): T = sparkSession.withActive {
    QueryExecution.withInternalError(s"The Spark SQL phase $phase failed with an internal error.") {
      tracker.measurePhase(phase)(block)
    }
  }
  //返回查询执行计划的简洁字符串表示
  def simpleString: String = {
    val concat = new PlanStringConcat()
    simpleString(false, SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def simpleString(
      formatted: Boolean,
      maxFields: Int,
      append: String => Unit): Unit = {
    append("== Physical Plan ==\n")
    if (formatted) {
      try {
        ExplainUtils.processPlan(executedPlan, append)
      } catch {
        case e: AnalysisException => append(e.toString)
        case e: IllegalArgumentException => append(e.toString)
      }
    } else {
      QueryPlan.append(executedPlan,
        append, verbose = false, addSuffix = false, maxFields = maxFields)
    }
    append("\n")
  }

  def explainString(mode: ExplainMode): String = {
    val concat = new PlanStringConcat()
    explainString(mode, SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def explainString(mode: ExplainMode, maxFields: Int, append: String => Unit): Unit = {
    val queryExecution = if (logical.isStreaming) {
      // This is used only by explaining `Dataset/DataFrame` created by `spark.readStream`, so the
      // output mode does not matter since there is no `Sink`.
      new IncrementalExecution(
        sparkSession, logical, OutputMode.Append(), "<unknown>",
        UUID.randomUUID, UUID.randomUUID, 0, None, OffsetSeqMetadata(0, 0),
        WatermarkPropagator.noop())
    } else {
      this
    }

    mode match {
      case SimpleMode =>
        queryExecution.simpleString(false, maxFields, append)
      case ExtendedMode =>
        queryExecution.toString(maxFields, append)
      case CodegenMode =>
        try {
          org.apache.spark.sql.execution.debug.writeCodegen(append, queryExecution.executedPlan)
        } catch {
          case e: AnalysisException => append(e.toString)
        }
      case CostMode =>
        queryExecution.stringWithStats(maxFields, append)
      case FormattedMode =>
        queryExecution.simpleString(formatted = true, maxFields = maxFields, append)
    }
  }

  private def writePlans(append: String => Unit, maxFields: Int): Unit = {
    val (verbose, addSuffix) = (true, false)
    append("== Parsed Logical Plan ==\n")
    QueryPlan.append(logical, append, verbose, addSuffix, maxFields)
    append("\n== Analyzed Logical Plan ==\n")
    try {
      if (analyzed.output.nonEmpty) {
        append(
          truncatedString(
            analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ", maxFields)
        )
        append("\n")
      }
      QueryPlan.append(analyzed, append, verbose, addSuffix, maxFields)
      append("\n== Optimized Logical Plan ==\n")
      QueryPlan.append(optimizedPlan, append, verbose, addSuffix, maxFields)
      append("\n== Physical Plan ==\n")
      QueryPlan.append(executedPlan, append, verbose, addSuffix, maxFields)
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  override def toString: String = withRedaction {
    val concat = new PlanStringConcat()
    toString(SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def toString(maxFields: Int, append: String => Unit): Unit = {
    writePlans(append, maxFields)
  }

  def stringWithStats: String = {
    val concat = new PlanStringConcat()
    stringWithStats(SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def stringWithStats(maxFields: Int, append: String => Unit): Unit = {
    // trigger to compute stats for logical plans
    try {
      // This will trigger to compute stats for all the nodes in the plan, including subqueries,
      // if the stats doesn't exist in the statsCache and update the statsCache corresponding
      // to the node.
      optimizedPlan.collectWithSubqueries {
        case plan => plan.stats
      }
    } catch {
      case e: AnalysisException => append(e.toString + "\n")
    }
    // only show optimized logical plan and physical plan
    append("== Optimized Logical Plan ==\n")
    QueryPlan.append(optimizedPlan, append, verbose = true, addSuffix = true, maxFields)
    append("\n== Physical Plan ==\n")
    QueryPlan.append(executedPlan, append, verbose = true, addSuffix = false, maxFields)
    append("\n")
  }

  /**
   * Redact the sensitive information in the given string.
   */
    //对查询执行中的敏感信息进行屏蔽（如密码、敏感的路径等）
  private def withRedaction(message: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, message)
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
  // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }

    /**
     * Get WholeStageCodegenExec subtrees and the codegen in a query plan
     *
     * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
     */
    def codegenToSeq(): Seq[(String, String, ByteCodeStats)] = {
      org.apache.spark.sql.execution.debug.codegenStringSeq(executedPlan)
    }

    /**
     * Dumps debug information about query execution into the specified file.
     *
     * @param path path of the file the debug info is written to.
     * @param maxFields maximum number of fields converted to string representation.
     * @param explainMode the explain mode to be used to generate the string
     *                    representation of the plan.
     */
    def toFile(
        path: String,
        maxFields: Int = Int.MaxValue,
        explainMode: Option[String] = None): Unit = {
      val filePath = new Path(path)
      val fs = filePath.getFileSystem(sparkSession.sessionState.newHadoopConf())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath)))
      try {
        val mode = explainMode.map(ExplainMode.fromString(_)).getOrElse(ExtendedMode)
        explainString(mode, maxFields, writer.write)
        if (mode != CodegenMode) {
          writer.write("\n== Whole Stage Codegen ==\n")
          org.apache.spark.sql.execution.debug.writeCodegen(writer.write, executedPlan)
        }
        log.info(s"Debug information was written at: $filePath")
      } finally {
        writer.close()
      }
    }
  }
}

/**
 * SPARK-35378: Commands should be executed eagerly so that something like `sql("INSERT ...")`
 * can trigger the table insertion immediately without a `.collect()`. To avoid end-less recursion
 * we should use `NON_ROOT` when recursively executing commands. Note that we can't execute
 * a query plan with leaf command nodes, because many commands return `GenericInternalRow`
 * and can't be put in a query plan directly, otherwise the query engine may cast
 * `GenericInternalRow` to `UnsafeRow` and fail. When running EXPLAIN, or commands inside other
 * command, we should use `SKIP` to not eagerly trigger the command execution.
 */
object CommandExecutionMode extends Enumeration {
  val SKIP, NON_ROOT, ALL = Value
}
//SKIP  表示跳过命令的执行。它通常用于 EXPLAIN 或者嵌套在其他命令中的命令执行，避免在这些场景下命令立即执行。
// 例如，如果一个查询是嵌套在另一个查询中运行，或者我们在调试查询计划时使用 EXPLAIN，就可以使用 SKIP 来避免不必要的命令执行
//NON_ROOT  递归执行命令时使用，特别是在命令节点是查询树的非根节点时。使用这个模式可以避免在递归中出现无限递归的问题。它确保命令被执行，但不执行根节点的命令
//ALL 表示所有的命令都应该被立即执行。这意味着任何命令（无论是在根节点还是非根节点）都会在查询过程中立即执行，适用于需要确保某些操作（如 INSERT 操作）立刻生效的情况
object QueryExecution {
  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  /**
   * Construct a sequence of rules that are used to prepare a planned [[SparkPlan]] for execution.
   * These rules will make sure subqueries are planned, make sure the data partitioning and ordering
   * are correct, insert whole stage code gen, and try to reduce the work done by reusing exchanges
   * and subqueries.
   */
  // 构建一系列规则，以便为执行计划（SparkPlan）做好准备。
  // 这个方法会确保子查询被规划好、数据分区和排序是正确的、插入代码生成（code generation）等
  private[execution] def preparations(
      sparkSession: SparkSession,
      adaptiveExecutionRule: Option[InsertAdaptiveSparkPlan] = None, //用于自适应查询计划。自适应执行允许 Spark 动态调整查询执行计划以提高性能，特别是在执行过程中调整分区大小、排序等
      subquery: Boolean): Seq[Rule[SparkPlan]] = { //指示当前是否是子查询。如果是子查询，则不应用一些需要在主查询中使用的优化规则
    // `AdaptiveSparkPlanExec` is a leaf node. If inserted, all the following rules will be no-op
    // as the original plan is hidden behind `AdaptiveSparkPlanExec`.
    adaptiveExecutionRule.toSeq ++  //toSeq 方法将 Option 类型的规则转换为一个空序列或包含规则的序列
    Seq(
      CoalesceBucketsInJoin,  //优化规则，用于合并分桶（buckets）以优化 Join 操作
      PlanDynamicPruningFilters(sparkSession),  //动态过滤规则。它会为查询计划插入动态修剪的过滤条件，通常用于优化基于数据的连接操作（如 Join）
      PlanSubqueries(sparkSession), //确保子查询被正确地规划和优化
      RemoveRedundantProjects,  //删除冗余的投影（Project）操作
      EnsureRequirements(), //确保查询计划满足所需的物理要求。比如，确保某些操作（如 Join 或 Sort）的输入符合预期的分区要求和排序要求
      // `ReplaceHashWithSortAgg` needs to be added after `EnsureRequirements` to guarantee the
      // sort order of each node is checked to be valid.
      ReplaceHashWithSortAgg,
      // `RemoveRedundantSorts` and `RemoveRedundantWindowGroupLimits` needs to be added after
      // `EnsureRequirements` to guarantee the same number of partitions when instantiating
      // PartitioningCollection.
      RemoveRedundantSorts,
      RemoveRedundantWindowGroupLimits,
      DisableUnnecessaryBucketedScan,
      ApplyColumnarRulesAndInsertTransitions(
        sparkSession.sessionState.columnarRules, outputsColumnar = false),
      CollapseCodegenStages()) ++
      (if (subquery) {  //如果是子查询，则不应用 ReuseExchangeAndSubquery 规则
        Nil
      } else {
        Seq(ReuseExchangeAndSubquery)
      })
  }

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  //提交前进行准备工作，进行一些分区排序方面的处理，确保 SparkPlan各节点能够正确 执行
  private[execution] def prepareForExecution(
      preparations: Seq[Rule[SparkPlan]],
      plan: SparkPlan): SparkPlan = {
    val planChangeLogger = new PlanChangeLogger[SparkPlan]()
    val preparedPlan = preparations.foldLeft(plan) { case (sp, rule) =>
      val result = rule.apply(sp)
      planChangeLogger.logRule(rule.ruleName, sp, result)
      result
    }
    planChangeLogger.logBatch("Preparations", plan, preparedPlan)
    preparedPlan
  }

  /**
   * Transform a [[LogicalPlan]] into a [[SparkPlan]].
   *
   * Note that the returned physical plan still needs to be prepared for execution.
   */
  def createSparkPlan(
      sparkSession: SparkSession,
      planner: SparkPlanner,
      plan: LogicalPlan): SparkPlan = {
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(plan)).next()  //目前是直接返回第一个执行计划，以后会选择best
  }

  /**
   * Prepare the [[SparkPlan]] for execution.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    prepareForExecution(preparations(spark, subquery = true), plan)
  }

  /**
   * Transform the subquery's [[LogicalPlan]] into a [[SparkPlan]] and prepare the resulting
   * [[SparkPlan]] for execution.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: LogicalPlan): SparkPlan = {
    val sparkPlan = createSparkPlan(spark, spark.sessionState.planner, plan.clone())
    prepareExecutedPlan(spark, sparkPlan)
  }

  /**
   * Prepare the [[SparkPlan]] for execution using exists adaptive execution context.
   * This method is only called by [[PlanAdaptiveDynamicPruningFilters]].
   */
  def prepareExecutedPlan(
      session: SparkSession,
      plan: LogicalPlan,
      context: AdaptiveExecutionContext): SparkPlan = {
    val sparkPlan = createSparkPlan(session, session.sessionState.planner, plan.clone())
    val preparationRules = preparations(session, Option(InsertAdaptiveSparkPlan(context)), true)
    prepareForExecution(preparationRules, sparkPlan.clone())
  }

  /**
   * Converts asserts, null pointer exceptions to internal errors.
   */
  private[sql] def toInternalError(msg: String, e: Throwable): Throwable = e match {
    case e @ (_: java.lang.NullPointerException | _: java.lang.AssertionError) =>
      SparkException.internalError(
        msg + " You hit a bug in Spark or the Spark plugins you use. Please, report this bug " +
          "to the corresponding communities or vendors, and provide the full stack trace.",
        e)
    case e: Throwable =>
      e
  }

  /**
   * Catches asserts, null pointer exceptions, and converts them to internal errors.
   */
  //执行block的代码，如果出现错误，则抛出msg异常信息
  private[sql] def withInternalError[T](msg: String)(block: => T): T = {
    try {
      block
    } catch {
      case e: Throwable => throw toInternalError(msg, e)
    }
  }
}
