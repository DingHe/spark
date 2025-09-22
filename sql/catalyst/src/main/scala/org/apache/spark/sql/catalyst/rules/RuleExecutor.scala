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

package org.apache.spark.sql.catalyst.rules

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

object RuleExecutor {
  //用于跟踪规则执行的计量器对象（QueryExecutionMetering），记录规则执行的时间和计数等信息
  protected val queryExecutionMeter = QueryExecutionMetering()

  /** Dump statistics about time spent running specific rules. */
    //返回一个字符串，表示每个规则执行的时间统计信息
  def dumpTimeSpent(): String = {
    queryExecutionMeter.dumpTimeSpent()
  }

  /** Resets statistics about time spent running specific rules */
  def resetMetrics(): Unit = {
    queryExecutionMeter.resetMetrics()
  }

  def getCurrentMetrics(): QueryExecutionMetrics = {
    queryExecutionMeter.getMetrics()
  }
}
//用于记录执行规则时的计划变更（plan changes）
class PlanChangeLogger[TreeType <: TreeNode[_]] extends Logging {

  private val logLevel = SQLConf.get.planChangeLogLevel
  //获取一个列表，指定哪些规则的变更需要记录
  private val logRules = SQLConf.get.planChangeRules.map(Utils.stringToSeq)
  //获取一个列表，指定哪些规则批次（batches）需要记录变更
  private val logBatches = SQLConf.get.planChangeBatches.map(Utils.stringToSeq)
  //记录规则执行后的计划变更。如果变更前后计划不同，并且该规则需要记录，则输出规则应用的日志信息
  def logRule(ruleName: String, oldPlan: TreeType, newPlan: TreeType): Unit = {
    if (!newPlan.fastEquals(oldPlan)) {
      if (logRules.isEmpty || logRules.get.contains(ruleName)) {
        def message(): String = {
          s"""
             |=== Applying Rule $ruleName ===
             |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
           """.stripMargin
        }

        logBasedOnLevel(message)
      }
    }
  }
 //记录规则批次执行后的计划变更。如果批次中的所有规则都没有改变计划，记录批次没有效果的信息
  def logBatch(batchName: String, oldPlan: TreeType, newPlan: TreeType): Unit = {
    if (logBatches.isEmpty || logBatches.get.contains(batchName)) {
      def message(): String = {
        if (!oldPlan.fastEquals(newPlan)) {
          s"""
             |=== Result of Batch $batchName ===
             |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
          """.stripMargin
        } else {
          s"Batch $batchName has no effect."
        }
      }

      logBasedOnLevel(message)
    }
  }
  //记录所有执行规则的统计信息（如总执行次数、总时间等）
  def logMetrics(metrics: QueryExecutionMetrics): Unit = {
    val totalTime = metrics.time / NANOS_PER_SECOND.toDouble
    val totalTimeEffective = metrics.timeEffective / NANOS_PER_SECOND.toDouble
    val message =
      s"""
         |=== Metrics of Executed Rules ===
         |Total number of runs: ${metrics.numRuns}
         |Total time: $totalTime seconds
         |Total number of effective runs: ${metrics.numEffectiveRuns}
         |Total time of effective runs: $totalTimeEffective seconds
      """.stripMargin

    logBasedOnLevel(message)
  }
  //根据 logLevel 来确定日志输出的级别（TRACE, DEBUG, INFO, WARN, ERROR）
  private def logBasedOnLevel(f: => String): Unit = {
    logLevel match {
      case "TRACE" => logTrace(f)
      case "DEBUG" => logDebug(f)
      case "INFO" => logInfo(f)
      case "WARN" => logWarning(f)
      case "ERROR" => logError(f)
      case _ => logTrace(f)
    }
  }
}

abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   */
  //定义了规则执行的策略，可以有不同的执行策略
  abstract class Strategy {

    /** The maximum number of executions. */
    def maxIterations: Int  //规则最大执行次数

    /** Whether to throw exception when exceeding the maximum number. */
    def errorOnExceed: Boolean = false  //当超过最大执行次数时，是否抛出异常

    /** The key of SQLConf setting to tune maxIterations */
    def maxIterationsSetting: String = null  //SQL 配置中指定的最大执行次数参数键
  }
  //表示规则只执行一次，即最大迭代次数为 1
  /** A strategy that is run once and idempotent. */
  case object Once extends Strategy { val maxIterations = 1 }

  /**
   * A strategy that runs until fix point or maxIterations times, whichever comes first.
   * Especially, a FixedPoint(1) batch is supposed to run only once.
   */
  //表示规则会一直执行，直到达到固定点（即计划不再发生变化），或者最大迭代次数达到。
  case class FixedPoint(
    override val maxIterations: Int,
    override val errorOnExceed: Boolean = false,
    override val maxIterationsSetting: String = null) extends Strategy

  /** A batch of rules. */
  //规则批次，表示一组规则（rules）和它们的执行策略（strategy）。每个批次有一个名称（name）
  protected[catalyst] case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  protected def batches: Seq[Batch]  //一个序列（Seq），包含多个规则批次（Batch）。子类需要实现这个方法，定义具体的规则批次

  /** Once batches that are excluded in the idempotence checker */
  protected val excludedOnceBatches: Set[String] = Set.empty  //一个集合，存储不需要进行幂等性检查的 "Once" 策略批次的名称

  /**
   * Defines a validate function that validates the plan changes after the execution of each rule,
   * to make sure these rules make valid changes to the plan. For example, we can check whether
   * a plan is still resolved after each rule in `Optimizer`, so that we can catch rules that
   * turn the plan into unresolved.
   */
    //一个用于验证规则应用后的计划变更是否有效的函数。例如，可以检查是否仍然是有效的查询计划
  protected def validatePlanChanges(
      previousPlan: TreeType,
      currentPlan: TreeType): Option[String] = None

  /**
   * Util method for checking whether a plan remains the same if re-optimized.
   */
    //检查一个规则批次是否满足幂等性条件（即多次应用同一批次时结果不变）。如果违反了幂等性条件，会抛出异常
  private def checkBatchIdempotence(batch: Batch, plan: TreeType): Unit = {
    val reOptimized = batch.rules.foldLeft(plan) { case (p, rule) => rule(p) }
    if (!plan.fastEquals(reOptimized)) {
      throw QueryExecutionErrors.onceStrategyIdempotenceIsBrokenForBatchError(
        batch.name, plan, reOptimized)
    }
  }

  /**
   * Executes the batches of rules defined by the subclass, and also tracks timing info for each
   * rule using the provided tracker.
   * @see [[execute]]
   */
    //执行规则批次并跟踪执行过程中的时间和效果信息。会通过 QueryPlanningTracker 记录每个规则的执行时间。
  def executeAndTrack(plan: TreeType, tracker: QueryPlanningTracker): TreeType = {
    QueryPlanningTracker.withTracker(tracker) {
      execute(plan)
    }
  }

  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   */
    //主要功能是执行由子类定义的一系列规则批次，并根据策略控制规则的执行次数、顺序等。
  // 整个方法执行过程中，还会对执行时间、计划变化等信息进行记录和跟踪
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    val queryExecutionMetrics = RuleExecutor.queryExecutionMeter
    val planChangeLogger = new PlanChangeLogger[TreeType]()
    val tracker: Option[QueryPlanningTracker] = QueryPlanningTracker.get
    val beforeMetrics = RuleExecutor.getCurrentMetrics()

    val enableValidation = SQLConf.get.getConf(SQLConf.PLAN_CHANGE_VALIDATION)
    // Validate the initial input.
    if (Utils.isTesting || enableValidation) {
      validatePlanChanges(plan, plan) match { //验证规则执行前后的变化
        case Some(msg) =>
          val ruleExecutorName = this.getClass.getName.stripSuffix("$")
          throw new SparkException(
            errorClass = "PLAN_VALIDATION_FAILED_RULE_EXECUTOR",
            messageParameters = Map("ruleExecutor" -> ruleExecutorName, "reason" -> msg),
            cause = null)
        case _ =>
      }
    }
    //遍历所有定义的规则批次，每个批次包含一组规则和执行策略
    batches.foreach { batch =>
      val batchStartPlan = curPlan  //记录当前批次开始时的查询计划
      var iteration = 1
      var lastPlan = curPlan       //记录上一次的查询计划，方便比较计划是否发生变化
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            val result = rule(plan)    //执行规则
            val runTime = System.nanoTime() - startTime
            val effective = !result.fastEquals(plan)

            if (effective) {
              queryExecutionMetrics.incNumEffectiveExecution(rule.ruleName)
              queryExecutionMetrics.incTimeEffectiveExecutionBy(rule.ruleName, runTime)
              planChangeLogger.logRule(rule.ruleName, plan, result)
              // Run the plan changes validation after each rule.
              if (Utils.isTesting || enableValidation) {
                validatePlanChanges(plan, result) match {
                  case Some(msg) =>
                    throw new SparkException(
                      errorClass = "PLAN_VALIDATION_FAILED_RULE_IN_BATCH",
                      messageParameters = Map(
                        "rule" -> rule.ruleName,
                        "batch" -> batch.name,
                        "reason" -> msg),
                      cause = null)
                  case _ =>
                }
              }
            }
            queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime)
            queryExecutionMetrics.incNumExecution(rule.ruleName)

            // Record timing information using QueryPlanningTracker
            tracker.foreach(_.recordRuleInvocation(rule.ruleName, runTime, effective))

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            val endingMsg = if (batch.strategy.maxIterationsSetting == null) {
              "."
            } else {
              s", please set '${batch.strategy.maxIterationsSetting}' to a larger value."
            }
            val message = s"Max iterations (${iteration - 1}) reached for batch ${batch.name}" +
              s"$endingMsg"
            if (Utils.isTesting || batch.strategy.errorOnExceed) {
              throw new RuntimeException(message)
            } else {
              logWarning(message)
            }
          }
          // Check idempotence for Once batches.
          if (batch.strategy == Once &&
            Utils.isTesting && !excludedOnceBatches.contains(batch.name)) {
            checkBatchIdempotence(batch, curPlan)
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      planChangeLogger.logBatch(batch.name, batchStartPlan, curPlan)
    }
    planChangeLogger.logMetrics(RuleExecutor.getCurrentMetrics() - beforeMetrics)

    curPlan
  }
}
