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

// RuleExecutor 是 Apache Spark Catalyst 引擎的核心组件之一。它是所有查询计划转换（如逻辑优化、物理计划生成）的底层驱动框架。
// 其核心职能是：有序、可控地将一组转换规则（Rules）应用到树状结构（通常是 SQL 的逻辑计划或物理计划）上。
// 它就像一条自动化流水线：
// 输入：一棵树（TreeNode）。
// 处理：按照预设的顺序、批次（Batch）和迭代策略（Strategy）运行规则。
// 输出：一棵经过优化或转换后的新树。
object RuleExecutor {
  // 持有一个 QueryExecutionMetering 实例，全局统计所有规则的执行时长、执行次数。
  protected val queryExecutionMeter = QueryExecutionMetering()

  /** Dump statistics about time spent running specific rules. */
  // 打印所有规则执行耗时的统计信息。
  def dumpTimeSpent(): String = {
    queryExecutionMeter.dumpTimeSpent()
  }

  /** Resets statistics about time spent running specific rules */
  // 重置所有计量数据。
  def resetMetrics(): Unit = {
    queryExecutionMeter.resetMetrics()
  }

  def getCurrentMetrics(): QueryExecutionMetrics = {
    queryExecutionMeter.getMetrics()
  }
}
// 计划变更记录器
// 用于监控规则应用前后，计划树发生了哪些具体变化。
class PlanChangeLogger[TreeType <: TreeNode[_]] extends Logging {

  private val logLevel = SQLConf.get.planChangeLogLevel


  private val logRules = SQLConf.get.planChangeRules.map(Utils.stringToSeq)

  private val logBatches = SQLConf.get.planChangeBatches.map(Utils.stringToSeq)
  //  如果某条规则改变了计划，它会以“左右对比（side-by-side）”的方式打印出变更前后的树结构。
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
  // 记录整个批次（Batch）执行后的结果。如果批次内没有任何规则生效，则记录“No effect”。
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

  // 输出执行统计信息（总运行次数、总时间等）
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
  // 核心辅助方法，根据 spark.sql.planChangeLog.level 配置动态决定日志输出级别。
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

// 主要作用是提供一个可扩展、可配置的框架，用于有序、重复地应用一组转换规则 (Rule[TreeType]) 到一个树形结构（TreeType，通常是逻辑计划 LogicalPlan 或物理计划 SparkPlan）上
// 核心职能： 管理和执行一系列规则批次 (Batch)，确保这些规则按照预定的策略 (Strategy)（如执行一次、或执行直到固定点收敛）正确应用到查询计划树上。
// 规则组织： 规则被组织成批次（Batch），每个批次可以有自己的执行策略和最大迭代次数。
abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   */
  // 定义规则执行的限制，如 maxIterations（最大迭代次数）
  abstract class Strategy {

    /** The maximum number of executions. */
    // 指定该策略下，规则批次的最大执行次数
    def maxIterations: Int

    /** Whether to throw exception when exceeding the maximum number. */
    // 指示当执行次数超过 maxIterations 时，是否应该抛出异常（默认为 false）
    def errorOnExceed: Boolean = false

    /** The key of SQLConf setting to tune maxIterations */
    // 指定一个 SQL 配置键，用于动态调整该策略的 maxIterations（默认为 null）
    def maxIterationsSetting: String = null
  }
  // 单次执行策略。 表示规则批次只执行 1 次，并且期望是幂等的。用于只需要执行一次的清理或初始化规则
  /** A strategy that is run once and idempotent. */
  case object Once extends Strategy { val maxIterations = 1 }

  /**
   * A strategy that runs until fix point or maxIterations times, whichever comes first.
   * Especially, a FixedPoint(1) batch is supposed to run only once.
   */
  // 固定点迭代策略。
  // 表示规则批次将重复执行，直到查询计划不再发生变化（达到固定点）或达到 maxIterations 次
  case class FixedPoint(
    override val maxIterations: Int,
    override val errorOnExceed: Boolean = false,
    override val maxIterationsSetting: String = null) extends Strategy

  /** A batch of rules. */
  // 规则批次容器。
  // 用于将一组规则组织在一起。
  // 包含三个元素：批次的 name（名称）、strategy（执行策略）和 rules（规则序列
  protected[catalyst] case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  // 一个序列（Seq），包含多个规则批次（Batch）。子类需要实现这个方法，定义具体的规则批次
  protected def batches: Seq[Batch]

  /** Once batches that are excluded in the idempotence checker */
  // 排除幂等性检查的批次。
  // 一个集合，存储使用 Once 策略但不需要在测试环境下进行幂等性检查的规则批次名称
  protected val excludedOnceBatches: Set[String] = Set.empty

  /**
   * Defines a validate function that validates the plan changes after the execution of each rule,
   * to make sure these rules make valid changes to the plan. For example, we can check whether
   * a plan is still resolved after each rule in `Optimizer`, so that we can catch rules that
   * turn the plan into unresolved.
   */
  // 计划变更验证函数
  // 用于在每条规则执行前后，检查计划的更改是否有效（例如，检查计划是否仍处于已解析状态）。如果发现无效变更，返回 Some(错误信息)
  protected def validatePlanChanges(
      previousPlan: TreeType,
      currentPlan: TreeType): Option[String] = None

  /**
   * Util method for checking whether a plan remains the same if re-optimized.
   */
  // 检查批次幂等性（私有）
  // 在测试模式下用于检查使用 Once 策略且未被排除的批次是否满足幂等性（即对已应用过一次的结果再次应用，计划是否仍然不变）
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
  // 执行规则批次并跟踪执行过程中的时间和效果信息。
  // 会通过 QueryPlanningTracker 记录每个规则的执行时间。
  def executeAndTrack(plan: TreeType, tracker: QueryPlanningTracker): TreeType = {
    QueryPlanningTracker.withTracker(tracker) {
      execute(plan)
    }
  }

  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   */

  // 实现了将复杂的优化逻辑分解为多个批次（Batch）、并在每个批次内重复应用**规则（Rule）**直到计划稳定的逻辑。
  def execute(plan: TreeType): TreeType = {
    // // 将输入的计划赋值给可变变量，用于在规则应用过程中不断更新
    var curPlan = plan
    val queryExecutionMetrics = RuleExecutor.queryExecutionMeter
    val planChangeLogger = new PlanChangeLogger[TreeType]()
    val tracker: Option[QueryPlanningTracker] = QueryPlanningTracker.get
    val beforeMetrics = RuleExecutor.getCurrentMetrics()
    // 获取是否开启计划验证的配置
    val enableValidation = SQLConf.get.getConf(SQLConf.PLAN_CHANGE_VALIDATION)
    // Validate the initial input.
    if (Utils.isTesting || enableValidation) {
      // 验证初始计划是否合法（例如：是否是已解析状态）
      validatePlanChanges(plan, plan) match {
        case Some(msg) =>
          val ruleExecutorName = this.getClass.getName.stripSuffix("$")
          throw new SparkException(
            errorClass = "PLAN_VALIDATION_FAILED_RULE_EXECUTOR",
            messageParameters = Map("ruleExecutor" -> ruleExecutorName, "reason" -> msg),
            cause = null)
        case _ =>
      }
    }
    // 遍历所有定义的规则批次，每个批次包含一组规则和执行策略
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
            val result = rule(plan)    // 【核心点】执行规则，返回新计划
            val runTime = System.nanoTime() - startTime
            val effective = !result.fastEquals(plan) // 通过地址或快速对比判断计划是否发生了改变

            if (effective) { // 如果规则生效（计划改变了）
              queryExecutionMetrics.incNumEffectiveExecution(rule.ruleName) // 增加有效执行次数
              queryExecutionMetrics.incTimeEffectiveExecutionBy(rule.ruleName, runTime) // 增加有效执行时间
              planChangeLogger.logRule(rule.ruleName, plan, result) // 打印规则应用后的计划对比日志
              // Run the plan changes validation after each rule.
              // 再次验证：确保规则没有把计划改坏（如：把 resolved 变成了 unresolved）
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
            // 返回本次规则处理后的计划，传给 foldLeft 的下一次循环
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
