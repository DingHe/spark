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

import java.util
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec._
import org.apache.spark.sql.execution.bucketing.{CoalesceBucketsInJoin, DisableUnnecessaryBucketedScan}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanLike
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLAdaptiveSQLMetricUpdates, SQLPlanMetric}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkFatalException, ThreadUtils}

/**
 * A root node to execute the query plan adaptively. It splits the query plan into independent
 * stages and executes them in order according to their dependencies. The query stage
 * materializes its output at the end. When one stage completes, the data statistics of the
 * materialized output will be used to optimize the remainder of the query.
 *
 * To create query stages, we traverse the query tree bottom up. When we hit an exchange node,
 * and if all the child query stages of this exchange node are materialized, we create a new
 * query stage for this exchange node. The new stage is then materialized asynchronously once it
 * is created.
 *
 * When one query stage finishes materialization, the rest query is re-optimized and planned based
 * on the latest statistics provided by all materialized stages. Then we traverse the query plan
 * again and create more stages if possible. After all stages have been materialized, we execute
 * the rest of the plan.
 */
// AdaptiveSparkPlanExec（自适应 Spark 计划执行）是 Apache Spark SQL 自适应查询执行 (Adaptive Query Execution, AQE) 的核心物理执行计划节点。
// 该类作为一个 根节点，将一个完整的查询计划拆分为独立的查询阶段 (Query Stages)，并按照依赖关系依次执行这些阶段
case class AdaptiveSparkPlanExec(
    inputPlan: SparkPlan, //原始物理计划
    @transient context: AdaptiveExecutionContext, // AQE 上下文， 包含整个 AQE 执行所需的共享状态和资源，如 SparkSession、子查询缓存 (subqueryCache) 等
    @transient preprocessingRules: Seq[Rule[SparkPlan]], //预处理规则。 在 查询阶段创建之前，对 inputPlan 应用的一系列物理计划规则
    @transient isSubquery: Boolean, //子查询标志。 指示当前 AdaptiveSparkPlanExec 实例是否为一个子查询的根节点。子查询的输出通常不需要特定的分布
    @transient override val supportsColumnar: Boolean = false)  // 列式执行支持。 指示查询是否支持列式存储和执行
  extends LeafExecNode {

  @transient private val lock = new Object()

  @transient private val logOnLevel: ( => String) => Unit = conf.adaptiveExecutionLogLevel match {
    case "TRACE" => logTrace(_)
    case "DEBUG" => logDebug(_)
    case "INFO" => logInfo(_)
    case "WARN" => logWarning(_)
    case "ERROR" => logError(_)
    case _ => logDebug(_)
  }
  //记录查询计划变更的日志
  @transient private val planChangeLogger = new PlanChangeLogger[SparkPlan]()

  // The logical plan optimizer for re-optimizing the current logical plan.
  //AQE优化器，用于在执行过程中根据统计信息动态优化查询计划
  @transient private val optimizer = new AQEOptimizer(conf,
    session.sessionState.adaptiveRulesHolder.runtimeOptimizerRules)

  // `EnsureRequirements` may remove user-specified repartition and assume the query plan won't
  // change its output partitioning. This assumption is not true in AQE. Here we check the
  // `inputPlan` which has not been processed by `EnsureRequirements` yet, to find out the
  // effective user-specified repartition. Later on, the AQE framework will make sure the final
  // output partitioning is not changed w.r.t the effective user-specified repartition.
  // 所需的输出分布。
  // 表示整个查询（如果不是子查询）对其最终输出的分区策略要求，用于确保优化不会破坏用户或系统要求的分布。
  @transient private val requiredDistribution: Option[Distribution] = if (isSubquery) {
    // Subquery output does not need a specific output partitioning.
    Some(UnspecifiedDistribution)
  } else {
    AQEUtils.getRequiredDistribution(inputPlan)
  }
  // 成本评估器。
  // 用于评估当前物理计划的执行成本。在重新优化时，用于比较新的计划与当前计划的成本，只有新计划成本更优或相等时才会被采纳。
  @transient private val costEvaluator =
    conf.getConf(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS) match {
      case Some(className) => CostEvaluator.instantiate(className, session.sparkContext.getConf) //如果有自定义，使用自定义
      case _ => SimpleCostEvaluator(conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN))  //没有，默认为SimpleCostEvaluator
    }

  // A list of physical plan rules to be applied before creation of query stages. The physical
  // plan should reach a final status of query stages (i.e., no more addition or removal of
  // Exchange nodes) after running these rules.
  // 用于查询阶段准备（Query Stage Preparation）的一系列物理优化规则
  // 在将一个大的物理执行计划（SparkPlan）分解成更小的可独立执行的“查询阶段”（Query Stage）之前或在阶段内应用，旨在为 **AQE（自适应查询执行）**优化打下基础
  @transient private val queryStagePreparationRules: Seq[Rule[SparkPlan]] = {
    // For cases like `df.repartition(a, b).select(c)`, there is no distribution requirement for
    // the final plan, but we do need to respect the user-specified repartition. Here we ask
    // `EnsureRequirements` to not optimize out the user-specified repartition-by-col to work
    // around this case.
    // 这个规则的目的是根据上层操作符的需求来确保数据满足特定的分布要求（例如，HashPartitioning）
    val ensureRequirements =
      EnsureRequirements(requiredDistribution.isDefined, requiredDistribution)
    // CoalesceBucketsInJoin can help eliminate shuffles and must be run before
    // EnsureRequirements
    Seq(
      // 规则 1：合并 Join 中的 Buckets ， 规则优化分桶（Bucketed）Join，尝试将 Join 双方的分桶数据进一步合并，可能减少或消除 Shuffle 操作
      CoalesceBucketsInJoin,
      // 规则 2：移除冗余投影 ， 消除不必要的 Project 操作，例如 Project 节点上的表达式与子节点完全相同，可以被移除
      RemoveRedundantProjects,
      ensureRequirements,
      // 规则 4：调整 ShuffleExchange 位置 ， 尝试将 ShuffleExchange 节点向下推，使其更接近数据源，以减少在 Shuffle 之前不必要的操作
      AdjustShuffleExchangePosition,
      // 规则 5：验证物理计划。
      ValidateSparkPlan,
      // 规则 6：替换 Hash 聚合为 Sort 聚合。将 HashAggregate（哈希聚合）节点替换为 SortAggregate（排序聚合）节点，通常是为了处理内存溢出或当数据规模不确定时，提供更稳定的性能。
      ReplaceHashWithSortAgg,
      // 规则 7：移除冗余排序。
      RemoveRedundantSorts,
      // 规则 8：移除冗余 Window 组限制。
      RemoveRedundantWindowGroupLimits,
      // 规则 9：禁用不必要的分桶扫描。
      DisableUnnecessaryBucketedScan,
      // 规则 10：优化数据倾斜 Join。
      // AQE 的关键优化之一
      OptimizeSkewedJoin(ensureRequirements)
    ) ++ context.session.sessionState.adaptiveRulesHolder.queryStagePrepRules
  }

  // A list of physical optimizer rules to be applied to a new stage before its execution. These
  // optimizations should be stage-independent.
  // 用于查询阶段优化（Query Stage Optimizer Rules）的一系列物理优化规则。
  // 这些规则在 **AQE（自适应查询执行）**过程中，当一个查询阶段执行完毕，并获取到运行时统计信息（如 Shuffle 后的数据量）之后被应用
  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    // 规则 1：计划自适应动态剪枝过滤器。
    // 此规则负责将动态分区剪枝的逻辑应用到执行计划中。
    // 它利用运行时获取的 Shuffle 统计信息（特别是小的表）来确定分区过滤条件，从而在执行大的表扫描时跳过不必要的分区
    PlanAdaptiveDynamicPruningFilters(this),
    // 规则 2：重用自适应子查询。
    // 此规则检查当前的物理计划是否包含可以重用的自适应子查询（即已计算过结果的子查询）
    ReuseAdaptiveSubquery(context.subqueryCache),
    // 规则 3：优化重新平衡分区中的数据倾斜。
    // 当 AQE 检测到 Shuffle 后的数据存在严重倾斜时，此规则会重写 RebalancePartitions 操作，将倾斜的分区进一步拆分，从而确保后续操作的负载均衡
    OptimizeSkewInRebalancePartitions,
    // 规则 4：合并 Shuffle 分区（核心）。
    // 创建 CoalesceShufflePartitions 规则实例。这是 AQE 的标志性优化之一。
    // 它根据 Shuffle 后的实际数据量大小，将多个小分区合并成更大的分区，减少后续任务数量和调度开销，提高 I/O 吞吐量。
    CoalesceShufflePartitions(context.session),
    // `OptimizeShuffleWithLocalRead` needs to make use of 'AQEShuffleReadExec.partitionSpecs'
    // added by `CoalesceShufflePartitions`, and must be executed after it.
    // 规则 5：优化本地读取 Shuffle 数据。
    OptimizeShuffleWithLocalRead
  ) ++ context.session.sessionState.adaptiveRulesHolder.queryStageOptimizerRules

  // This rule is stateful as it maintains the codegen stage ID. We can't create a fresh one every
  // time and need to keep it in a variable.
  //用于合并代码生成阶段的规则
  @transient private val collapseCodegenStagesRule: Rule[SparkPlan] =
    CollapseCodegenStages()

  // A list of physical optimizer rules to be applied right after a new stage is created. The input
  // plan to these rules has exchange as its root node.
  // 定义了一个物理优化规则序列，这些规则将在一个新的查询阶段创建完成之后，以及在 AQE 循环的最后（即生成最终计划时），对物理计划应用。
  // 这些规则主要关注列式执行的转换和代码生成阶段的合并
  // outputsColumnar，该参数指示当前的查询是否被期望以列式数据格式（Columnar Data Format）输出
  private def postStageCreationRules(outputsColumnar: Boolean) = Seq(
    //规则1
    ApplyColumnarRulesAndInsertTransitions(
      context.session.sessionState.columnarRules, outputsColumnar),
    //规则2
    collapseCodegenStagesRule
  )

  // 目的是在自适应查询执行 (AQE) 的过程中，对查询阶段内部的物理计划应用一套专门的优化规则，这些规则通常依赖于已物化阶段的精确运行时统计信息
  // plan: SparkPlan：要优化的物理计划子树。
  // isFinalStage: Boolean：一个标志，指示当前是否正在优化最终的、顶层的计划阶段（即 AQE 循环结束后）。
  private def optimizeQueryStage(plan: SparkPlan, isFinalStage: Boolean): SparkPlan = {
    // 配置 SQLConf.ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS 被设置为 false（即禁止在最终阶段应用 Shuffle 优化）
    val rules = if (isFinalStage &&
        !conf.getConf(SQLConf.ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS)) {
      // 如果上述两个条件都满足，则从完整的 queryStageOptimizerRules 列表中排除掉 AQEShuffleReadRule
      queryStageOptimizerRules.filterNot(_.isInstanceOf[AQEShuffleReadRule])
    } else {
      queryStageOptimizerRules
    }
    // 规则应用循环（FoldLeft）
    val optimized = rules.foldLeft(plan) { case (latestPlan, rule) =>
      val applied = rule.apply(latestPlan)
      val result = rule match {
        // 特殊处理 AQEShuffleReadRule（例如，用于分区合并的规则）
        // 只有在规则确实改变了计划 (!applied.fastEquals(latestPlan)) 时才执行
        case _: AQEShuffleReadRule if !applied.fastEquals(latestPlan) =>
          val distribution = if (isFinalStage) {
            // If `requiredDistribution` is None, it means `EnsureRequirements` will not optimize
            // out the user-specified repartition, thus we don't have a distribution requirement
            // for the final plan.
            requiredDistribution.getOrElse(UnspecifiedDistribution)
          } else {
            UnspecifiedDistribution
          }
          // 检查应用了 AQEShuffleReadRule 后的新计划 (applied) 是否仍然满足所需的分布要求 (distribution)
          if (ValidateRequirements.validate(applied, distribution)) {
            applied
          } else {
            logDebug(s"Rule ${rule.ruleName} is not applied as it breaks the " +
              "distribution requirement of the query plan.")
            latestPlan
          }
        // 对于所有非 AQEShuffleReadRule 的规则，跳过分布验证，直接采纳优化后的计划 applied 作为结果
        case _ => applied
      }
      // 记录本次规则的应用结果。记录内容包括规则名称、应用规则前的计划 (latestPlan) 和应用规则后的最终计划 (result)
      planChangeLogger.logRule(rule.ruleName, latestPlan, result)
      result
    }
    planChangeLogger.logBatch("AQE Query Stage Optimization", plan, optimized)
    optimized
  }
  //应用一组后处理物理规则，通常是在查询计划生成后，作为最后一阶段的优化操作
  private def applyQueryPostPlannerStrategyRules(plan: SparkPlan): SparkPlan = {
    applyPhysicalRules(
      plan,
      context.session.sessionState.adaptiveRulesHolder.queryPostPlannerStrategyRules,
      Some((planChangeLogger, "AQE Query Post Planner Strategy Rules"))
    )
  }
  // 初始物理计划。
  // 经过 preprocessingRules 和 queryStagePreparationRules 处理后的初始物理计划
  @transient val initialPlan = context.session.withActive {
    applyPhysicalRules(
      applyQueryPostPlannerStrategyRules(inputPlan),
      queryStagePreparationRules,
      Some((planChangeLogger, "AQE Preparations")))
  }
  // 当前物理计划。
  // 动态变化。
  // 代表当前正在执行或等待执行的物理计划，会随着查询阶段的物化而更新
  @volatile private var currentPhysicalPlan = initialPlan
  // 最终计划标志。
  // 标记 currentPhysicalPlan 是否已经完成了所有的阶段物化和动态优化，达到了最终可执行状态
  @volatile private var _isFinalPlan = false
  // 当前阶段 ID。
  // 用于为新创建的 QueryStageExec 节点分配唯一的 ID
  private var currentStageId = 0

  /**
   * Return type for `createQueryStages`
   * @param newPlan the new plan with created query stages.
   * @param allChildStagesMaterialized whether all child stages have been materialized.
   * @param newStages the newly created query stages, including new reused query stages.
   */
  private case class CreateStageResult (
    newPlan: SparkPlan, // 替换当前节点的新的查询计划（SparkPlan）
    allChildStagesMaterialized: Boolean, // 表示当前节点的所有子查询阶段是否都已物化
    newStages: Seq[QueryStageExec] )  // 新的查询阶段列表，表示为当前节点生成或复用的查询阶段
  //返回当前执行的物理查询计划
  def executedPlan: SparkPlan = currentPhysicalPlan
  // 返回 _isFinalPlan 的值，
  // 指示是否已达到最终物理计划
  def isFinalPlan: Boolean = _isFinalPlan

  override def conf: SQLConf = context.session.sessionState.conf

  override def output: Seq[Attribute] = inputPlan.output

  override def doCanonicalize(): SparkPlan = inputPlan.canonicalized

  override def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
    executedPlan.resetMetrics()
  }
  //返回SQL的执行ID
  private def getExecutionId: Option[Long] = {
    Option(context.session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map(_.toLong)
  }
  //用来判断是否需要更新查询计划的 UI 信息
  private lazy val shouldUpdatePlan: Boolean = {
    // There are two cases that should not update plan:
    // 1. When executing subqueries, we can't update the query plan in the UI as the
    //    UI doesn't support partial update yet. However, the subquery may have been
    //    optimized into a different plan and we must let the UI know the SQL metrics
    //    of the new plan nodes, so that it can track the valid accumulator updates later
    //    and display SQL metrics correctly.
    // 2. If the `QueryExecution` does not match the current execution ID, it means the execution
    //    ID belongs to another (parent) query, and we should not call update UI in this query.
    //    e.g., a nested `AdaptiveSparkPlanExec` in `InMemoryTableScanLike`.
    //
    // That means only the root `AdaptiveSparkPlanExec` of the main query that triggers this
    // query execution need to do a plan update for the UI.
    //如果当前执行的是子查询（isSubquery == true），则不更新查询计划
    //如果当前的 QueryExecution 不匹配当前的执行 ID,则不更新查询计划。因为这意味着执行 ID 属于另一个（父级）查询，这时应该避免更新当前查询的 UI
    !isSubquery && getExecutionId.exists(SQLExecution.getQueryExecution(_) eq context.qe)
  }

  def finalPhysicalPlan: SparkPlan = withFinalPlanUpdate(identity)
  // 返回最终优化后的物理执行计划 SparkPlan
  // 确保了在任何时刻，只有一个线程可以执行 AQE 的动态优化和计划修改逻辑，从而保证线程安全
  private def getFinalPhysicalPlan(): SparkPlan = lock.synchronized {
    if (isFinalPlan) return currentPhysicalPlan  //如果已经是最终的物理计划，直接返回

    // In case of this adaptive plan being executed out of `withActive` scoped functions, e.g.,
    // `plan.queryExecution.rdd`, we need to set active session here as new plan nodes can be
    // created in the middle of the execution.
    context.session.withActive {
      // 获取当前的 SQL 执行 ID
      val executionId = getExecutionId
      // Use inputPlan logicalLink here in case some top level physical nodes may be removed
      // during `initialPlan`
      // 初始化当前逻辑计划。
      // 使用 inputPlan 的 logicalLink 作为起始，即使顶层物理节点在 initialPlan 阶段被移除，也能保持对原始逻辑的链接。
      // 这个变量在动态优化时会被更新
      var currentLogicalPlan = inputPlan.logicalLink.get
      // 第一次调用 createQueryStages
      // 遍历初始的物理计划 (currentPhysicalPlan)，将其中的 Exchange 节点替换为 QueryStageExec 节点（如果其子节点已准备就绪），返回结果包含新计划和新阶段列表
      var result = createQueryStages(currentPhysicalPlan)
      val events = new LinkedBlockingQueue[StageMaterializationEvent]()
      val errors = new mutable.ArrayBuffer[Throwable]()
      var stagesToReplace = Seq.empty[QueryStageExec]
      while (!result.allChildStagesMaterialized) {
        currentPhysicalPlan = result.newPlan
        if (result.newStages.nonEmpty) {
          stagesToReplace = result.newStages ++ stagesToReplace
          executionId.foreach(onUpdatePlan(_, result.newStages.map(_.plan)))

          // SPARK-33933: we should submit tasks of broadcast stages first, to avoid waiting
          // for tasks to be scheduled and leading to broadcast timeout.
          // This partial fix only guarantees the start of materialization for BroadcastQueryStage
          // is prior to others, but because the submission of collect job for broadcasting is
          // running in another thread, the issue is not completely resolved.
          val reorderedNewStages = result.newStages
            .sortWith {
              case (_: BroadcastQueryStageExec, _: BroadcastQueryStageExec) => false
              case (_: BroadcastQueryStageExec, _) => true
              case _ => false
            }

          // Start materialization of all new stages and fail fast if any stages failed eagerly
          reorderedNewStages.foreach { stage =>
            try {
              stage.materialize().onComplete { res =>
                if (res.isSuccess) {
                  events.offer(StageSuccess(stage, res.get))
                } else {
                  events.offer(StageFailure(stage, res.failed.get))
                }
                // explicitly clean up the resources in this stage
                stage.cleanupResources()
              }(AdaptiveSparkPlanExec.executionContext)
            } catch {
              case e: Throwable =>
                stage.error.set(Some(e))
                cleanUpAndThrowException(Seq(e), Some(stage.id))
            }
          }
        }

        // Wait on the next completed stage, which indicates new stats are available and probably
        // new stages can be created. There might be other stages that finish at around the same
        // time, so we process those stages too in order to reduce re-planning.
        val nextMsg = events.take()
        val rem = new util.ArrayList[StageMaterializationEvent]()
        events.drainTo(rem)
        (Seq(nextMsg) ++ rem.asScala).foreach {
          case StageSuccess(stage, res) =>
            stage.resultOption.set(Some(res))
          case StageFailure(stage, ex) =>
            stage.error.set(Some(ex))
            errors.append(ex)
        }

        // In case of errors, we cancel all running stages and throw exception.
        if (errors.nonEmpty) {
          cleanUpAndThrowException(errors.toSeq, None)
        }

        // Try re-optimizing and re-planning. Adopt the new plan if its cost is equal to or less
        // than that of the current plan; otherwise keep the current physical plan together with
        // the current logical plan since the physical plan's logical links point to the logical
        // plan it has originated from.
        // Meanwhile, we keep a list of the query stages that have been created since last plan
        // update, which stands for the "semantic gap" between the current logical and physical
        // plans. And each time before re-planning, we replace the corresponding nodes in the
        // current logical plan with logical query stages to make it semantically in sync with
        // the current physical plan. Once a new plan is adopted and both logical and physical
        // plans are updated, we can clear the query stage list because at this point the two plans
        // are semantically and physically in sync again.
        val logicalPlan = replaceWithQueryStagesInLogicalPlan(currentLogicalPlan, stagesToReplace)
        val afterReOptimize = reOptimize(logicalPlan)
        if (afterReOptimize.isDefined) {
          val (newPhysicalPlan, newLogicalPlan) = afterReOptimize.get
          val origCost = costEvaluator.evaluateCost(currentPhysicalPlan)
          val newCost = costEvaluator.evaluateCost(newPhysicalPlan)
          if (newCost < origCost ||
            (newCost == origCost && currentPhysicalPlan != newPhysicalPlan)) {
            logOnLevel("Plan changed:\n" +
              sideBySide(currentPhysicalPlan.treeString, newPhysicalPlan.treeString).mkString("\n"))
            cleanUpTempTags(newPhysicalPlan)
            currentPhysicalPlan = newPhysicalPlan
            currentLogicalPlan = newLogicalPlan
            stagesToReplace = Seq.empty[QueryStageExec]
          }
        }
        // Now that some stages have finished, we can try creating new stages.
        result = createQueryStages(currentPhysicalPlan)
      }

      // Run the final plan when there's no more unfinished stages.
      currentPhysicalPlan = applyPhysicalRules(
        optimizeQueryStage(result.newPlan, isFinalStage = true),
        postStageCreationRules(supportsColumnar),
        Some((planChangeLogger, "AQE Post Stage Creation")))
      _isFinalPlan = true
      executionId.foreach(onUpdatePlan(_, Seq(currentPhysicalPlan)))
      currentPhysicalPlan
    }
  }

  // Use a lazy val to avoid this being called more than once.
  @transient private lazy val finalPlanUpdate: Unit = {
    // Subqueries that don't belong to any query stage of the main query will execute after the
    // last UI update in `getFinalPhysicalPlan`, so we need to update UI here again to make sure
    // the newly generated nodes of those subqueries are updated.
    if (shouldUpdatePlan && currentPhysicalPlan.exists(_.subqueries.nonEmpty)) {
      getExecutionId.foreach(onUpdatePlan(_, Seq.empty))
    }
    logOnLevel(s"Final plan:\n$currentPhysicalPlan")
  }

  override def executeCollect(): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeCollect())
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeTake(n))
  }

  override def executeTail(n: Int): Array[InternalRow] = {
    withFinalPlanUpdate(_.executeTail(n))
  }


  // Spark 物理计划中最主要的执行入口。
  // 它返回一个 RDD，代表查询结果。
  // 任何 Spark 操作（如 df.collect(), df.write()）在底层最终都会调用此方法或其变体
  override def doExecute(): RDD[InternalRow] = {
    withFinalPlanUpdate(_.execute())
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    withFinalPlanUpdate(_.executeColumnar())
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    withFinalPlanUpdate { finalPlan =>
      assert(finalPlan.isInstanceOf[BroadcastQueryStageExec])
      finalPlan.doExecuteBroadcast()
    }
  }
  // 确保了在执行任何操作之前，自适应查询执行 (AQE) 过程已经完成，并获得了最终优化后的物理执行计划
  // 封装了从动态计划到最终执行的过渡逻辑
  private def withFinalPlanUpdate[T](fun: SparkPlan => T): T = {
    // 获取最终计划（核心）
    val plan = getFinalPhysicalPlan()
    // 执行实际操作
    val result = fun(plan)
    // 执行最终清理/更新
    finalPlanUpdate
    // 返回执行结果
    result
  }

  protected override def stringArgs: Iterator[Any] = Iterator(s"isFinalPlan=$isFinalPlan")

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
    super.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix,
      maxFields,
      printNodeId,
      indent)
    if (currentPhysicalPlan.fastEquals(initialPlan)) {
      lastChildren.add(true)
      currentPhysicalPlan.generateTreeString(
        depth + 1,
        lastChildren,
        append,
        verbose,
        prefix = "",
        addSuffix = false,
        maxFields,
        printNodeId,
        indent)
      lastChildren.remove(lastChildren.size() - 1)
    } else {
      generateTreeStringWithHeader(
        if (isFinalPlan) "Final Plan" else "Current Plan",
        currentPhysicalPlan,
        depth,
        append,
        verbose,
        maxFields,
        printNodeId)
      generateTreeStringWithHeader(
        "Initial Plan",
        initialPlan,
        depth,
        append,
        verbose,
        maxFields,
        printNodeId)
    }
  }


  private def generateTreeStringWithHeader(
      header: String,
      plan: SparkPlan,
      depth: Int,
      append: String => Unit,
      verbose: Boolean,
      maxFields: Int,
      printNodeId: Boolean): Unit = {
    append("   " * depth)
    append(s"+- == $header ==\n")
    plan.generateTreeString(
      0,
      new java.util.ArrayList(),
      append,
      verbose,
      prefix = "",
      addSuffix = false,
      maxFields,
      printNodeId,
      indent = depth + 1)
  }

  override def hashCode(): Int = inputPlan.hashCode()

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[AdaptiveSparkPlanExec]) {
      return false
    }

    this.inputPlan == obj.asInstanceOf[AdaptiveSparkPlanExec].inputPlan
  }

  /**
   * This method is called recursively to traverse the plan tree bottom-up and create a new query
   * stage or try reusing an existing stage if the current node is an [[Exchange]] node and all of
   * its child stages have been materialized.
   *
   * With each call, it returns:
   * 1) The new plan replaced with [[QueryStageExec]] nodes where new stages are created.
   * 2) Whether the child query stages (if any) of the current node have all been materialized.
   * 3) A list of the new query stages that have been created.
   */
  // Spark 自适应查询执行 (AQE) 中用于将整个查询计划分解为可独立执行和物化的查询阶段 (Query Stages) 的核心递归函数
  // 采用自底向上 (bottom-up) 的方式遍历物理计划树，主要处理 Exchange、InMemoryTableScanLike 和已存在的 QueryStageExec 节点
  private def createQueryStages(plan: SparkPlan): CreateStageResult = plan match {
    // Case 1: 遇到 Exchange 节点
    case e: Exchange =>
      // First have a quick check in the `stageCache` without having to traverse down the node.
      // 尝试在全局的 stageCache 中查找是否存在一个等价的已创建或已完成的 Exchange 阶段
      context.stageCache.get(e.canonicalized) match {
        // 如果存在且启用了复用，则复用现有的查询阶段
        case Some(existingStage) if conf.exchangeReuseEnabled =>
          val stage = reuseQueryStage(existingStage, e)
          // 检查这个复用阶段是否已经物化完成
          val isMaterialized = stage.isMaterialized
          // 返回一个结果对象
          CreateStageResult(
            newPlan = stage,
            allChildStagesMaterialized = isMaterialized,
            newStages = if (isMaterialized) Seq.empty else Seq(stage))
        // 如果缓存未命中或复用未启用
        case _ =>
          // 递归调用，先处理当前 Exchange 节点的子节点
          val result = createQueryStages(e.child)
          // 将子节点递归返回的新计划，替换当前 Exchange 节点的子节点
          val newPlan = e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]
          // Create a query stage only when all the child query stages are ready.
          // 创建阶段的条件检查。
          // 只有当 Exchange 的所有子阶段（即 e.child 下的所有可物化阶段）都已物化完成时，才允许将当前的 Exchange 封装为一个新的查询阶段
          if (result.allChildStagesMaterialized) {
            var newStage = newQueryStage(newPlan).asInstanceOf[ExchangeQueryStageExec]  //创建新的查询阶段
            if (conf.exchangeReuseEnabled) {
              // Check the `stageCache` again for reuse. If a match is found, ditch the new stage
              // and reuse the existing stage found in the `stageCache`, otherwise update the
              // `stageCache` with the new stage.
              val queryStage = context.stageCache.getOrElseUpdate(
                newStage.plan.canonicalized, newStage)

              // 如果返回的 queryStage 不等于我们刚刚创建的 newStage (queryStage.ne(newStage))，
              // 说明有并发命中，此时抛弃 newStage，转而使用 reuseQueryStage 来复用已存在的 queryStage

              if (queryStage.ne(newStage)) {
                newStage = reuseQueryStage(queryStage, e)
              }
            }
            val isMaterialized = newStage.isMaterialized
            // 返回新创建或复用的阶段结果
            CreateStageResult(
              newPlan = newStage,
              allChildStagesMaterialized = isMaterialized,
              newStages = if (isMaterialized) Seq.empty else Seq(newStage))
          } else {
            // 如果 Exchange 的子阶段尚未全部物化 ，则当前 Exchange 节点不能被封装为新的查询阶段
            CreateStageResult(newPlan = newPlan,
              allChildStagesMaterialized = false, newStages = result.newStages)
          }
      }

    // Case 2: 遇到 InMemoryTableScanLike 节点
    case i: InMemoryTableScanLike =>
      // There is no reuse for `InMemoryTableScanLike`, which is different from `Exchange`.
      // If we hit it the first time, we should always create a new query stage.
      // 匹配到内存表扫描节点（例如 df.cache().collect() 中的读取操作）
      val newStage = newQueryStage(i)
      CreateStageResult(
        newPlan = newStage,
        allChildStagesMaterialized = false,
        newStages = Seq(newStage))
    // Case 3: 遇到已存在的 QueryStageExec 节点
    case q: QueryStageExec =>
      // 断言该阶段没有失败，如果失败则抛出异常
      assertStageNotFailed(q)
      CreateStageResult(newPlan = q,
        allChildStagesMaterialized = q.isMaterialized, newStages = Seq.empty)
    // Case 4: 遇到其他所有节点（通用递归逻辑）
    case _ =>
      // 如果是叶子节点，返回自身作为新计划
      if (plan.children.isEmpty) {
        CreateStageResult(newPlan = plan, allChildStagesMaterialized = true, newStages = Seq.empty)
      } else {
        // 非叶子节点
        // 对所有子节点进行递归调用
        val results = plan.children.map(createQueryStages)
        CreateStageResult(
          newPlan = plan.withNewChildren(results.map(_.newPlan)),
          allChildStagesMaterialized = results.forall(_.allChildStagesMaterialized),
          newStages = results.flatMap(_.newStages))
      }
  }

  //  创建一个新的查询阶段 (QueryStageExec)，并根据不同类型的 SparkPlan 节点生成适当的查询阶段实现。
  //  这是自适应查询执行（AQE）中的一个关键步骤，用于处理各种物理计划节点并将其转换为相应的查询阶段
  private def newQueryStage(plan: SparkPlan): QueryStageExec = {
    val queryStage = plan match {
      case e: Exchange =>
        //先优化 Exchange 节点的子节点 e.child
        val optimized = e.withNewChildren(Seq(optimizeQueryStage(e.child, isFinalStage = false)))
        //然后将优化后的节点传递到 applyPhysicalRules 中进一步处理
        val newPlan = applyPhysicalRules(
          optimized,
          postStageCreationRules(outputsColumnar = plan.supportsColumnar),
          Some((planChangeLogger, "AQE Post Stage Creation")))
        // 验证： 验证应用 postStageCreationRules 后，根节点仍然是 Shuffle 类型（因为列式规则不应改变 Shuffle 的本质）。如果改变了，抛出内部错误
        if (e.isInstanceOf[ShuffleExchangeLike]) {
          if (!newPlan.isInstanceOf[ShuffleExchangeLike]) {
            throw SparkException.internalError(
              "Custom columnar rules cannot transform shuffle node to something else.")
          }
          // 创建阶段
          // 封装成 ShuffleQueryStageExec
          ShuffleQueryStageExec(currentStageId, newPlan, e.canonicalized)
        } else {
          // 验证应用规则后，根节点仍然是 Broadcast 类型。如果改变了，抛出内部错误
          assert(e.isInstanceOf[BroadcastExchangeLike])
          if (!newPlan.isInstanceOf[BroadcastExchangeLike]) {
            throw SparkException.internalError(
              "Custom columnar rules cannot transform broadcast node to something else.")
          }
          BroadcastQueryStageExec(currentStageId, newPlan, e.canonicalized)
        }
        // 匹配到内存表扫描节点（缓存读取）
      case i: InMemoryTableScanLike =>
        // Apply `queryStageOptimizerRules` so that we can reuse subquery.
        // No need to apply `postStageCreationRules` for `InMemoryTableScanLike`
        // as it's a leaf node.
        val newPlan = optimizeQueryStage(i, isFinalStage = false)
        // 验证优化后根节点仍是 InMemoryTableScanLike 类型
        if (!newPlan.isInstanceOf[InMemoryTableScanLike]) {
          throw SparkException.internalError(
            "Custom AQE rules cannot transform table scan node to something else.")
        }
        TableCacheQueryStageExec(currentStageId, newPlan)
    }
    currentStageId += 1
    setLogicalLinkForNewQueryStage(queryStage, plan)
    queryStage
  }
  // 在自适应查询执行 (AQE) 中，复用一个已经存在（可能正在运行或已完成）的 Exchange 查询阶段
  //
  private def reuseQueryStage(
      existing: ExchangeQueryStageExec, // 已缓存或已存在的查询阶段实例。这个阶段的结果可以被复用。
      exchange: Exchange): // 当前物理计划中需要替换的 Exchange 节点。方法返回一个新的复用阶段实例。
  ExchangeQueryStageExec = {
    val queryStage = existing.newReuseInstance(currentStageId, exchange.output)
    currentStageId += 1
    // 将新创建的复用阶段 (queryStage) 与原始 exchange 节点所对应的逻辑计划节点关联起来
    setLogicalLinkForNewQueryStage(queryStage, exchange)
    queryStage
  }

  /**
   * Set the logical node link of the `stage` as the corresponding logical node of the `plan` it
   * encloses. If an `plan` has been transformed from a `Repartition`, it should have `logicalLink`
   * available by itself; otherwise traverse down to find the first node that is not generated by
   * `EnsureRequirements`.
   */
  // 为新创建的查询阶段 (QueryStageExec) 设置正确的逻辑计划链接 (logicalLink)。
  // 这个链接是 自适应查询执行 (AQE) 进行重新优化和计划同步时不可或缺的依据
  private def setLogicalLinkForNewQueryStage(stage: QueryStageExec, plan: SparkPlan): Unit = {
    // 临时逻辑计划标签
    val link = plan.getTagValue(TEMP_LOGICAL_PLAN_TAG).orElse(
      // 第二优先级：节点自身逻辑链接。 如果没有临时标签，则尝试使用物理节点 plan 自身携带的永久逻辑链接 (logicalLink)
      plan.logicalLink.orElse(plan.collectFirst {
        // 第三优先级：向下遍历寻找链接。 如果前两者都没有找到链接，则向下遍历当前 plan 的子树，寻找最近的、有效的逻辑链接
        case p if p.getTagValue(TEMP_LOGICAL_PLAN_TAG).isDefined =>
          p.getTagValue(TEMP_LOGICAL_PLAN_TAG).get
        case p if p.logicalLink.isDefined => p.logicalLink.get
      }))
    assert(link.isDefined)
    stage.setLogicalLink(link.get)
  }

  /**
   * For each query stage in `stagesToReplace`, find their corresponding logical nodes in the
   * `logicalPlan` and replace them with new [[LogicalQueryStage]] nodes.
   * 1. If the query stage can be mapped to an integral logical sub-tree, replace the corresponding
   *    logical sub-tree with a leaf node [[LogicalQueryStage]] referencing this query stage. For
   *    example:
   *        Join                   SMJ                      SMJ
   *      /     \                /    \                   /    \
   *    r1      r2    =>    Xchg1     Xchg2    =>    Stage1     Stage2
   *                          |        |
   *                          r1       r2
   *    The updated plan node will be:
   *                               Join
   *                             /     \
   *    LogicalQueryStage1(Stage1)     LogicalQueryStage2(Stage2)
   *
   * 2. Otherwise (which means the query stage can only be mapped to part of a logical sub-tree),
   *    replace the corresponding logical sub-tree with a leaf node [[LogicalQueryStage]]
   *    referencing to the top physical node into which this logical node is transformed during
   *    physical planning. For example:
   *     Agg           HashAgg          HashAgg
   *      |               |                |
   *    child    =>     Xchg      =>     Stage1
   *                      |
   *                   HashAgg
   *                      |
   *                    child
   *    The updated plan node will be:
   *    LogicalQueryStage(HashAgg - Stage1)
   */
  //用于递归地遍历一个 LogicalPlan 并将与给定查询阶段 (QueryStageExec) 对应的逻辑节点替换为新的 LogicalQueryStage 节点。
  // 此方法用于自适应查询执行（AQE）中，将物理查询阶段映射到逻辑计划中并进行相应的替换
  private def replaceWithQueryStagesInLogicalPlan(
      plan: LogicalPlan, //输入的逻辑计划
      stagesToReplace: Seq[QueryStageExec]): LogicalPlan = { //要替换的查询阶段列表。每个查询阶段都将被映射到对应的逻辑节点并进行替换
    var logicalPlan = plan
    stagesToReplace.foreach {
      case stage if currentPhysicalPlan.exists(_.eq(stage)) =>
        val logicalNodeOpt = stage.getTagValue(TEMP_LOGICAL_PLAN_TAG).orElse(stage.logicalLink)
        assert(logicalNodeOpt.isDefined)
        val logicalNode = logicalNodeOpt.get
        val physicalNode = currentPhysicalPlan.collectFirst {
          case p if p.eq(stage) ||
            p.getTagValue(TEMP_LOGICAL_PLAN_TAG).exists(logicalNode.eq) ||
            p.logicalLink.exists(logicalNode.eq) => p
        }
        assert(physicalNode.isDefined)
        // Set the temp link for those nodes that are wrapped inside a `LogicalQueryStage` node for
        // they will be shared and reused by different physical plans and their usual logical links
        // can be overwritten through re-planning processes.
        setTempTagRecursive(physicalNode.get, logicalNode)
        // Replace the corresponding logical node with LogicalQueryStage
        val newLogicalNode = LogicalQueryStage(logicalNode, physicalNode.get)
        val newLogicalPlan = logicalPlan.transformDown {
          case p if p.eq(logicalNode) => newLogicalNode
        }
        logicalPlan = newLogicalPlan

      case _ => // Ignore those earlier stages that have been wrapped in later stages.
    }
    logicalPlan
  }

  /**
   * Re-optimize and run physical planning on the current logical plan based on the latest stats.
   */
  private def reOptimize(logicalPlan: LogicalPlan): Option[(SparkPlan, LogicalPlan)] = {
    try {
      logicalPlan.invalidateStatsCache()
      val optimized = optimizer.execute(logicalPlan)
      val sparkPlan = context.session.sessionState.planner.plan(ReturnAnswer(optimized)).next()
      val newPlan = applyPhysicalRules(
        applyQueryPostPlannerStrategyRules(sparkPlan),
        preprocessingRules ++ queryStagePreparationRules,
        Some((planChangeLogger, "AQE Replanning")))

      // When both enabling AQE and DPP, `PlanAdaptiveDynamicPruningFilters` rule will
      // add the `BroadcastExchangeExec` node manually in the DPP subquery,
      // not through `EnsureRequirements` rule. Therefore, when the DPP subquery is complicated
      // and need to be re-optimized, AQE also need to manually insert the `BroadcastExchangeExec`
      // node to prevent the loss of the `BroadcastExchangeExec` node in DPP subquery.
      // Here, we also need to avoid to insert the `BroadcastExchangeExec` node when the newPlan is
      // already the `BroadcastExchangeExec` plan after apply the `LogicalQueryStageStrategy` rule.
      val finalPlan = inputPlan match {
        case b: BroadcastExchangeLike
          if (!newPlan.isInstanceOf[BroadcastExchangeLike]) => b.withNewChildren(Seq(newPlan))
        case _ => newPlan
      }

      Some((finalPlan, optimized))
    } catch {
      case e: InvalidAQEPlanException[_] =>
        logOnLevel(s"Re-optimize - ${e.getMessage()}:\n${e.plan}")
        None
    }
  }

  /**
   * Recursively set `TEMP_LOGICAL_PLAN_TAG` for the current `plan` node.
   */
  private def setTempTagRecursive(plan: SparkPlan, logicalPlan: LogicalPlan): Unit = {
    plan.setTagValue(TEMP_LOGICAL_PLAN_TAG, logicalPlan)
    plan.children.foreach(c => setTempTagRecursive(c, logicalPlan))
  }

  /**
   * Unset all `TEMP_LOGICAL_PLAN_TAG` tags.
   */
  private def cleanUpTempTags(plan: SparkPlan): Unit = {
    plan.foreach {
      case plan: SparkPlan if plan.getTagValue(TEMP_LOGICAL_PLAN_TAG).isDefined =>
        plan.unsetTagValue(TEMP_LOGICAL_PLAN_TAG)
      case _ =>
    }
  }

  /**
   * Notify the listeners of the physical plan change.
   */
  private def onUpdatePlan(executionId: Long, newSubPlans: Seq[SparkPlan]): Unit = {
    if (!shouldUpdatePlan) {
      val newMetrics = newSubPlans.flatMap { p =>
        p.flatMap(_.metrics.values.map(m => SQLPlanMetric(m.name.get, m.id, m.metricType)))
      }
      context.session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveSQLMetricUpdates(
        executionId, newMetrics))
    } else {
      val planDescriptionMode = ExplainMode.fromString(conf.uiExplainMode)
      context.session.sparkContext.listenerBus.post(SparkListenerSQLAdaptiveExecutionUpdate(
        executionId,
        context.qe.explainString(planDescriptionMode),
        SparkPlanInfo.fromSparkPlan(context.qe.executedPlan)))
    }
  }

  private def assertStageNotFailed(stage: QueryStageExec): Unit = {
    if (stage.hasFailed) {
      throw stage.error.get().get match {
        case fatal: SparkFatalException => fatal.throwable
        case other => other
      }
    }
  }

  /**
   * Cancel all running stages with best effort and throw an Exception containing all stage
   * materialization errors and stage cancellation errors.
   */
  private def cleanUpAndThrowException(
       errors: Seq[Throwable],
       earlyFailedStage: Option[Int]): Unit = {
    currentPhysicalPlan.foreach {
      // earlyFailedStage is the stage which failed before calling doMaterialize,
      // so we should avoid calling cancel on it to re-trigger the failure again.
      case s: ExchangeQueryStageExec if !earlyFailedStage.contains(s.id) =>
        try {
          s.cancel()
        } catch {
          case NonFatal(t) =>
            logError(s"Exception in cancelling query stage: ${s.treeString}", t)
        }
      case _ =>
    }
    // Respect SparkFatalException which can be thrown by BroadcastExchangeExec
    val originalErrors = errors.map {
      case fatal: SparkFatalException => fatal.throwable
      case other => other
    }
    val e = if (originalErrors.size == 1) {
      originalErrors.head
    } else {
      val se = QueryExecutionErrors.multiFailuresInStageMaterializationError(originalErrors.head)
      originalErrors.tail.foreach(se.addSuppressed)
      se
    }
    throw e
  }
}

object AdaptiveSparkPlanExec {
  private[adaptive] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("QueryStageCreator", 16))

  /**
   * The temporary [[LogicalPlan]] link for query stages.
   *
   * Physical nodes wrapped in a [[LogicalQueryStage]] can be shared among different physical plans
   * and thus their usual logical links can be overwritten during query planning, leading to
   * situations where those nodes point to a new logical plan and the rest point to the current
   * logical plan. In this case we use temp logical links to make sure we can always trace back to
   * the original logical links until a new physical plan is adopted, by which time we can clear up
   * the temp logical links.
   */
  val TEMP_LOGICAL_PLAN_TAG = TreeNodeTag[LogicalPlan]("temp_logical_plan")

  /**
   * Apply a list of physical operator rules on a [[SparkPlan]].
   */
  // 主要功能是应用一系列优化规则到查询计划上，并且根据是否提供日志记录的参数，决定是否记录每个规则的应用过程
  def applyPhysicalRules(
      plan: SparkPlan,  // 表示当前的物理查询计划（即执行计划）
      rules: Seq[Rule[SparkPlan]], // 表示一个优化规则的序列。这些规则是用来优化传入的 SparkPlan 的
      loggerAndBatchName: Option[(PlanChangeLogger[SparkPlan], String)] = None): SparkPlan = {
    if (loggerAndBatchName.isEmpty) { //不记录日志，foldLeft直接展开
      rules.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
    } else {
      val (logger, batchName) = loggerAndBatchName.get
      val newPlan = rules.foldLeft(plan) { case (sp, rule) =>
        val result = rule.apply(sp)
        logger.logRule(rule.ruleName, sp, result)
        result
      }
      logger.logBatch(batchName, plan, newPlan)
      newPlan
    }
  }
}

/**
 * The execution context shared between the main query and all sub-queries.
 */
// 自适应执行上下文
// 作用是作为一个容器，存储和管理主查询及其所有子查询之间共享的执行状态和缓存资源
// 全局资源访问： 提供了对当前 SparkSession 和 QueryExecution 对象的访问
// 子查询复用： 允许整个查询（包括主查询和所有子查询）复用已经计算完成的子查询结果，避免重复计算。
// Exchange 阶段复用： 允许复用已完成的 Exchange（Shuffle 或 Broadcast）阶段的结果，这是 AQE 提高性能的关键机制之一
case class AdaptiveExecutionContext(session: SparkSession, qe: QueryExecution) {

  /**
   * The subquery-reuse map shared across the entire query.
   */
  // TrieMap 是 Scala 中的一个线程安全的、并发友好的 Map 实现，位于 scala.collection.concurrent 包中。
  // 与传统的 HashMap 或 TreeMap 不同，TrieMap 使用了高效的非阻塞算法，特别适用于在并发环境中执行读写操作时
  // 子查询复用缓存
  // 用于缓存已执行或正在执行的子查询结果。它的键是子查询的物理计划 (SparkPlan)，值是对应的子查询执行节点 (BaseSubqueryExec)
  val subqueryCache: TrieMap[SparkPlan, BaseSubqueryExec] =
    new TrieMap[SparkPlan, BaseSubqueryExec]()

  /**
   * The exchange-reuse map shared across the entire query, including sub-queries.
   */
   // Exchange 阶段复用缓存
   // 用于缓存已完成或正在进行的 Exchange 阶段的结果。
  //  它的键是Exchange 节点的规范化物理计划 (SparkPlan.canonicalized)，值是对应的Exchange 查询阶段 (ExchangeQueryStageExec)。
  //  这是实现 Shuffle/Broadcast 复用的核心机制
  val stageCache: TrieMap[SparkPlan, ExchangeQueryStageExec] =
    new TrieMap[SparkPlan, ExchangeQueryStageExec]()
}

/**
 * The event type for stage materialization.
 */
sealed trait StageMaterializationEvent

/**
 * The materialization of a query stage completed with success.
 */
case class StageSuccess(stage: QueryStageExec, result: Any) extends StageMaterializationEvent

/**
 * The materialization of a query stage hit an error and failed.
 */
case class StageFailure(stage: QueryStageExec, error: Throwable) extends StageMaterializationEvent
