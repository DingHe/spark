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
//查询执行时动态地重新优化查询计划，以便根据执行过程中获得的统计信息对后续阶段进行优化
case class AdaptiveSparkPlanExec(
    inputPlan: SparkPlan, //输入的查询计划，表示原始的物理查询计划
    @transient context: AdaptiveExecutionContext,
    @transient preprocessingRules: Seq[Rule[SparkPlan]], //在查询阶段创建之前应用的物理计划规则（如优化规则等）
    @transient isSubquery: Boolean, //指示当前计划是否为子查询
    @transient override val supportsColumnar: Boolean = false)  //指示查询是否支持列式存储（列式执行）
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
  //查询计划要求的分布策略。如果是子查询，输出不需要特定的分区方式
  @transient private val requiredDistribution: Option[Distribution] = if (isSubquery) {
    // Subquery output does not need a specific output partitioning.
    Some(UnspecifiedDistribution)
  } else {
    AQEUtils.getRequiredDistribution(inputPlan)
  }
  //用于计算查询计划成本的评估器，确定当前计划的代价
  @transient private val costEvaluator =
    conf.getConf(SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS) match {
      case Some(className) => CostEvaluator.instantiate(className, session.sparkContext.getConf) //如果有自定义，使用自定义
      case _ => SimpleCostEvaluator(conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN))  //没有，默认为SimpleCostEvaluator
    }

  // A list of physical plan rules to be applied before creation of query stages. The physical
  // plan should reach a final status of query stages (i.e., no more addition or removal of
  // Exchange nodes) after running these rules.
  //在创建查询阶段之前应用的物理计划规则
  @transient private val queryStagePreparationRules: Seq[Rule[SparkPlan]] = {
    // For cases like `df.repartition(a, b).select(c)`, there is no distribution requirement for
    // the final plan, but we do need to respect the user-specified repartition. Here we ask
    // `EnsureRequirements` to not optimize out the user-specified repartition-by-col to work
    // around this case.
    val ensureRequirements =
      EnsureRequirements(requiredDistribution.isDefined, requiredDistribution)
    // CoalesceBucketsInJoin can help eliminate shuffles and must be run before
    // EnsureRequirements
    Seq(
      CoalesceBucketsInJoin,
      RemoveRedundantProjects,
      ensureRequirements,
      AdjustShuffleExchangePosition,
      ValidateSparkPlan,
      ReplaceHashWithSortAgg,
      RemoveRedundantSorts,
      RemoveRedundantWindowGroupLimits,
      DisableUnnecessaryBucketedScan,
      OptimizeSkewedJoin(ensureRequirements)
    ) ++ context.session.sessionState.adaptiveRulesHolder.queryStagePrepRules
  }

  // A list of physical optimizer rules to be applied to a new stage before its execution. These
  // optimizations should be stage-independent.
  //在查询阶段创建后，应用的物理优化规则
  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
    PlanAdaptiveDynamicPruningFilters(this),
    ReuseAdaptiveSubquery(context.subqueryCache),
    OptimizeSkewInRebalancePartitions,
    CoalesceShufflePartitions(context.session),
    // `OptimizeShuffleWithLocalRead` needs to make use of 'AQEShuffleReadExec.partitionSpecs'
    // added by `CoalesceShufflePartitions`, and must be executed after it.
    OptimizeShuffleWithLocalRead
  ) ++ context.session.sessionState.adaptiveRulesHolder.queryStageOptimizerRules

  // This rule is stateful as it maintains the codegen stage ID. We can't create a fresh one every
  // time and need to keep it in a variable.
  //用于合并代码生成阶段的规则
  @transient private val collapseCodegenStagesRule: Rule[SparkPlan] =
    CollapseCodegenStages()

  // A list of physical optimizer rules to be applied right after a new stage is created. The input
  // plan to these rules has exchange as its root node.
  private def postStageCreationRules(outputsColumnar: Boolean) = Seq(
    ApplyColumnarRulesAndInsertTransitions(
      context.session.sessionState.columnarRules, outputsColumnar),
    collapseCodegenStagesRule
  )
  //优化查询阶段的物理执行计划。它通过应用一系列优化规则来调整和优化查询计划
  //plan: 当前的物理查询计划（SparkPlan）
  //isFinalStage: 布尔值，指示当前阶段是否为查询的最终阶段
  private def optimizeQueryStage(plan: SparkPlan, isFinalStage: Boolean): SparkPlan = {
    val rules = if (isFinalStage &&
        !conf.getConf(SQLConf.ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS)) {
      queryStageOptimizerRules.filterNot(_.isInstanceOf[AQEShuffleReadRule])
    } else {
      queryStageOptimizerRules
    }
    //foldLeft 用来从初始的 plan（当前的查询计划）开始，逐一应用规则并更新计划
    val optimized = rules.foldLeft(plan) { case (latestPlan, rule) =>
      val applied = rule.apply(latestPlan)
      val result = rule match {
        //遍历优化规则并应用，特别针对 AQEShuffleReadRule 进行额外的分布验证，确保应用优化后不违反分布要求
        case _: AQEShuffleReadRule if !applied.fastEquals(latestPlan) =>
          val distribution = if (isFinalStage) {//如果是最终阶段（isFinalStage 为 true），使用 requiredDistribution
            // If `requiredDistribution` is None, it means `EnsureRequirements` will not optimize
            // out the user-specified repartition, thus we don't have a distribution requirement
            // for the final plan.
            requiredDistribution.getOrElse(UnspecifiedDistribution)
          } else {
            UnspecifiedDistribution
          }
          if (ValidateRequirements.validate(applied, distribution)) {
            applied
          } else {
            logDebug(s"Rule ${rule.ruleName} is not applied as it breaks the " +
              "distribution requirement of the query plan.")
            latestPlan
          }
        case _ => applied
      }
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
  //初始查询计划，包含经过预处理规则优化后的物理计划
  @transient val initialPlan = context.session.withActive {
    applyPhysicalRules(
      applyQueryPostPlannerStrategyRules(inputPlan),
      queryStagePreparationRules,
      Some((planChangeLogger, "AQE Preparations")))
  }
  //当前执行的物理查询计划，随着阶段的推进可能会发生变化
  @volatile private var currentPhysicalPlan = initialPlan

  @volatile private var _isFinalPlan = false

  private var currentStageId = 0

  /**
   * Return type for `createQueryStages`
   * @param newPlan the new plan with created query stages.
   * @param allChildStagesMaterialized whether all child stages have been materialized.
   * @param newStages the newly created query stages, including new reused query stages.
   */
  private case class CreateStageResult(
    newPlan: SparkPlan, //替换当前节点的新的查询计划（SparkPlan）
    allChildStagesMaterialized: Boolean, //示当前节点的所有子查询阶段是否都已物化
    newStages: Seq[QueryStageExec])  //新的查询阶段列表，表示为当前节点生成或复用的查询阶段
  //返回当前执行的物理查询计划
  def executedPlan: SparkPlan = currentPhysicalPlan

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
  //负责动态更新物理计划并优化查询执行。它主要管理物理计划的生成、查询阶段的物化（即执行）、以及在查询执行过程中对计划的调整和优化
  private def getFinalPhysicalPlan(): SparkPlan = lock.synchronized {
    if (isFinalPlan) return currentPhysicalPlan  //如果已经是最终的物理计划，直接返回

    // In case of this adaptive plan being executed out of `withActive` scoped functions, e.g.,
    // `plan.queryExecution.rdd`, we need to set active session here as new plan nodes can be
    // created in the middle of the execution.
    context.session.withActive {
      val executionId = getExecutionId
      // Use inputPlan logicalLink here in case some top level physical nodes may be removed
      // during `initialPlan`
      var currentLogicalPlan = inputPlan.logicalLink.get
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

  private def withFinalPlanUpdate[T](fun: SparkPlan => T): T = {
    val plan = getFinalPhysicalPlan()
    val result = fun(plan)
    finalPlanUpdate
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
  //用于遍历查询计划树，并根据当前节点的类型决定是否创建新的查询阶段，或者重用现有的查询阶段。
  // 它的作用是生成新的查询阶段（QueryStageExec）或复用已存在的查询阶段，并更新物理计划
  private def createQueryStages(plan: SparkPlan): CreateStageResult = plan match {
    case e: Exchange =>
      // First have a quick check in the `stageCache` without having to traverse down the node.
      context.stageCache.get(e.canonicalized) match {
        //如果存在且启用了复用，则复用现有的查询阶段
        case Some(existingStage) if conf.exchangeReuseEnabled =>
          val stage = reuseQueryStage(existingStage, e)
          val isMaterialized = stage.isMaterialized
          CreateStageResult(
            newPlan = stage,
            allChildStagesMaterialized = isMaterialized,
            newStages = if (isMaterialized) Seq.empty else Seq(stage))

        case _ =>
          val result = createQueryStages(e.child)
          val newPlan = e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]
          // Create a query stage only when all the child query stages are ready.
          if (result.allChildStagesMaterialized) {
            var newStage = newQueryStage(newPlan).asInstanceOf[ExchangeQueryStageExec]  //创建新的查询阶段
            if (conf.exchangeReuseEnabled) {
              // Check the `stageCache` again for reuse. If a match is found, ditch the new stage
              // and reuse the existing stage found in the `stageCache`, otherwise update the
              // `stageCache` with the new stage.
              val queryStage = context.stageCache.getOrElseUpdate(
                newStage.plan.canonicalized, newStage)
              if (queryStage.ne(newStage)) {
                newStage = reuseQueryStage(queryStage, e)
              }
            }
            val isMaterialized = newStage.isMaterialized
            CreateStageResult(
              newPlan = newStage,
              allChildStagesMaterialized = isMaterialized,
              newStages = if (isMaterialized) Seq.empty else Seq(newStage))
          } else {
            CreateStageResult(newPlan = newPlan,
              allChildStagesMaterialized = false, newStages = result.newStages)
          }
      }

    case i: InMemoryTableScanLike =>
      // There is no reuse for `InMemoryTableScanLike`, which is different from `Exchange`.
      // If we hit it the first time, we should always create a new query stage.
      val newStage = newQueryStage(i)
      CreateStageResult(
        newPlan = newStage,
        allChildStagesMaterialized = false,
        newStages = Seq(newStage))

    case q: QueryStageExec =>
      assertStageNotFailed(q)
      CreateStageResult(newPlan = q,
        allChildStagesMaterialized = q.isMaterialized, newStages = Seq.empty)

    case _ =>
      if (plan.children.isEmpty) {
        CreateStageResult(newPlan = plan, allChildStagesMaterialized = true, newStages = Seq.empty)
      } else {
        val results = plan.children.map(createQueryStages)
        CreateStageResult(
          newPlan = plan.withNewChildren(results.map(_.newPlan)),
          allChildStagesMaterialized = results.forall(_.allChildStagesMaterialized),
          newStages = results.flatMap(_.newStages))
      }
  }
  //创建一个新的查询阶段 (QueryStageExec)，并根据不同类型的 SparkPlan 节点生成适当的查询阶段实现。
  // 这是自适应查询执行（AQE）中的一个关键步骤，用于处理各种物理计划节点并将其转换为相应的查询阶段
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
        if (e.isInstanceOf[ShuffleExchangeLike]) {
          if (!newPlan.isInstanceOf[ShuffleExchangeLike]) {
            throw SparkException.internalError(
              "Custom columnar rules cannot transform shuffle node to something else.")
          }
          ShuffleQueryStageExec(currentStageId, newPlan, e.canonicalized)
        } else {
          assert(e.isInstanceOf[BroadcastExchangeLike])
          if (!newPlan.isInstanceOf[BroadcastExchangeLike]) {
            throw SparkException.internalError(
              "Custom columnar rules cannot transform broadcast node to something else.")
          }
          BroadcastQueryStageExec(currentStageId, newPlan, e.canonicalized)
        }
      case i: InMemoryTableScanLike =>
        // Apply `queryStageOptimizerRules` so that we can reuse subquery.
        // No need to apply `postStageCreationRules` for `InMemoryTableScanLike`
        // as it's a leaf node.
        val newPlan = optimizeQueryStage(i, isFinalStage = false)
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
  //复用Exchange阶段
  private def reuseQueryStage(
      existing: ExchangeQueryStageExec,
      exchange: Exchange): ExchangeQueryStageExec = {
    val queryStage = existing.newReuseInstance(currentStageId, exchange.output)
    currentStageId += 1
    setLogicalLinkForNewQueryStage(queryStage, exchange)
    queryStage
  }

  /**
   * Set the logical node link of the `stage` as the corresponding logical node of the `plan` it
   * encloses. If an `plan` has been transformed from a `Repartition`, it should have `logicalLink`
   * available by itself; otherwise traverse down to find the first node that is not generated by
   * `EnsureRequirements`.
   */
  private def setLogicalLinkForNewQueryStage(stage: QueryStageExec, plan: SparkPlan): Unit = {
    val link = plan.getTagValue(TEMP_LOGICAL_PLAN_TAG).orElse(
      plan.logicalLink.orElse(plan.collectFirst {
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
  //主要功能是应用一系列优化规则到查询计划上，并且根据是否提供日志记录的参数，决定是否记录每个规则的应用过程
  def applyPhysicalRules(
      plan: SparkPlan,  //表示当前的物理查询计划（即执行计划）
      rules: Seq[Rule[SparkPlan]], //表示一个优化规则的序列。这些规则是用来优化传入的 SparkPlan 的
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
case class AdaptiveExecutionContext(session: SparkSession, qe: QueryExecution) {

  /**
   * The subquery-reuse map shared across the entire query.
   */
  //TrieMap 是 Scala 中的一个线程安全的、并发友好的 Map 实现，位于 scala.collection.concurrent 包中。
  // 与传统的 HashMap 或 TreeMap 不同，TrieMap 使用了高效的非阻塞算法，特别适用于在并发环境中执行读写操作时
  val subqueryCache: TrieMap[SparkPlan, BaseSubqueryExec] =
    new TrieMap[SparkPlan, BaseSubqueryExec]()

  /**
   * The exchange-reuse map shared across the entire query, including sub-queries.
   */
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
