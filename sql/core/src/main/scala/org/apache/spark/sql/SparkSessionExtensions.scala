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

package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.annotation.{DeveloperApi, Experimental, Unstable}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

/**
 * :: Experimental ::
 * Holder for injection points to the [[SparkSession]]. We make NO guarantee about the stability
 * regarding binary compatibility and source compatibility of methods here.
 *
 * This current provides the following extension points:
 *
 * <ul>
 * <li>Analyzer Rules.</li>
 * <li>Check Analysis Rules.</li>
 * <li>Cache Plan Normalization Rules.</li>
 * <li>Optimizer Rules.</li>
 * <li>Pre CBO Rules.</li>
 * <li>Planning Strategies.</li>
 * <li>Customized Parser.</li>
 * <li>(External) Catalog listeners.</li>
 * <li>Columnar Rules.</li>
 * <li>Adaptive Query Post Planner Strategy Rules.</li>
 * <li>Adaptive Query Stage Preparation Rules.</li>
 * <li>Adaptive Query Execution Runtime Optimizer Rules.</li>
 * <li>Adaptive Query Stage Optimizer Rules.</li>
 * </ul>
 *
 * The extensions can be used by calling `withExtensions` on the [[SparkSession.Builder]], for
 * example:
 * {{{
 *   SparkSession.builder()
 *     .master("...")
 *     .config("...", true)
 *     .withExtensions { extensions =>
 *       extensions.injectResolutionRule { session =>
 *         ...
 *       }
 *       extensions.injectParser { (session, parser) =>
 *         ...
 *       }
 *     }
 *     .getOrCreate()
 * }}}
 *
 * The extensions can also be used by setting the Spark SQL configuration property
 * `spark.sql.extensions`. Multiple extensions can be set using a comma-separated list. For example:
 * {{{
 *   SparkSession.builder()
 *     .master("...")
 *     .config("spark.sql.extensions", "org.example.MyExtensions,org.example.YourExtensions")
 *     .getOrCreate()
 *
 *   class MyExtensions extends Function1[SparkSessionExtensions, Unit] {
 *     override def apply(extensions: SparkSessionExtensions): Unit = {
 *       extensions.injectResolutionRule { session =>
 *         ...
 *       }
 *       extensions.injectParser { (session, parser) =>
 *         ...
 *       }
 *     }
 *   }
 *
 *   class YourExtensions extends SparkSessionExtensionsProvider {
 *     override def apply(extensions: SparkSessionExtensions): Unit = {
 *       extensions.injectResolutionRule { session =>
 *         ...
 *       }
 *       extensions.injectFunction(...)
 *     }
 *   }
 * }}}
 *
 * Note that none of the injected builders should assume that the [[SparkSession]] is fully
 * initialized and should not touch the session's internals (e.g. the SessionState).
 */
// 在 Apache Spark SQL 中，SparkSessionExtensions 是一个极其关键的类，它是 Spark 提供给开发者的官方插件化机制。
// 该类的核心作用是作为 “注入点容器”。它允许开发者在不修改 Spark 源码的情况下，介入 Spark SQL 的执行引擎。
// 通过这个类，你可以：
// 自定义 SQL 解析：比如添加特殊的语法支持。
// 自定义优化规则：在逻辑计划（Logical Plan）阶段进行规则重写。
// 干预物理执行：将逻辑算子转换成你自定义的物理算子（SparkPlan）
// 硬件加速支持：比如 Gluten 或 Photon，它们通过 injectColumnar 接口将原生的物理执行替换为列式（Columnar）执行（如 Velox/Arrow 路径）
@DeveloperApi
@Experimental
@Unstable
class SparkSessionExtensions {
  // 接受 SparkSession，返回逻辑计划优化规则 Rule[LogicalPlan]
  type RuleBuilder = SparkSession => Rule[LogicalPlan]
  // CheckRule 发生在 Analyzer（分析器） 阶段的最后
  // 不同于 RuleBuilder（用于修改或优化计划），CheckRuleBuilder 的目的不是为了“改变”，而是为了**“验证”**。
  // 如果逻辑计划是合法的，函数平稳运行结束（返回 Unit）
  type CheckRuleBuilder = SparkSession => LogicalPlan => Unit
  // 接受 SparkSession，返回物理规划策略 Strategy
  type StrategyBuilder = SparkSession => Strategy
  // 接受当前 Session 和上一个解析器，返回一个新的 ParserInterface（支持解析器链式堆叠）。
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  // 主要用于在运行时向 Spark 的函数注册表（FunctionRegistry）中注入自定义函数（UDF）。
  // FunctionIdentifier (函数的“名字”)  定义函数在 SQL 中被调用的名称和所属的数据库（Database）
  // ExpressionInfo (函数的“元数据”)  提供关于函数的帮助信息、文档、示例和类名。 示例：当用户在 Spark SQL 终端输入 DESCRIBE FUNCTION my_add 时，显示的说明文字就来自于这个对象。
  // FunctionBuilder (函数的“构造器”)  当 Spark 在 SQL 中解析到该函数时，会调用这个 Builder，并将传入的参数（表达式序列）转换为 Catalyst 树中的一个 Expression 节点。
  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)
  // 专门用于处理 UDTF（用户自定义表生成函数，User-Defined Table-Generating Functions）。
  // 三元组定义了将一个“表生成函数”注册到 Spark 内核中所需的完整信息：
  // FunctionIdentifier (函数标识符) 指定函数的 SQL 名称（如 explode, json_tuple, stack）以及可选的数据库命名空间。
  // ExpressionInfo (表达式元数据) 存储函数的反射信息。包括函数实现的类名、详细的帮助文档（Usage）、以及在 SQL 中执行 DESCRIBE FUNCTION 时展示的示例代码。
  // TableFunctionBuilder (表函数构造器) 与普通函数返回 Expression 不同，表函数在解析后会生成一个**逻辑计划（Logical Plan）**节点（通常是 Generator 节点的包装）
  // 当 Spark 解析到 SELECT * FROM my_tf(col) 时，它会调用这个 Builder，根据传入的参数 Seq[Expression] 构建出一个能够产生多行多列数据的逻辑节点。
  type TableFunctionDescription = (FunctionIdentifier, ExpressionInfo, TableFunctionBuilder)
  // 接受 SparkSession，返回列式转换规则 ColumnarRule
  type ColumnarRuleBuilder = SparkSession => ColumnarRule
  // 定义了一个针对 AQE（自适应查询执行，Adaptive Query Execution） 阶段的扩展点。
  // 输入：接受当前活跃的 SparkSession
  // 返回一个物理计划规则 Rule[SparkPlan]
  // 核心目标：该规则的操作对象是 SparkPlan（物理计划节点），而不是逻辑计划。
  // 它的作用：AQE 运行时的“最后修饰”
  // 在 Spark 的传统流水线中，物理计划一旦由 Planner 生成，通常就固定了。但在 AQE 开启后，物理计划是在运行时动态调整的。
  // QueryPostPlannerStrategy 的注入点非常特殊：它发生在 Planner 策略应用之后，但在 注入 Exchange（Shuffle）之前。
  type QueryPostPlannerStrategyBuilder = SparkSession => Rule[SparkPlan]
  // 定义了针对 AQE（自适应查询执行） 物理计划准备阶段的扩展接口。
  // Rule[SparkPlan]。这是一个作用于物理计划（SparkPlan）树的转换规则。
  // 核心逻辑：该规则接收一个初步生成的物理计划，通过 transform 或 transformUp 方法，将其中的节点替换、修改或修饰，最后返回一个新的物理计划。
  // 在 Spark 的执行流程中，当逻辑计划被转换为物理计划后，在正式将其划分为多个 Query Stages（查询阶段，通常以 Shuffle 为界）之前，会执行一组准备规则。
  // 关键应用场景：
  // 算子替换（Operator Replacement）：
  // 这是 Gluten 及其后端（如 Velox）最核心的使用点。Gluten 会在这里扫描物理计划，将能够被原生加速的行式算子（如 FileSourceScanExec）替换为原生的列式算子（如 BatchScanExecTransformer）。
  // 插入辅助节点：
  // 例如在某些算子前后插入数据转换节点（RowToColumnar 或 ColumnarToRow）
  // 物理特性的微调：
  //在 AQE 决定如何拆分 Stage 之前，调整算子的分布要求（Distribution）或排序要求（Ordering）。
  type QueryStagePrepRuleBuilder = SparkSession => Rule[SparkPlan]
  // 定义了针对 AQE（自适应查询执行）运行时优化 的扩展接口。
  // 该规则在 AQE 重新优化查询阶段（Query Stage）时被调用。
  // 这是 Spark SQL 中最灵活、最具动态性的扩展点之一。与前面提到的“准备规则（PrepRule）”不同，QueryStageOptimizerRule 发生在 查询执行期间。
  // 当一个 Query Stage 执行完成并产生了实际的 Shuffle 统计数据（如数据大小、行数、倾斜情况）后，AQE 会暂停执行，并调用这些优化规则来重新审视剩余的物理计划。
  // 关键应用场景：
  // 动态 Join 策略转换：根据刚刚跑完的 Shuffle 数据的实际大小，决定是否将 SortMergeJoin 转换为 BroadcastHashJoin。
  // Gluten 的动态回退：在 Gluten 这种 Native 引擎中，如果发现某个阶段产生的中间数据格式不符合 Native 算子预期，可以在此处动态决定是否将后续计划回退（Fallback）到 Spark 原生行式执行。
  type QueryStageOptimizerRuleBuilder = SparkSession => Rule[SparkPlan]
  // 存放列式执行规则。
  private[this] val columnarRuleBuilders = mutable.Buffer.empty[ColumnarRuleBuilder]
  private[this] val queryPostPlannerStrategyRuleBuilders =
    mutable.Buffer.empty[QueryPostPlannerStrategyBuilder]
  private[this] val queryStagePrepRuleBuilders = mutable.Buffer.empty[QueryStagePrepRuleBuilder]
  private[this] val runtimeOptimizerRules = mutable.Buffer.empty[RuleBuilder]
  private[this] val queryStageOptimizerRuleBuilders =
    mutable.Buffer.empty[QueryStageOptimizerRuleBuilder]

  /**
   * Build the override rules for columnar execution.
   */
    // 构建列式执行的重写规则
  private[sql] def buildColumnarRules(session: SparkSession): Seq[ColumnarRule] = {
    columnarRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Build the override rules for the query post planner strategy phase of adaptive query execution.
   */
    // 构建查询后规划策略阶段的规则
  private[sql] def buildQueryPostPlannerStrategyRules(
      session: SparkSession): Seq[Rule[SparkPlan]] = {
    queryPostPlannerStrategyRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Build the override rules for the query stage preparation phase of adaptive query execution.
   */
  // 构建查询阶段准备规则
  private[sql] def buildQueryStagePrepRules(session: SparkSession): Seq[Rule[SparkPlan]] = {
    queryStagePrepRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Build the override rules for the optimizer of adaptive query execution.
   */
  // 构建运行时优化器规则
  private[sql] def buildRuntimeOptimizerRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    runtimeOptimizerRules.map(_.apply(session)).toSeq
  }

  /**
   * Build the override rules for the query stage optimizer phase of adaptive query execution.
   */
  // 构建查询阶段优化规则
  private[sql] def buildQueryStageOptimizerRules(session: SparkSession): Seq[Rule[SparkPlan]] = {
    queryStageOptimizerRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Inject a rule that can override the columnar execution of an executor.
   */
  // 注入一个 ColumnarRuleBuilder，该规则用于列式执行的重写
  def injectColumnar(builder: ColumnarRuleBuilder): Unit = {
    columnarRuleBuilders += builder
  }

  /**
   * Inject a rule that applied between `plannerStrategy` and `queryStagePrepRules`, so
   * it can get the whole plan before injecting exchanges.
   * Note, these rules can only be applied within AQE.
   */
    // 注入一个规则，这些规则应用于查询计划后阶段，即 plannerStrategy 和 queryStagePrepRules 之间，通常在自适应查询执行（AQE）过程中使用
  def injectQueryPostPlannerStrategyRule(builder: QueryPostPlannerStrategyBuilder): Unit = {
    queryPostPlannerStrategyRuleBuilders += builder
  }

  /**
   * Inject a rule that can override the query stage preparation phase of adaptive query
   * execution.
   */
    //注入一个 QueryStagePrepRuleBuilder，用于自适应查询执行中的查询阶段准备
  def injectQueryStagePrepRule(builder: QueryStagePrepRuleBuilder): Unit = {
    queryStagePrepRuleBuilders += builder
  }

  /**
   * Inject a runtime `Rule` builder into the [[SparkSession]].
   * The injected rules will be executed after built-in
   * [[org.apache.spark.sql.execution.adaptive.AQEOptimizer]] rules are applied.
   * A runtime optimizer rule is used to improve the quality of a logical plan during execution
   * which can leverage accurate statistics from shuffle.
   *
   * Note that, it does not work if adaptive query execution is disabled.
   */
    //注入一个 RuleBuilder，用于运行时优化器规则。它可以在自适应查询执行启用时，提高基于准确统计数据的逻辑计划质量
  def injectRuntimeOptimizerRule(builder: RuleBuilder): Unit = {
    runtimeOptimizerRules += builder
  }

  /**
   * Inject a rule that can override the query stage optimizer phase of adaptive query
   * execution.
   */
    //注入一个 QueryStageOptimizerRuleBuilder，用于优化查询阶段
  def injectQueryStageOptimizerRule(builder: QueryStageOptimizerRuleBuilder): Unit = {
    queryStageOptimizerRuleBuilders += builder
  }
  // 存放分析器（Analyzer）解析阶段的规则
  private[this] val resolutionRuleBuilders = mutable.Buffer.empty[RuleBuilder]

  /**
   * Build the analyzer resolution `Rule`s using the given [[SparkSession]].
   */
    //构建解析规则。
  private[sql] def buildResolutionRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    resolutionRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Inject an analyzer resolution `Rule` builder into the [[SparkSession]]. These analyzer
   * rules will be executed as part of the resolution phase of analysis.
   */
    //注入一个 RuleBuilder，用于分析解析阶段的规则。这些规则在查询解析过程中应用，通常用于自定义规则的注入
  def injectResolutionRule(builder: RuleBuilder): Unit = {
    resolutionRuleBuilders += builder
  }
  // 一个存储**分析后置解析规则（Post-hoc Resolution Rules）**构造器的缓冲区。它是 Spark SQL 分析阶段（Analysis）中一个非常微妙且关键的扩展点。
  // 在 Spark SQL 的 Analyzer（分析器）执行逻辑中，解析过程是分多个批次（Batches）进行的。
  // 通常，Resolution 规则用于将未解析的符号（如表名、列名）绑定到实际的数据库对象。
  //Post-hoc Resolution Rules 发生在所有标准的解析规则运行之后。
  // 其核心作用包括：
  // 处理残留的未解析节点：如果标准的解析规则无法处理某些特殊的逻辑计划节点，可以在这个阶段进行最后的尝试。
  // 全局一致性检查后的重写：当所有表和列都已经绑定完成后，如果你需要根据完整的上下文信息来转换逻辑计划（例如：根据已确定的列类型自动插入特定的转换函数），这个阶段是最佳选择。
  // 自定义视图或宏的展开：有些复杂的扩展需要确保在所有基础元素都解析正确后，再进行二次展开。
  private[this] val postHocResolutionRuleBuilders = mutable.Buffer.empty[RuleBuilder]

  /**
   * Build the analyzer post-hoc resolution `Rule`s using the given [[SparkSession]].
   */
  private[sql] def buildPostHocResolutionRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    postHocResolutionRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Inject an analyzer `Rule` builder into the [[SparkSession]]. These analyzer
   * rules will be executed after resolution.
   */
  def injectPostHocResolutionRule(builder: RuleBuilder): Unit = {
    postHocResolutionRuleBuilders += builder
  }

  private[this] val checkRuleBuilders = mutable.Buffer.empty[CheckRuleBuilder]

  /**
   * Build the check analysis `Rule`s using the given [[SparkSession]].
   */
  private[sql] def buildCheckRules(session: SparkSession): Seq[LogicalPlan => Unit] = {
    checkRuleBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Inject an check analysis `Rule` builder into the [[SparkSession]]. The injected rules will
   * be executed after the analysis phase. A check analysis rule is used to detect problems with a
   * LogicalPlan and should throw an exception when a problem is found.
   */
  def injectCheckRule(builder: CheckRuleBuilder): Unit = {
    checkRuleBuilders += builder
  }
  // 定义了用于存储**计划归一化规则（Plan Normalization Rules）**构造器的缓冲区。它是 Spark SQL 缓存机制（Caching）优化中的一个专用扩展点
  // 在 Spark SQL 中，用户可以使用 .cache() 或 .persist() 来缓存中间结果。Spark 在内部通过比较逻辑计划（Logical Plan）的结构来判断一个查询是否可以复用已有的缓存。
  // 由于逻辑计划在表达上具有多样性，两个逻辑上等价的查询可能生成的计划树略有不同。
  //归一化规则的作用是将不同的逻辑计划转换成同一种“标准形式”。
  // 主要目的包括：
  // 消除不一致性：例如，将 a > 10 AND b < 5 和 b < 5 AND a > 10 统一排序，使它们在计划比较时被视为相同。
  // 别名处理：统一处理列的别名（Alias），防止因为别名不同导致缓存失效。
  // 常量折叠与简化：在缓存匹配前，先进行简单的逻辑简化。
  private[this] val planNormalizationRules = mutable.Buffer.empty[RuleBuilder]

  def buildPlanNormalizationRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    planNormalizationRules.map(_.apply(session)).toSeq
  }

  /**
   * Inject a plan normalization `Rule` builder into the [[SparkSession]]. The injected rules will
   * be executed just before query caching decisions are made. Such rules can be used to improve the
   * cache hit rate by normalizing different plans to the same form. These rules should never modify
   * the result of the LogicalPlan.
   */
  def injectPlanNormalizationRule(builder: RuleBuilder): Unit = {
    planNormalizationRules += builder
  }
  // 存放优化器（Optimizer）阶段的规则
  private[this] val optimizerRules = mutable.Buffer.empty[RuleBuilder]

  private[sql] def buildOptimizerRules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    optimizerRules.map(_.apply(session)).toSeq
  }

  /**
   * Inject an optimizer `Rule` builder into the [[SparkSession]]. The injected rules will be
   * executed during the operator optimization batch. An optimizer rule is used to improve the
   * quality of an analyzed logical plan; these rules should never modify the result of the
   * LogicalPlan.
   */
  def injectOptimizerRule(builder: RuleBuilder): Unit = {
    optimizerRules += builder
  }
  // 存储基于成本优化（CBO, Cost-Based Optimization）之前的规则构造器的缓冲区。它是逻辑优化阶段中一个非常高级的接入点。
  // 作用：为 CBO 扫清障碍或提供参考
  // 在 Spark SQL 的优化器（Optimizer）中，逻辑优化分为两个主要流派：
  // 基于规则的优化 (RBO)：根据经验公式进行转换（如谓词下推、投影裁剪）。
  // 基于成本的优化 (CBO)：根据数据的统计信息（如表大小、列分布、基数等）来计算不同执行路径的代价。
  // 在 CBO 开始计算代价之前，最后一次对逻辑计划进行重写。
  private[this] val preCBORules = mutable.Buffer.empty[RuleBuilder]

  private[sql] def buildPreCBORules(session: SparkSession): Seq[Rule[LogicalPlan]] = {
    preCBORules.map(_.apply(session)).toSeq
  }

  /**
   * Inject an optimizer `Rule` builder that rewrites logical plans into the [[SparkSession]].
   * The injected rules will be executed once after the operator optimization batch and
   * before any cost-based optimization rules that depend on stats.
   */
  def injectPreCBORule(builder: RuleBuilder): Unit = {
    preCBORules += builder
  }
  // 定义了一个存储**物理规划策略构造器（Planner Strategy Builders）**的缓冲区。它是将 SQL 从“逻辑世界”转换到“物理执行世界”的关键转换点。
  // 在 Spark SQL 中，Strategy（策略） 的职责是查看逻辑计划（LogicalPlan）树，并决定如何用一个或多个物理计划（SparkPlan）节点来实现它。
  // 核心功能：
  // 定义执行方式：例如，逻辑上的 Join 算子，通过策略可以被翻译成物理上的 BroadcastHashJoinExec、SortMergeJoinExec 或 ShuffledHashJoinExec。
  // 接入自定义算子：如果你开发了一个全新的物理算子（比如针对特定硬件优化的 NativeProjectExec），你需要注入一个策略，告诉 Spark：“当你看到逻辑计划中的 Project 时，请尝试使用我的 NativeProjectExec”。
  private[this] val plannerStrategyBuilders = mutable.Buffer.empty[StrategyBuilder]

  private[sql] def buildPlannerStrategies(session: SparkSession): Seq[Strategy] = {
    plannerStrategyBuilders.map(_.apply(session)).toSeq
  }

  /**
   * Inject a planner `Strategy` builder into the [[SparkSession]]. The injected strategy will
   * be used to convert a `LogicalPlan` into a executable
   * [[org.apache.spark.sql.execution.SparkPlan]].
   */
  def injectPlannerStrategy(builder: StrategyBuilder): Unit = {
    plannerStrategyBuilders += builder
  }
  // 存放自定义 SQL 解析器
  private[this] val parserBuilders = mutable.Buffer.empty[ParserBuilder]

  private[sql] def buildParser(
      session: SparkSession,
      initial: ParserInterface): ParserInterface = {
    parserBuilders.foldLeft(initial) { (parser, builder) =>
      builder(session, parser)
    }
  }

  /**
   * Inject a custom parser into the [[SparkSession]]. Note that the builder is passed a session
   * and an initial parser. The latter allows for a user to create a partial parser and to delegate
   * to the underlying parser for completeness. If a user injects more parsers, then the parsers
   * are stacked on top of each other.
   */
  def injectParser(builder: ParserBuilder): Unit = {
    parserBuilders += builder
  }

  private[this] val injectedFunctions = mutable.Buffer.empty[FunctionDescription]

  private[this] val injectedTableFunctions = mutable.Buffer.empty[TableFunctionDescription]

  private[sql] def registerFunctions(functionRegistry: FunctionRegistry) = {
    for ((name, expressionInfo, function) <- injectedFunctions) {
      functionRegistry.registerFunction(name, expressionInfo, function)
    }
    functionRegistry
  }

  private[sql] def registerTableFunctions(tableFunctionRegistry: TableFunctionRegistry) = {
    for ((name, expressionInfo, function) <- injectedTableFunctions) {
      tableFunctionRegistry.registerFunction(name, expressionInfo, function)
    }
    tableFunctionRegistry
  }

  /**
  * Injects a custom function into the [[org.apache.spark.sql.catalyst.analysis.FunctionRegistry]]
  * at runtime for all sessions.
  */
  def injectFunction(functionDescription: FunctionDescription): Unit = {
    injectedFunctions += functionDescription
  }

  /**
   * Injects a custom function into the
   * [[org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry]] at runtime for all sessions.
   */
  def injectTableFunction(functionDescription: TableFunctionDescription): Unit = {
    injectedTableFunctions += functionDescription
  }
}
