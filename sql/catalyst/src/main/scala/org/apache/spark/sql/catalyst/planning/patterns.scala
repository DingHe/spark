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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf
//供了方法来处理查询计划中的投影（Project）和过滤（Filter）操作
trait OperationHelper extends AliasHelper with PredicateHelper {
  import org.apache.spark.sql.catalyst.optimizer.CollapseProject.canCollapseExpressions

  type IntermediateType =
    (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, AttributeMap[Alias])
  //Option[Seq[NamedExpression]]：顶层收集到的投影列表（Project 节点中的表达式）
  //Seq[Expression]：收集到的所有过滤条件（Filter 节点中的条件）
  //LogicalPlan：停止递归的最底层逻辑计划（即非 Project/Filter 节点）
  //AttributeMap[Alias]：在收集的 Project 节点中定义的别名映射关系（Key: 别名，Value: 别名所代表的底层表达式）

  protected def collectAllFilters: Boolean

  /**
   * Collects all adjacent projects and filters, in-lining/substituting aliases if necessary.
   * Here are two examples for alias in-lining/substitution.
   * Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  //模式匹配器（如 PhysicalOperation）的基础，用于递归地遍历逻辑计划，将所有相邻的 Project（投影）和 Filter（过滤）操作收集起来，并进行关键的别名内联/替换优化
  // 从一个逻辑计划的顶部开始，向下遍历，收集连续的 Project 和 Filter 节点，直到遇到一个非 Project/Filter 的节点为止。
  // 在收集过程中，它还会处理 Alias（别名），确保后续的过滤或投影操作引用的是底层真实的表达式，而不是中间的别名
  protected def collectProjectsAndFilters(
      plan: LogicalPlan,
      alwaysInline: Boolean): IntermediateType = {
    def empty: IntermediateType = (None, Nil, plan, AttributeMap.empty)

    plan match {
      //匹配 Project
      //匹配当前节点是一个 Project 操作，其中 fields 是投影列表，child 是其子计划
      case Project(fields, child) =>
        //递归
        val (_, filters, other, aliases) = collectProjectsAndFilters(child, alwaysInline)
        //检查当前的 fields 是否可以安全地向下合并到子计划的别名中。这通常涉及检查 fields 是否引用了 aliases 中的表达式，以及是否满足非确定性表达式等约束
        if (canCollapseExpressions(fields, aliases, alwaysInline)) {
          val replaced = fields.map(replaceAliasButKeepName(_, aliases))
          (Some(replaced), filters, other, getAliasMap(replaced))
        } else {
          empty
        }
      //匹配 Filter
      //匹配当前节点是一个 Filter 操作，condition 是过滤条件，child 是子计划。
      case Filter(condition, child) =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child, alwaysInline)
        // When collecting projects and filters, we effectively push down filters through
        // projects. We need to meet the following conditions to do so:
        //   1) no Project collected so far or the collected Projects are all deterministic
        //   2) this filter does not repeat any expensive expressions from the collected
        //      projects.
        // 关键的复杂检查，判断是否可以将当前的 Filter 条件下推到之前收集到的 Project 列表下方：
        // 1. 条件 1： 之前收集到的所有 Project 表达式都必须是确定性的（fields.forall(_.forall(_.deterministic))）。
        // 2. 条件 2： 当前的 condition 表达式可以安全地被别名内联（即不会重复计算昂贵的表达式）
        val canPushFilterThroughProject = fields.forall(_.forall(_.deterministic)) &&
          canCollapseExpressions(Seq(condition), aliases, alwaysInline)
        if (canPushFilterThroughProject) {
          // Ideally we can't combine non-deterministic filters, but if `collectAllFilters` is true,
          // we relax this restriction and assume the caller will take care of it.
          val canIncludeThisFilter = filters.isEmpty || {
            filters.last.deterministic && condition.deterministic
          }
          if (canIncludeThisFilter || collectAllFilters) {
            (fields, filters :+ replaceAlias(condition, aliases), other, aliases)
          } else {
            empty
          }
        } else {
          empty
        }

      //忽略 Hint
      //如果是 ResolvedHint 节点（例如，BROADCAST 提示），则直接递归处理其子计划，忽略 Hint 节点本身
      case h: ResolvedHint => collectProjectsAndFilters(h.child, alwaysInline)
      //默认情况
      //匹配到任何其他类型的逻辑计划节点（如 Join、Aggregate 或 Relation）
      case _ => empty
    }
  }
}

/**
 * A pattern that matches any number of project or filter operations even if they are
 * non-deterministic, as long as they satisfy the requirement of CollapseProject and CombineFilters.
 * All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator. [[Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
// Spark 优化器（Optimizer） 阶段充当一个模式匹配器（Pattern Matcher），用于识别和解构一系列连续的 Project（投影）和 Filter（过滤）逻辑操作
// 主要作用是在 Spark 的逻辑优化和物理转换阶段，简化（Flatten） 一段连续的逻辑计划链，从而进行重要的优化：
// 模式识别： 它识别出顶层可能包含连续的 Project 和 Filter 操作的逻辑计划结构。
// 操作合并与下推（Optimization）： 通过解构，它将所有连续的 Filter 条件收集起来，将最上层的 Project 列表收集起来，并找出最底层的非 Project/Filter 操作（通常是数据源或 Join 等）
//支持优化规则： 这种扁平化的结果为像 CollapseProject（合并多个 Project）和 CombineFilters（合并多个 Filter）以及谓词下推（Predicate Pushdown） 等关键优化规则提供了便利的数据结构。
// 简而言之，它将 Project -> Filter -> Project -> Filter -> ... -> Relation 这样的复杂链条，抽象地简化为：(最终投影列表, 所有过滤条件, 基础数据源)
object PhysicalOperation extends OperationHelper {
  // Returns: (the final project list, filters to push down, relation)
  //(最终投影列表, 要下推的过滤条件, 基础逻辑计划)
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)
  // 重写了 OperationHelper 中的方法。设置为 false 表明在收集 Filter 操作时，要遵循 CollapseProject 和 CombineFilters 规则的要求，
  // 不一定收集所有（例如，如果过滤器包含非确定性表达式，可能只收集满足条件的）
  override protected def collectAllFilters: Boolean = false

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    //读取 Spark 配置，确定是否应该总是内联（Substitute）Alias 表达式
    val alwaysInline = SQLConf.get.getConf(SQLConf.COLLAPSE_PROJECT_ALWAYS_INLINE)
    //递归遍历 plan： 1. fields：顶层 Project 列表（如果有）。 2. filters：收集到的所有 Filter 表达式列表。 3. child：不再是 Project 或 Filter 的底层逻辑计划
    val (fields, filters, child, _) = collectProjectsAndFilters(plan, alwaysInline)
    // If more than 2 filters are collected, they must all be deterministic.
    //如果收集到多于一个的过滤器（filters.length > 1），则断言所有这些过滤器都必须是确定性的
    if (filters.length > 1) assert(filters.forall(_.deterministic))
    Some((
      fields.getOrElse(child.output),
      filters.flatMap(splitConjunctivePredicates),
      child))
  }
}

/**
 * A variant of [[PhysicalOperation]] which can match multiple Filters that are not combinable due
 * to non-deterministic predicates. This is useful for scan operations as we need to match a bunch
 * of adjacent Projects/Filters to apply column pruning, even if the Filters can't be combined,
 * such as `Project(a, Filter(rand() > 0.5, Filter(rand() < 0.8, TableScan)))`, which we should
 * only read column `a` from the relation.
 */
//处理那些由于存在非确定性谓词（例如 rand()）而无法合并的多个过滤器，并将其推送到适当的位置。这样可以进行列修剪（pruning）操作，即仅扫描必要的列
object ScanOperation extends OperationHelper {
  // Returns: (the final project list, filters to stay up, filters to push down, relation)
  type ReturnType = (Seq[NamedExpression], Seq[Expression], Seq[Expression], LogicalPlan)
  //Seq[NamedExpression]：最终的投影列表，表示要返回的列
  //Seq[Expression]：应该保留在上层的过滤器
  //Seq[Expression]：应该推送到下层的过滤器
  //LogicalPlan：操作的子查询计划（即扫描的原始关系）
  override protected def collectAllFilters: Boolean = true  //表示是否应该收集所有的过滤器

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val alwaysInline = SQLConf.get.getConf(SQLConf.COLLAPSE_PROJECT_ALWAYS_INLINE)
    //获取当前计划中的字段、过滤器和子查询
    val (fields, filters, child, _) = collectProjectsAndFilters(plan, alwaysInline)
    // `collectProjectsAndFilters` transforms the plan bottom-up, so the bottom-most filter are
    // placed at the beginning of `filters` list. According to the SQL semantic, we cannot merge
    // Filters if one or more of them are nondeterministic. This means we can only push down the
    // bottom-most Filter, or more following deterministic Filters if the bottom-most Filter is
    // also deterministic.
    if (filters.isEmpty) {
      Some((fields.getOrElse(child.output), Nil, Nil, child))
    } else if (filters.head.deterministic) {
      //如果第一个过滤器是确定性的（deterministic），则将其及其后的所有确定性过滤器一起推送到下层
      val filtersCanPushDown = filters.takeWhile(_.deterministic)
        .flatMap(splitConjunctivePredicates)
      val filtersStayUp = filters.dropWhile(_.deterministic)
      Some((fields.getOrElse(child.output), filtersStayUp, filtersCanPushDown, child))
    } else {
      //如果是非确定性的过滤器，则只能将第一个过滤器推到下层，其后的过滤器将保留在上层
      val filtersCanPushDown = splitConjunctivePredicates(filters.head)
      val filtersStayUp = filters.drop(1)
      Some((fields.getOrElse(child.output), filtersStayUp, filtersCanPushDown, child))
    }
  }
}

object NodeWithOnlyDeterministicProjectAndFilter {
  @scala.annotation.tailrec
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case Project(projectList, child) if projectList.forall(_.deterministic) => unapply(child)
    case Filter(cond, child) if cond.deterministic => unapply(child)
    case _ => Some(plan)
  }
}

/**
 * A pattern that finds joins with equality conditions that can be evaluated using equi-join.
 *
 * Null-safe equality will be transformed into equality as joining key (replace null with default
 * value).
 */
// 目标是从一个 Join 操作中提取出可以通过等值连接来优化的连接条件，
// 特别是那些可以在连接之前被计算的条件。它会尝试将空值安全的等式转换成普通的等式，以便更高效地进行连接
object ExtractEquiJoinKeys extends Logging with PredicateHelper {
  /** (joinType, leftKeys, rightKeys, otherCondition, conditionOnJoinKeys, leftChild,
   * rightChild, joinHint).
   */
  // Note that `otherCondition` is NOT the original Join condition and it contains only
  // the subset that is not handled by the 'leftKeys' to 'rightKeys' equijoin.
  // 'conditionOnJoinKeys' is the subset of the original Join condition that corresponds to the
  // 'leftKeys' to 'rightKeys' equijoin.
  type ReturnType =
    (JoinType, Seq[Expression], Seq[Expression],
      Option[Expression], Option[Expression], LogicalPlan, LogicalPlan, JoinHint)
  //JoinType: 连接的类型（如 InnerJoin, LeftOuterJoin 等）
  //Seq[Expression]: 左侧连接键的表达式
  //Seq[Expression]: 右侧连接键的表达式
  //Option[Expression]: 连接条件中与连接键无关的其他条件，非等值条件
  //Option[Expression]: 连接条件中与连接键相关的条件，等值条件
  //LogicalPlan: 左侧子查询的计划
  //LogicalPlan: 右侧子查询的计划
  //JoinHint: 连接提示，可能用于指示优化器选择某种连接策略

  // 核心作用是在优化器尝试将逻辑 Join 转换为物理 Join 策略时，从 Join 节点中提取出所有可用的等值连接键（Equi-Join Keys）
  def unapply(join: Join): Option[ReturnType] = join match {
    case Join(left, right, joinType, condition, hint) =>
      logDebug(s"Considering join on: $condition")
      // Find equi-join predicates that can be evaluated before the join, and thus can be used
      // as join keys.

      val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil) // 将连接条件分解成多个子条件

      // 这段代码主要是提取等值条件中左右两侧的表达式
      val joinKeys = predicates.flatMap { // 提取连接键
        case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => None
        // 如果左侧和右侧的表达式（l 和 r）可以在左子查询和右子查询中计算出来，则将它们作为连接键，返回来也可以
        case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => Some((l, r))
        case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => Some((r, l))
        // Replace null with default value for joining key, then those rows with null in it could
        // be joined together
        // 这种条件用于处理空值安全的等值连接（即 NULL 值也参与连接）
        case EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, right) =>
          Seq((Coalesce(Seq(l, Literal.default(l.dataType))),
            Coalesce(Seq(r, Literal.default(r.dataType)))),
            (IsNull(l), IsNull(r))
          )  // (coalesce(l, default) = coalesce(r, default)) and (isnull(l) = isnull(r))
        case EqualNullSafe(l, r) if canEvaluate(l, right) && canEvaluate(r, left) =>
          Seq((Coalesce(Seq(r, Literal.default(r.dataType))),
            Coalesce(Seq(l, Literal.default(l.dataType)))),
            (IsNull(r), IsNull(l))
          )  // Same as above with left/right reversed.
        case _ => None
      }
      // 将原始谓词重新划分为两类：可以作为 Join Key 的谓词（predicatesOfJoinKeys）和不能作为 Join Key 的谓词（otherPredicates）
      val (predicatesOfJoinKeys, otherPredicates) = predicates.partition {  //将连接条件分为两类
        case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => false
        case Equality(l, r) =>
          canEvaluate(l, left) && canEvaluate(r, right) ||
            canEvaluate(l, right) && canEvaluate(r, left)
        case _ => false
      }

      if (joinKeys.nonEmpty) {
        val (leftKeys, rightKeys) = joinKeys.unzip
        logDebug(s"leftKeys:$leftKeys | rightKeys:$rightKeys")
        Some((joinType, leftKeys, rightKeys, otherPredicates.reduceOption(And),
          predicatesOfJoinKeys.reduceOption(And), left, right, hint))
      } else {
        None
      }
  }
}

/**
 * A pattern that collects the filter and inner joins.
 *
 *          Filter
 *            |
 *        inner Join
 *          /    \            ---->      (Seq(plan0, plan1, plan2), conditions)
 *      Filter   plan2
 *        |
 *  inner join
 *      /    \
 *   plan0    plan1
 *
 * Note: This pattern currently only works for left-deep trees.
 */
object ExtractFiltersAndInnerJoins extends PredicateHelper {

  /**
   * Flatten all inner joins, which are next to each other.
   * Return a list of logical plans to be joined with a boolean for each plan indicating if it
   * was involved in an explicit cross join. Also returns the entire list of join conditions for
   * the left-deep tree.
   */
  def flattenJoin(plan: LogicalPlan, parentJoinType: InnerLike = Inner)
      : (Seq[(LogicalPlan, InnerLike)], Seq[Expression]) = plan match {
    case Join(left, right, joinType: InnerLike, cond, hint) if hint == JoinHint.NONE =>
      val (plans, conditions) = flattenJoin(left, joinType)
      (plans ++ Seq((right, joinType)), conditions ++
        cond.toSeq.flatMap(splitConjunctivePredicates))
    case Filter(filterCondition, j @ Join(_, _, _: InnerLike, _, hint)) if hint == JoinHint.NONE =>
      val (plans, conditions) = flattenJoin(j)
      (plans, conditions ++ splitConjunctivePredicates(filterCondition))

    case _ => (Seq((plan, parentJoinType)), Seq.empty)
  }

  def unapply(plan: LogicalPlan)
      : Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]
      = plan match {
    case f @ Filter(filterCondition, j @ Join(_, _, joinType: InnerLike, _, hint))
        if hint == JoinHint.NONE =>
      Some(flattenJoin(f))
    case j @ Join(_, _, joinType, _, hint) if hint == JoinHint.NONE =>
      Some(flattenJoin(j))
    case _ => None
  }
}

/**
 * An extractor used when planning the physical execution of an aggregation. Compared with a logical
 * aggregation, the following transformations are performed:
 *  - Unnamed grouping expressions are named so that they can be referred to across phases of
 *    aggregation
 *  - Aggregations that appear multiple times are deduplicated.
 *  - The computation of the aggregations themselves is separated from the final result. For
 *    example, the `count` in `count + 1` will be split into an [[AggregateExpression]] and a final
 *    computation that computes `count.resultAttribute + 1`.
 */
//用于物理执行计划的聚合操作的提取器对象（extractor）
//是在逻辑聚合操作的基础上，进行一些优化和转换，为物理计划做准备
//对聚合表达式、分组表达式、结果表达式进行处理和重写，确保聚合操作可以在物理执行过程中高效地执行
object PhysicalAggregation {
  // groupingExpressions, aggregateExpressions, resultExpressions, child
  type ReturnType =
    (Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
      // A single aggregate expression might appear multiple times in resultExpressions.
      // In order to avoid evaluating an individual aggregate function multiple times, we'll
      // build a set of semantically distinct aggregate expressions and re-write expressions so
      // that they reference the single copy of the aggregate function which actually gets computed.
      // Non-deterministic aggregate expressions are not deduplicated.
      val equivalentAggregateExpressions = new EquivalentExpressions
      //去重聚合表达式
      val aggregateExpressions = resultExpressions.flatMap { expr =>
        expr.collect {
          // addExpr() always returns false for non-deterministic expressions and do not add them.
          case a: AggregateExpression if !equivalentAggregateExpressions.addExpr(a) =>
            a
        }
      }
      //处理分组表达式。如果分组表达式已经是 NamedExpression 类型（具有名称的表达式），则直接使用它；
      // 否则，给没有名称的表达式添加别名。这样，分组表达式就能在后续的聚合操作中被正确引用
      val namedGroupingExpressions = groupingExpressions.map {
        case ne: NamedExpression => ne -> ne
        // If the expression is not a NamedExpressions, we add an alias.
        // So, when we generate the result of the operator, the Aggregate Operator
        // can directly get the Seq of attributes representing the grouping expressions.
        case other =>
          val withAlias = Alias(other, other.toString)()
          other -> withAlias
      }
      val groupExpressionMap = namedGroupingExpressions.toMap

      // The original `resultExpressions` are a set of expressions which may reference
      // aggregate expressions, grouping column values, and constants. When aggregate operator
      // emits output rows, we will use `resultExpressions` to generate an output projection
      // which takes the grouping columns and final aggregate result buffer as input.
      // Thus, we must re-write the result expressions so that their attributes match up with
      // the attributes of the final result projection's input row:
      //替换聚合表达式为它们的最终结果属性（resultAttribute）
      //将分组表达式替换为它们的别名属性（通过 groupExpressionMap）
      val rewrittenResultExpressions = resultExpressions.map { expr =>
        expr.transformDown {
          case ae: AggregateExpression =>
            // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
            // so replace each aggregate expression by its corresponding attribute in the set:
            equivalentAggregateExpressions.getExprState(ae).map(_.expr)
              .getOrElse(ae).asInstanceOf[AggregateExpression].resultAttribute
          case expression if !expression.foldable =>
            // Since we're using `namedGroupingAttributes` to extract the grouping key
            // columns, we need to replace grouping key expressions with their corresponding
            // attributes. We do not rely on the equality check at here since attributes may
            // differ cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }

      Some((
        namedGroupingExpressions.map(_._2),
        aggregateExpressions,
        rewrittenResultExpressions,
        child))

    case _ => None
  }
}

/**
 * An extractor used when planning physical execution of a window. This extractor outputs
 * the window function type of the logical window.
 *
 * The input logical window must contain same type of window functions, which is ensured by
 * the rule ExtractWindowExpressions in the analyzer.
 */
//用于物理执行窗口操作的提取器（extractor）。
// 在 Spark SQL 查询的物理计划生成过程中，PhysicalWindow 用于提取与窗口操作相关的信息，并确保所有的窗口表达式是同一类型的窗口函数
object PhysicalWindow {
  // windowFunctionType, windowExpression, partitionSpec, orderSpec, child
  private type ReturnType =
    (WindowFunctionType, Seq[NamedExpression], Seq[Expression], Seq[SortOrder], LogicalPlan)
  //窗口函数的类型，表示该窗口操作使用的窗口函数类型，SQL或者Python
  //窗口表达式的序列，表示要在窗口中执行的表达式（例如，ROW_NUMBER(), RANK() 等）
  //Seq[Expression]：分区规范，表示窗口操作的数据分区依据
  //Seq[SortOrder]：排序规范，表示窗口操作中每个分区内数据的排序方式
  //LogicalPlan：子查询的逻辑计划，表示窗口操作要作用的子查询或操作（例如，一个扫描操作或者其他变换）

  //Scala 提供的提取器方法，定义了如何从给定的 Any 类型的对象中提取出窗口操作的相关信息
  def unapply(a: Any): Option[ReturnType] = a match {
    case expr @ logical.Window(windowExpressions, partitionSpec, orderSpec, child) =>

      // The window expression should not be empty here, otherwise it's a bug.
      // 确保窗口表达式不为空，否则抛出错误
      if (windowExpressions.isEmpty) {
        throw QueryCompilationErrors.emptyWindowExpressionError(expr)
      }
      //// 获取窗口函数类型，确保所有窗口表达式的类型一致
      val windowFunctionType = windowExpressions.map(WindowFunctionType.functionType)
        .reduceLeft { (t1: WindowFunctionType, t2: WindowFunctionType) =>
          if (t1 != t2) {
            // We shouldn't have different window function type here, otherwise it's a bug.
            throw QueryCompilationErrors.foundDifferentWindowFunctionTypeError(windowExpressions)
          } else {
            t1
          }
        }

      Some((windowFunctionType, windowExpressions, partitionSpec, orderSpec, child))

    case _ => None
  }
}
// 用于提取单列 NULL-aware Anti Join 的对象。该类在 Apache Spark 中的优化过程中起到了关键作用，
// 特别是针对 LeftAnti 类型的连接，能够通过条件优化连接方式，避免了代价高昂的广播嵌套循环连接
object ExtractSingleColumnNullAwareAntiJoin extends JoinSelectionHelper with PredicateHelper {

  // TODO support multi column NULL-aware anti join in future.
  // See. http://www.vldb.org/pvldb/vol2/vldb09-423.pdf Section 6
  // multi-column null aware anti join is much more complicated than single column ones.

  // streamedSideKeys, buildSideKeys
  private type ReturnType = (Seq[Expression], Seq[Expression])

  /**
   * See. [SPARK-32290]
   * LeftAnti(condition: Or(EqualTo(a=b), IsNull(EqualTo(a=b)))
   * will almost certainly be planned as a Broadcast Nested Loop join,
   * which is very time consuming because it's an O(M*N) calculation.
   * But if it's a single column case O(M*N) calculation could be optimized into O(M)
   * using hash lookup instead of loop lookup.
   */
  def unapply(join: Join): Option[ReturnType] = join match {
    // 当连接条件是 EqualTo(a = b) 或 EqualTo(a = b) OR IsNull(EqualTo(a = b)) 时，
    // 能够通过优化把原本需要进行 O(M*N) 次的连接操作转化为 O(M) 的哈希查找，从而大大提高效率
    case Join(left, right, LeftAnti,
      Some(Or(e @ EqualTo(leftAttr: Expression, rightAttr: Expression),
        IsNull(e2 @ EqualTo(_, _)))), _)
        if SQLConf.get.optimizeNullAwareAntiJoin &&
          e.semanticEquals(e2) =>
      if (canEvaluate(leftAttr, left) && canEvaluate(rightAttr, right)) {
        Some(Seq(leftAttr), Seq(rightAttr))
      } else if (canEvaluate(leftAttr, right) && canEvaluate(rightAttr, left)) {
        Some(Seq(rightAttr), Seq(leftAttr))
      } else {
        None
      }
    case _ => None
  }
}

/**
 * An extractor for row-level commands such as DELETE, UPDATE, MERGE that were rewritten using plans
 * that operate on groups of rows.
 *
 * This class extracts the following entities:
 *  - the group-based rewrite plan;
 *  - the condition that defines matching groups;
 *  - the group filter condition;
 *  - the read relation that can be either [[DataSourceV2Relation]] or [[DataSourceV2ScanRelation]]
 *  depending on whether the planning has already happened;
 */
object GroupBasedRowLevelOperation {
  type ReturnType = (ReplaceData, Expression, Option[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case rd @ ReplaceData(DataSourceV2Relation(table, _, _, _, _),
        cond, query, _, groupFilterCond, _) =>
      // group-based UPDATEs that are rewritten as UNION read the table twice
      val allowMultipleReads = rd.operation.command == UPDATE
      val readRelation = findReadRelation(table, query, allowMultipleReads)
      readRelation.map((rd, cond, groupFilterCond, _))

    case _ =>
      None
  }

  private def findReadRelation(
      table: Table,
      plan: LogicalPlan,
      allowMultipleReads: Boolean): Option[LogicalPlan] = {

    val readRelations = plan.collect {
      case r: DataSourceV2Relation if r.table eq table => r
      case r: DataSourceV2ScanRelation if r.relation.table eq table => r
    }

    // in some cases, the optimizer replaces the v2 read relation with a local relation
    // for example, there is no reason to query the table if the condition is always false
    // that's why it is valid not to find the corresponding v2 read relation

    readRelations match {
      case relations if relations.isEmpty =>
        None

      case Seq(relation) =>
        Some(relation)

      case Seq(relation1: DataSourceV2Relation, relation2: DataSourceV2Relation)
          if allowMultipleReads && (relation1.table eq relation2.table) =>
        Some(relation1)

      case Seq(relation1: DataSourceV2ScanRelation, relation2: DataSourceV2ScanRelation)
          if allowMultipleReads && (relation1.scan eq relation2.scan) =>
        Some(relation1)

      case other =>
        throw new AnalysisException(
          s"Unexpected row-level read relations (allow multiple = $allowMultipleReads): $other")
    }
  }
}
