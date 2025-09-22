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

package org.apache.spark.sql.catalyst.analysis

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils.wrapOuterReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors}
import org.apache.spark.sql.internal.SQLConf
//提供了多种辅助方法来解析查询计划中的列、属性和表达式。这个 trait 主要负责处理查询中的列解析、表达式解析以及与列相关的错误处理
trait ColumnResolutionHelper extends Logging with DataTypeErrorsBase {

  def conf: SQLConf

  /**
   * This method tries to resolve expressions and find missing attributes recursively.
   * Specifically, when the expressions used in `Sort` or `Filter` contain unresolved attributes
   * or resolved attributes which are missing from child output. This method tries to find the
   * missing attributes and add them into the projection.
   */
    //递归解析表达式，并处理缺失的属性，尤其是当表达式在 Sort 或 Filter 中包含未解析的属性，或者已解析的属性在子计划的输出中缺失时
  protected def resolveExprsAndAddMissingAttrs(
      exprs: Seq[Expression], plan: LogicalPlan): (Seq[Expression], LogicalPlan) = {
    // exprs: Seq[Expression]：待解析的表达式列表
    // plan: LogicalPlan：当前的逻辑计划，用于解析表达式中的属性
    // Missing attributes can be unresolved attributes or resolved attributes which are not in
    // the output attributes of the plan.
    if (exprs.forall(e => e.resolved && e.references.subsetOf(plan.outputSet))) {
      (exprs, plan)  //如果所有表达式已经解析（e.resolved 为 true）且引用的属性（e.references）都在当前计划的输出集中（plan.outputSet）时，直接返回当前表达式和计划
    } else {
      plan match {
        // For `Distinct` and `SubqueryAlias`, we can't recursively resolve and add attributes
        // via its children.
        case u: UnaryNode if !u.isInstanceOf[Distinct] && !u.isInstanceOf[SubqueryAlias] =>
          //当 plan 是 UnaryNode 类型（例如 Project, Aggregate 等），且不是 Distinct 或 SubqueryAlias 时，会进行递归解析
          val (newExprs, newChild) = {
            // Resolving expressions against current plan.
            val maybeResolvedExprs = exprs.map(resolveExpressionByPlanOutput(_, u))  //解析当前计划的表达式
            // Recursively resolving expressions on the child of current plan.
            resolveExprsAndAddMissingAttrs(maybeResolvedExprs, u.child) //递归调用
          }
          // If some attributes used by expressions are resolvable only on the rewritten child
          // plan, we need to add them into original projection.
          lazy val missingAttrs =
            (AttributeSet(newExprs) -- u.outputSet).intersect(newChild.outputSet)
          u match {  //根据计划类型不同，处理缺失的属性
            case p: Project =>  //如果当前计划是 Project，将缺失的属性添加到原始的 projectList 中，构建新的 Project 节点
              val newProject = Project(p.projectList ++ missingAttrs, newChild)
              newProject.copyTagsFrom(p)
              (newExprs, newProject)
            //如果当前计划是 Aggregate，检查缺失的属性是否为分组表达式（groupExprs）。如果所有缺失的属性都可以作为分组表达式，才将它们添加到聚合表达式中；否则，不处理
            case a @ Aggregate(groupExprs, aggExprs, child) =>
              if (missingAttrs.forall(attr => groupExprs.exists(_.semanticEquals(attr)))) {
                // All the missing attributes are grouping expressions, valid case.
                (newExprs,
                  a.copy(aggregateExpressions = aggExprs ++ missingAttrs, child = newChild))
              } else {
                // Need to add non-grouping attributes, invalid case.
                (exprs, a)
              }
            //如果是 Generate 节点，直接返回新的表达式和计划
            case g: Generate =>
              (newExprs, g.copy(unrequiredChildIndex = Nil, child = newChild))
             //对于其他类型的计划，直接创建一个新的计划，将新的子计划添加到当前计划中，并返回表达式和新的计划
            case _ =>
              (newExprs, u.withNewChildren(Seq(newChild)))
          }
        // For other operators, we can't recursively resolve and add attributes via its children.
        case other =>
          (exprs.map(resolveExpressionByPlanOutput(_, other)), other)
      }
    }
  }

    // support CURRENT_DATE, CURRENT_TIMESTAMP, and grouping__id
    // 字面量函数（literal functions），例如 CURRENT_DATE、CURRENT_TIMESTAMP 和 GROUPING__ID。
    // 这些函数不需要用户在调用时指定括号，它们的作用是在表达式解析时被用作常量值或特定的虚拟列
  private val literalFunctions: Seq[(String, () => Expression, Expression => String)] = Seq(
    (CurrentDate().prettyName, () => CurrentDate(), toPrettySQL(_)),
    (CurrentTimestamp().prettyName, () => CurrentTimestamp(), toPrettySQL(_)),
    (CurrentUser().prettyName, () => CurrentUser(), toPrettySQL),
    ("user", () => CurrentUser(), toPrettySQL),
    (VirtualColumn.hiveGroupingIdName, () => GroupingID(Nil), _ => VirtualColumn.hiveGroupingIdName)
  )

  /**
   * Literal functions do not require the user to specify braces when calling them
   * When an attributes is not resolvable, we try to resolve it as a literal function.
   */
  //目的是解析传入的 nameParts（一个字符串序列），并试图根据提供的名称查找是否存在一个字面量函数。如果找到了对应的字面量函数，它将返回一个别名（Alias）作为解析结果
  private def resolveLiteralFunction(nameParts: Seq[String]): Option[NamedExpression] = {
    if (nameParts.length != 1) return None
    val name = nameParts.head
    literalFunctions.find(func => caseInsensitiveResolution(func._1, name)).map {
      case (_, getFuncExpr, getAliasName) =>   //如果找到匹配的字面量函数
        val funcExpr = getFuncExpr()  //getFuncExpr() 会调用相应的函数（例如 CurrentDate()）来获取一个表达式
        Alias(funcExpr, getAliasName(funcExpr))()
    }
  }

  /**
   * Resolves `UnresolvedAttribute`, `GetColumnByOrdinal` and extract value expressions(s) by
   * traversing the input expression in top-down manner. It must be top-down because we need to
   * skip over unbound lambda function expression. The lambda expressions are resolved in a
   * different place [[ResolveLambdaVariables]].
   *
   * Example :
   * SELECT transform(array(1, 2, 3), (x, i) -> x + i)"
   *
   * In the case above, x and i are resolved as lambda variables in [[ResolveLambdaVariables]].
   */
    //负责解析表达式中的不同类型的未解析的元素，并返回解析后的结果
  private def resolveExpression(
      expr: Expression,    //待解析的表达式
      resolveColumnByName: Seq[String] => Option[Expression],  //通过列名来解析表达式的函数
      getAttrCandidates: () => Seq[Attribute],  //提供可选属性的候选集合，用于解析列
      throws: Boolean,   //是否在解析失败时抛出异常
      allowOuter: Boolean): Expression = {  //是否允许解析外部引用
    def innerResolve(e: Expression, isTopLevel: Boolean): Expression = withOrigin(e.origin) {
      if (e.resolved) return e  //如果当前表达式已经解析（resolved 为 true），则直接返回它
      val resolved = e match {
        case f: LambdaFunction if !f.bound => f   //未绑定的 Lambda 函数（LambdaFunction），则跳过解析。因为 Lambda 变量的解析在另外的地方（ResolveLambdaVariables）进行

        case GetColumnByOrdinal(ordinal, _) =>  //如果是通过列的序号获取列（GetColumnByOrdinal），则根据提供的序号从候选属性（getAttrCandidates()）中获取对应的属性
          val attrCandidates = getAttrCandidates()
          assert(ordinal >= 0 && ordinal < attrCandidates.length)
          attrCandidates(ordinal)

        case GetViewColumnByNameAndOrdinal(  //如果是通过视图列的名称和序号来获取列（GetViewColumnByNameAndOrdinal），则通过列名过滤候选属性，确保与期望的候选数目一致，否则抛出异常
            viewName, colName, ordinal, expectedNumCandidates, viewDDL) =>
          val attrCandidates = getAttrCandidates()
          val matched = attrCandidates.filter(a => conf.resolver(a.name, colName))
          if (matched.length != expectedNumCandidates) {
            throw QueryCompilationErrors.incompatibleViewSchemaChangeError(
              viewName, colName, expectedNumCandidates, matched, viewDDL)
          }
          matched(ordinal)

        case u @ UnresolvedAttribute(nameParts) =>  //如果是 UnresolvedAttribute，则尝试通过 resolveColumnByName 解析列名，或者通过其他方法解析（例如字面量函数）
          val result = withPosition(u) {
            resolveColumnByName(nameParts).orElse(resolveLiteralFunction(nameParts)).map {
              // We trim unnecessary alias here. Note that, we cannot trim the alias at top-level,
              // as we should resolve `UnresolvedAttribute` to a named expression. The caller side
              // can trim the top-level alias if it's safe to do so. Since we will call
              // CleanupAliases later in Analyzer, trim non top-level unnecessary alias is safe.
              case Alias(child, _) if !isTopLevel => child
              case other => other
            }.getOrElse(u)
          }
          logDebug(s"Resolving $u to $result")
          result

        // Re-resolves `TempResolvedColumn` if it has tried to be resolved with Aggregate
        // but failed. If we still can't resolve it, we should keep it as `TempResolvedColumn`,
        // so that it won't become a fresh `TempResolvedColumn` again.
        //如果是临时解析列（TempResolvedColumn），且已经尝试过解析，则递归解析该列。如果依然无法解析为 UnresolvedAttribute，则返回该临时解析列
        case t: TempResolvedColumn if t.hasTried => withPosition(t) {
          innerResolve(UnresolvedAttribute(t.nameParts), isTopLevel) match {
            case _: UnresolvedAttribute => t
            case other => other
          }
        }
        //如果是 UnresolvedExtractValue（提取值表达式），递归解析它的子表达式。如果子表达式已经解析，则返回 ExtractValue，否则继续保持未解析的状态
        case u @ UnresolvedExtractValue(child, fieldName) =>
          val newChild = innerResolve(child, isTopLevel = false)
          if (newChild.resolved) {
            ExtractValue(newChild, fieldName, conf.resolver)
          } else {
            u.copy(child = newChild)
          }
         //对于其他类型的表达式，递归地解析其子表达式
        case _ => e.mapChildren(innerResolve(_, isTopLevel = false))
      }
      resolved.copyTagsFrom(e)
      resolved
    }

    try {
      val resolved = innerResolve(expr, isTopLevel = true)
      if (allowOuter) resolveOuterRef(resolved) else resolved
    } catch {
      case ae: AnalysisException if !throws =>
        logDebug(ae.getMessage)
        expr
    }
  }

  // Resolves `UnresolvedAttribute` to `OuterReference`.
  //用于将某些 UnresolvedAttribute（未解析的属性）转换为 外部引用（OuterReference）。它主要用于处理那些在外部查询中引用的字段，尤其是涉及子查询或外部查询中的聚合操作时
  protected def resolveOuterRef(e: Expression): Expression = {
    val outerPlan = AnalysisContext.get.outerPlan
    if (outerPlan.isEmpty) return e
    //负责根据 nameParts（即属性的名称部分）尝试在外部计划中解析该属性
    def resolve(nameParts: Seq[String]): Option[Expression] = try {
      outerPlan.get match {
        // Subqueries in UnresolvedHaving can host grouping expressions and aggregate functions.
        // We should resolve columns with `agg.output` and the rule `ResolveAggregateFunctions` will
        // push them down to Aggregate later. This is similar to what we do in `resolveColumns`.
        case u @ UnresolvedHaving(_, agg: Aggregate) =>
          //如果表达式是 UnresolvedHaving（表示 HAVING 子句中的未解析属性），并且该子查询包含聚合函数（Aggregate），它将尝试通过 Aggregate 的输出（agg.output）来解析属性
          agg.resolveChildren(nameParts, conf.resolver)
            .orElse(u.resolveChildren(nameParts, conf.resolver))
            .map(wrapOuterReference)
        case other =>
          other.resolveChildren(nameParts, conf.resolver).map(wrapOuterReference)
      }
    } catch {
      case ae: AnalysisException =>
        logDebug(ae.getMessage)
        None
    }

    e.transformWithPruning(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE, TEMP_RESOLVED_COLUMN)) {
      case u: UnresolvedAttribute =>
        resolve(u.nameParts).getOrElse(u)
      // Re-resolves `TempResolvedColumn` as outer references if it has tried to be resolved with
      // Aggregate but failed.
      case t: TempResolvedColumn if t.hasTried =>
        resolve(t.nameParts).getOrElse(t)
    }
  }

  // Resolves `UnresolvedAttribute` to `TempResolvedColumn` via `plan.child.output` if plan is an
  // `Aggregate`. If `TempResolvedColumn` doesn't end up as aggregate function input or grouping
  // column, we will undo the column resolution later to avoid confusing error message. E,g,, if
  // a table `t` has columns `c1` and `c2`, for query `SELECT ... FROM t GROUP BY c1 HAVING c2 = 0`,
  // even though we can resolve column `c2` here, we should undo it and fail with
  // "Column c2 not found".
  //过聚合节点的子查询输出（agg.child.output）来解析未解析的属性，如果最终这些列没有被用作聚合函数的输入或分组列，则会撤销解析，避免产生混淆的错误消息
  protected def resolveColWithAgg(e: Expression, plan: LogicalPlan): Expression = plan match {
    case agg: Aggregate =>
      e.transformWithPruning(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE)) {
        case u: UnresolvedAttribute =>
          try {
            agg.child.resolve(u.nameParts, conf.resolver).map({
              case a: Alias => TempResolvedColumn(a.child, u.nameParts)
              case o => TempResolvedColumn(o, u.nameParts)  //TempResolvedColumn 是一种临时的解析列，表示该列暂时被解析为某个表达式，但仍可能在后续过程中被撤销
            }).getOrElse(u)
          } catch {
            case ae: AnalysisException =>
              logDebug(ae.getMessage)
              u
          }
      }
    case _ => e
  }

  protected def resolveLateralColumnAlias(selectList: Seq[Expression]): Seq[Expression] = {
    if (!conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) return selectList

    // A mapping from lower-cased alias name to either the Alias itself, or the count of aliases
    // that have the same lower-cased name. If the count is larger than 1, we won't use it to
    // resolve lateral column aliases.
    val aliasMap = mutable.HashMap.empty[String, Either[Alias, Int]]

    def resolve(e: Expression): Expression = {
      e.transformUpWithPruning(
        _.containsAnyPattern(UNRESOLVED_ATTRIBUTE, LATERAL_COLUMN_ALIAS_REFERENCE)) {
        case w: WindowExpression if w.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE) =>
          w.transformDownWithPruning(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
            case lcaRef: LateralColumnAliasReference =>
              throw QueryCompilationErrors.lateralColumnAliasInWindowUnsupportedError(
                lcaRef.nameParts, w)
          }

        case u: UnresolvedAttribute =>
          // Lateral column alias does not have qualifiers. We always use the first name part to
          // look up lateral column aliases.
          val lowerCasedName = u.nameParts.head.toLowerCase(Locale.ROOT)
          aliasMap.get(lowerCasedName).map {
            case scala.util.Left(alias) =>
              if (alias.resolved) {
                val resolvedAttr = resolveExpressionByPlanOutput(
                  u, LocalRelation(Seq(alias.toAttribute)), throws = true
                ).asInstanceOf[NamedExpression]
                assert(resolvedAttr.resolved)
                LateralColumnAliasReference(resolvedAttr, u.nameParts, alias.toAttribute)
              } else {
                // Still returns a `LateralColumnAliasReference` even if the lateral column alias
                // is not resolved yet. This is to make sure we won't mistakenly resolve it to
                // outer references.
                LateralColumnAliasReference(u, u.nameParts, alias.toAttribute)
              }
            case scala.util.Right(count) =>
              throw QueryCompilationErrors.ambiguousLateralColumnAliasError(u.name, count)
          }.getOrElse(u)

        case LateralColumnAliasReference(u: UnresolvedAttribute, _, _) =>
          resolve(u)
      }
    }

    selectList.map {
      case a: Alias =>
        val result = resolve(a)
        val lowerCasedName = a.name.toLowerCase(Locale.ROOT)
        aliasMap.get(lowerCasedName) match {
          case Some(scala.util.Left(_)) =>
            aliasMap(lowerCasedName) = scala.util.Right(2)
          case Some(scala.util.Right(count)) =>
            aliasMap(lowerCasedName) = scala.util.Right(count + 1)
          case None =>
            aliasMap += lowerCasedName -> scala.util.Left(a)
        }
        result
      case other => resolve(other)
    }
  }

  /**
   * Resolves `UnresolvedAttribute`, `GetColumnByOrdinal` and extract value expressions(s) by the
   * input plan's output attributes. In order to resolve the nested fields correctly, this function
   * makes use of `throws` parameter to control when to raise an AnalysisException.
   *
   * Example :
   * SELECT * FROM t ORDER BY a.b
   *
   * In the above example, after `a` is resolved to a struct-type column, we may fail to resolve `b`
   * if there is no such nested field named "b". We should not fail and wait for other rules to
   * resolve it if possible.
   */
  //解析表达式中的未解析属性（如 UnresolvedAttribute），并尝试通过给定的逻辑计划（plan）的输出属性来解析它们
  def resolveExpressionByPlanOutput(
      expr: Expression,
      plan: LogicalPlan,
      throws: Boolean = false,
      allowOuter: Boolean = false): Expression = {
    resolveExpression(
      tryResolveColumnByPlanId(expr, plan),
      resolveColumnByName = nameParts => {
        plan.resolve(nameParts, conf.resolver)
      },
      getAttrCandidates = () => plan.output,
      throws = throws,
      allowOuter = allowOuter)
  }

  /**
   * Resolves `UnresolvedAttribute`, `GetColumnByOrdinal` and extract value expressions(s) by the
   * input plan's children output attributes.
   *
   * @param e The expression need to be resolved.
   * @param q The LogicalPlan whose children are used to resolve expression's attribute.
   * @return resolved Expression.
   */
  def resolveExpressionByPlanChildren(
      e: Expression,
      q: LogicalPlan,
      allowOuter: Boolean = false): Expression = {
    resolveExpression(
      tryResolveColumnByPlanId(e, q),
      resolveColumnByName = nameParts => {
        q.resolveChildren(nameParts, conf.resolver)
      },
      getAttrCandidates = () => {
        assert(q.children.length == 1)
        q.children.head.output
      },
      throws = true,
      allowOuter = allowOuter)
  }

  def resolveExprInAssignment(expr: Expression, hostPlan: LogicalPlan): Expression = {
    resolveExpressionByPlanChildren(expr, hostPlan) match {
      // Assignment key and value does not need the alias when resolving nested columns.
      case Alias(child: ExtractValue, _) => child
      case other => other
    }
  }

  // If the TreeNodeTag 'LogicalPlan.PLAN_ID_TAG' is attached, it means that the plan and
  // expression are from Spark Connect, and need to be resolved in this way:
  //    1. extract the attached plan id from UnresolvedAttribute;
  //    2. top-down traverse the query plan to find the plan node that matches the plan id;
  //    3. if can not find the matching node, fail the analysis due to illegal references;
  //    4. if more than one matching nodes are found, fail due to ambiguous column reference;
  //    5. resolve the expression with the matching node, if any error occurs here, return the
  //       original expression as it is.
  //目的是根据传入的 UnresolvedAttribute（未解析的属性）以及与之相关的 planId，在给定的逻辑计划 (LogicalPlan) 中查找并解析该属性。
  // 如果找不到相关的子计划，或者解析失败，方法会适当处理异常并返回 None
  private def tryResolveColumnByPlanId(
      e: Expression,  //未解析的属性表达式，表示当前需要解析的属性
      q: LogicalPlan,  //当前的逻辑计划。通常是整个查询计划，用于上下文查找和验证
      idToPlan: mutable.HashMap[Long, LogicalPlan] = mutable.HashMap.empty): Expression = e match {  //包含了计划 ID 与对应的逻辑计划之间的关系
    case u: UnresolvedAttribute =>
      resolveUnresolvedAttributeByPlanId(
        u, q, idToPlan: mutable.HashMap[Long, LogicalPlan]
      ).getOrElse(u)
    case _ if e.containsPattern(UNRESOLVED_ATTRIBUTE) =>
      e.mapChildren(c => tryResolveColumnByPlanId(c, q, idToPlan))
    case _ => e
  }

  private def resolveUnresolvedAttributeByPlanId(
      u: UnresolvedAttribute,
      q: LogicalPlan,
      idToPlan: mutable.HashMap[Long, LogicalPlan]): Option[NamedExpression] = {
    val planIdOpt = u.getTagValue(LogicalPlan.PLAN_ID_TAG)  //表示该属性所在的逻辑计划的ID
    if (planIdOpt.isEmpty) return None
    val planId = planIdOpt.get
    logDebug(s"Extract plan_id $planId from $u")

    val plan = idToPlan.getOrElseUpdate(planId, {
      findPlanById(u, planId, q).getOrElse {
        // For example:
        //  df1 = spark.createDataFrame([Row(a = 1, b = 2, c = 3)]])
        //  df2 = spark.createDataFrame([Row(a = 1, b = 2)]])
        //  df1.select(df2.a)   <-   illegal reference df2.a
        throw new AnalysisException(s"When resolving $u, " +
          s"fail to find subplan with plan_id=$planId in $q")
      }
    })

    try {
      //找到子计划后，调用 plan.resolve(u.nameParts, conf.resolver) 来解析属性 u。这里的 u.nameParts 是未解析属性的名称部分，conf.resolver 是用于解析属性的函数或规则
      plan.resolve(u.nameParts, conf.resolver)
    } catch {
      case e: AnalysisException =>
        logDebug(s"Fail to resolve $u with $plan due to $e")
        None
    }
  }
  //负责在给定的逻辑计划树中递归查找具有指定 planId 的子计划。方法的目标是根据 id 从树状结构的逻辑计划中找到匹配的子计划
  private def findPlanById(
      u: UnresolvedAttribute,
      id: Long,
      plan: LogicalPlan): Option[LogicalPlan] = {
    if (plan.getTagValue(LogicalPlan.PLAN_ID_TAG).contains(id)) {
      Some(plan)
    } else if (plan.children.length == 1) {
      findPlanById(u, id, plan.children.head)
    } else if (plan.children.length > 1) {
      val matched = plan.children.flatMap(findPlanById(u, id, _))
      if (matched.length > 1) {
        throw new AnalysisException(
          errorClass = "AMBIGUOUS_COLUMN_REFERENCE",
          messageParameters = Map("name" -> toSQLId(u.nameParts)),
          origin = u.origin
        )
      } else {
        matched.headOption
      }
    } else {
      None
    }
  }
}
