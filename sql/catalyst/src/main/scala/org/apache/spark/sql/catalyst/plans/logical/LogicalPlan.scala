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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{AliasAwareQueryOutputOrdering, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.LogicalPlanStats
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, TreeNodeTag, UnaryLike}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.MetadataColumnHelper
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types.StructType

//用于表示 SQL 查询的逻辑计划。它继承自 QueryPlan 类，并包含了一些特定于逻辑计划的属性和方法。
// 这个类主要用于查询优化过程中的数据结构，它的子类负责具体的 SQL 逻辑操作
abstract class LogicalPlan
  extends QueryPlan[LogicalPlan]
  with AnalysisHelper
  with LogicalPlanStats
  with LogicalPlanDistinctKeys
  with QueryPlanConstraints
  with Logging {

  /**
   * Metadata fields that can be projected from this node.
   * Should be overridden if the plan does not propagate its children's output.
   */
    //如果子节点有元数据输出，那么当前节点将继承这些元数据字段。通常用于处理额外的查询信息，比如某些 SQL 操作的统计信息
  def metadataOutput: Seq[Attribute] = children.flatMap(_.metadataOutput)

  /**
   * Searches for a metadata attribute by its logical name.
   *
   * The search works in spite of conflicts with column names in the data schema.
   */
    //根据列的逻辑名称从当前节点的输出和元数据输出中查找匹配的元数据属性。它允许在输出列名称和元数据名称之间进行解析
  def getMetadataAttributeByNameOpt(name: String): Option[AttributeReference] = {
    // NOTE: An already-referenced column might appear in `output` instead of `metadataOutput`.
    (metadataOutput ++ output).collectFirst {
      case MetadataAttributeWithLogicalName(attr, logicalName)
          if conf.resolver(name, logicalName) => attr
    }
  }

  /**
   * Returns the metadata attribute having the specified logical name.
   *
   * Throws [[AnalysisException]] if no such metadata attribute exists.
   */
    //与 getMetadataAttributeByNameOpt 类似，不过该方法在找不到属性时会抛出异常
  def getMetadataAttributeByName(name: String): AttributeReference = {
    getMetadataAttributeByNameOpt(name).getOrElse {
      val availableMetadataColumns = (metadataOutput ++ output).collect {
        case MetadataAttributeWithLogicalName(_, logicalName) => logicalName
      }
      throw QueryCompilationErrors.unresolvedAttributeError(
        "UNRESOLVED_COLUMN", name, availableMetadataColumns.distinct, origin)
    }
  }

  /** Returns true if this subtree has data from a streaming data source. */
    //如果当前计划或任何子节点是流式的，返回 true，否则返回 false
  def isStreaming: Boolean = _isStreaming
  private[this] lazy val _isStreaming = children.exists(_.isStreaming)
  //返回当前节点的详细信息，并附加统计信息（如果有）
  override def verboseStringWithSuffix(maxFields: Int): String = {
    super.verboseString(maxFields) + statsCache.map(", " + _.toString).getOrElse("")
  }

  /**
   * Returns the maximum number of rows that this plan may compute.
   *
   * Any operator that a Limit can be pushed passed should override this function (e.g., Union).
   * Any operator that can push through a Limit should override this function (e.g., Project).
   */
    //返回该计划计算的最大行数
  def maxRows: Option[Long] = None

  /**
   * Returns the maximum number of rows this plan may compute on each partition.
   */
    //返回每个分区上最大计算行数
  def maxRowsPerPartition: Option[Long] = maxRows

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
   */
    //判断当前计划是否已经解析完成
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * Returns true if all its children of this query plan have been resolved.
   */
    //判断当前节点的所有子节点是否已解析
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Resolves a given schema to concrete [[Attribute]] references in this query plan. This function
   * should only be called on analyzed plans since it will throw [[AnalysisException]] for
   * unresolved [[Attribute]]s.
   */
    //于将给定的模式解析为 Attribute 引用，确保查询计划中的每个字段都能被解析。如果某个字段无法解析，则会抛出 AnalysisException
  def resolve(schema: StructType, resolver: Resolver): Seq[Attribute] = {
    schema.map { field =>
      resolve(field.name :: Nil, resolver).map {
        case a: AttributeReference =>
          // Keep the metadata in given schema.
          a.withMetadata(field.metadata)
        case _ => throw QueryExecutionErrors.resolveCannotHandleNestedSchema(this)
      }.getOrElse {
        throw QueryCompilationErrors.cannotResolveAttributeError(
          field.name, output.map(_.name).mkString(", "))
      }
    }
  }
  //
  private[this] lazy val childAttributes = AttributeSeq.fromNormalOutput(children.flatMap(_.output))

  private[this] lazy val childMetadataAttributes = AttributeSeq(children.flatMap(_.metadataOutput))

  private[this] lazy val outputAttributes = AttributeSeq.fromNormalOutput(output)

  private[this] lazy val outputMetadataAttributes = AttributeSeq(metadataOutput)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
    //法用于从子节点的输出和元数据输出中解析给定的属性，返回对应的 NamedExpression
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    childAttributes.resolve(nameParts, resolver)
      .orElse(childMetadataAttributes.resolve(nameParts, resolver))

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    outputAttributes.resolve(nameParts, resolver)
      .orElse(outputMetadataAttributes.resolve(nameParts, resolver))

  /**
   * Given an attribute name, split it to name parts by dot, but
   * don't split the name parts quoted by backticks, for example,
   * `ab.cd`.`efg` should be split into two parts "ab.cd" and "efg".
   */
    //如果属性名称带有反引号（如 ab.cd），该方法会根据给定的解析器进行解析
  def resolveQuoted(
      name: String,
      resolver: Resolver): Option[NamedExpression] = {
    resolve(UnresolvedAttribute.parseAttributeName(name), resolver)
  }

  /**
   * Refreshes (or invalidates) any metadata/data cached in the plan recursively.
   */
    //刷新或使当前计划中的所有缓存失效
  def refresh(): Unit = children.foreach(_.refresh())

  /**
   * Returns true iff `other`'s output is semantically the same, i.e.:
   *  - it contains the same number of `Attribute`s;
   *  - references are the same;
   *  - the order is equal too.
   */
    //判断当前计划和另一个计划的输出是否语义上相同
  def sameOutput(other: LogicalPlan): Boolean = {
    val thisOutput = this.output
    val otherOutput = other.output
    thisOutput.length == otherOutput.length && thisOutput.zip(otherOutput).forall {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }
}

object LogicalPlan {
  // A dedicated tag for Spark Connect.
  // If an expression (only support UnresolvedAttribute for now) was attached by this tag,
  // the analyzer will:
  //    1, extract the plan id;
  //    2, top-down traverse the query plan to find the node that was attached by the same tag.
  //    and fails the whole analysis if can not find it;
  //    3, resolve this expression with the matching node. If any error occurs, analyzer fallbacks
  //    to the old code path.
  private[spark] val PLAN_ID_TAG = TreeNodeTag[Long]("plan_id")
}

/** 表示叶子节点
 * A logical plan node with no children.
 */
trait LeafNode extends LogicalPlan with LeafLike[LogicalPlan] {
  override def producedAttributes: AttributeSet = outputSet

  /** Leaf nodes that can survive analysis must define their own statistics. */
  def computeStats(): Statistics = throw new UnsupportedOperationException
}

/**
 * A logical plan node with single child.
 */
trait UnaryNode extends LogicalPlan with UnaryLike[LogicalPlan] {
  /**
   * Generates all valid constraints including an set of aliased constraints by replacing the
   * original constraint expressions with the corresponding alias
   */
    //生成所有有效的约束，并包括通过别名替换原始约束表达式的约束
  protected def getAllValidConstraints(projectList: Seq[NamedExpression]): ExpressionSet = {
    var allConstraints = child.constraints
    projectList.foreach {
      case a @ Alias(l: Literal, _) =>  //表示该表达式是一个字面量（Literal）的别名
        allConstraints += EqualNullSafe(a.toAttribute, l)  //生成一个 EqualNullSafe 约束，这种约束表示在比较时，即使是 null 值也视为相等
      case a @ Alias(e, _) if e.deterministic =>
        // For every alias in `projectList`, replace the reference in constraints by its attribute.
        allConstraints ++= allConstraints.map(_ transform {  //表示该表达式是一个确定性的表达式的别名
          case expr: Expression if expr.semanticEquals(e) =>
            a.toAttribute   //更新 allConstraints 中的所有约束，将其中所有对 e 的引用替换为别名的属性 a.toAttribute，并为该别名和原表达式 e 之间生成一个 EqualNullSafe 约束
        })
        allConstraints += EqualNullSafe(e, a.toAttribute)
      case _ => // Don't change.
    }

    allConstraints
  }

  override protected lazy val validConstraints: ExpressionSet = child.constraints
}

/**
 * A logical plan node with a left and right child.
 */
trait BinaryNode extends LogicalPlan with BinaryLike[LogicalPlan]

trait OrderPreservingUnaryNode extends UnaryNode
  with AliasAwareQueryOutputOrdering[LogicalPlan] {
  override protected def outputExpressions: Seq[NamedExpression] = child.output
  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering
}
//检查逻辑计划在处理过程中是否符合各种约束条件，防止不一致的情况出现
object LogicalPlanIntegrity {
  //检查逻辑计划 p 是否可以获取输出属性（output）。它确保计划已解析，并且其中不包含无法解析的子查询表达式，尤其是 ScalarSubquery（标量子查询）没有任何列的情况
  def canGetOutputAttrs(p: LogicalPlan): Boolean = {
    p.resolved && !p.expressions.exists { e =>
      e.exists {
        // We cannot call `output` in plans with a `ScalarSubquery` expr having no column,
        // so, we filter out them in advance.
        case s: ScalarSubquery => s.plan.schema.fields.isEmpty  //检查 expressions 中是否有 ScalarSubquery，且该子查询没有任何列
        case _ => false
      }
    }
  }

  /**
   * Since some logical plans (e.g., `Union`) can build `AttributeReference`s in their `output`,
   * this method checks if the same `ExprId` refers to attributes having the same data type
   * in plan output. Returns the error message if the check does not pass.
   */
  //检查逻辑计划输出中，是否存在相同的 ExprId 对应不同的数据类型
  def hasUniqueExprIdsForOutput(plan: LogicalPlan): Option[String] = {
    //从逻辑计划中收集所有的 ExprId 和对应的 dataType，并确保它们在输出中是唯一的
    val exprIds = plan.collect { case p if canGetOutputAttrs(p) =>
      // NOTE: we still need to filter resolved expressions here because the output of
      // some resolved logical plans can have unresolved references,
      // e.g., outer references in `ExistenceJoin`.
      p.output.filter(_.resolved).map { a => (a.exprId, a.dataType.asNullable) }
    }.flatten
    //忽略了 Union 操作的 ExprId，因为 Union 中的 ExprId 可能会重复使用
    val ignoredExprIds = plan.collect {
      // NOTE: `Union` currently reuses input `ExprId`s for output references, but we cannot
      // simply modify the code for assigning new `ExprId`s in `Union#output` because
      // the modification will make breaking changes (See SPARK-32741(#29585)).
      // So, this check just ignores the `exprId`s of `Union` output.
      case u: Union if u.resolved => u.output.map(_.exprId)
    }.flatten.toSet

    val groupedDataTypesByExprId = exprIds.filterNot { case (exprId, _) =>
      ignoredExprIds.contains(exprId)
    }.groupBy(_._1).values.map(_.distinct)
    //如果发现问题，返回一个 Option[String]，包含错误信息；否则返回 None
    groupedDataTypesByExprId.collectFirst {
      case group if group.length > 1 =>
        val exprId = group.head._1
        val types = group.map(_._2.sql)
        s"Multiple attributes have the same expression ID ${exprId.id} but different data types: " +
          types.mkString(", ") + ". The plan tree:\n" + plan.treeString
    }
  }

  /**
   * This method checks if reference `ExprId`s are not reused when assigning a new `ExprId`.
   * For example, it returns false if plan transformers create an alias having the same `ExprId`
   * with one of reference attributes, e.g., `a#1 + 1 AS a#1`. Returns the error message if the
   * check does not pass.
   */
  def checkIfSameExprIdNotReused(plan: LogicalPlan): Option[String] = {
    plan.collectFirst { case p if p.resolved =>
      p.expressions.collectFirst {
        // Even if a plan is resolved, `a.references` can return unresolved references,
        // e.g., in `Grouping`/`GroupingID`, so we need to filter out them and
        // check if the same `exprId` in `Alias` does not exist among reference `exprId`s.
        case a: Alias if a.references.filter(_.resolved).map(_.exprId).exists(_ == a.exprId) =>
          "An alias reuses the same expression ID as previously present in an attribute, " +
            s"which is invalid: ${a.sql}. The plan tree:\n" + plan.treeString
      }
    }.flatten
  }

  /**
   * This method checks if the same `ExprId` refers to an unique attribute in a plan tree.
   * Some plan transformers (e.g., `RemoveNoopOperators`) rewrite logical
   * plans based on this assumption. Returns the error message if the check does not pass.
   */
  def validateExprIdUniqueness(plan: LogicalPlan): Option[String] = {
    LogicalPlanIntegrity.checkIfSameExprIdNotReused(plan).orElse(
      LogicalPlanIntegrity.hasUniqueExprIdsForOutput(plan))
  }

  /**
   * Validate the structural integrity of an optimized plan.
   * For example, we can check after the execution of each rule that each plan:
   * - is still resolved
   * - only host special expressions in supported operators
   * - has globally-unique attribute IDs
   * - has the same result schema as the previous plan
   * - has no dangling attribute references
   */
  def validateOptimizedPlan(
      previousPlan: LogicalPlan,
      currentPlan: LogicalPlan): Option[String] = {
    if (!currentPlan.resolved) {
      Some("The plan becomes unresolved: " + currentPlan.treeString + "\nThe previous plan: " +
        previousPlan.treeString)
    } else if (currentPlan.exists(PlanHelper.specialExpressionsInUnsupportedOperator(_).nonEmpty)) {
      Some("Special expressions are placed in the wrong plan: " + currentPlan.treeString)
    } else {
      LogicalPlanIntegrity.validateExprIdUniqueness(currentPlan).orElse {
        if (!DataTypeUtils.equalsIgnoreNullability(previousPlan.schema, currentPlan.schema)) {
          Some(s"The plan output schema has changed from ${previousPlan.schema.sql} to " +
            currentPlan.schema.sql + s". The previous plan: ${previousPlan.treeString}\nThe new " +
            "plan:\n" + currentPlan.treeString)
        } else {
          None
        }
      }
    }
  }
}

/**
 * A logical plan node that can generate metadata columns
 */
trait ExposesMetadataColumns extends LogicalPlan {
  protected def metadataOutputWithOutConflicts(
      metadataOutput: Seq[AttributeReference],
      renameOnConflict: Boolean = true): Seq[AttributeReference] = {
    // If `metadataColFromOutput` is not empty that means `AddMetadataColumns` merged
    // metadata output into output. We should still return an available metadata output
    // so that the rule `ResolveReferences` can resolve metadata column correctly.
    val metadataColFromOutput = output.filter(_.isMetadataCol)
    if (metadataColFromOutput.isEmpty) {
      val resolve = conf.resolver
      val outputNames = outputSet.map(_.name)

      def isOutputColumn(colName: String): Boolean = outputNames.exists(resolve(colName, _))

      @scala.annotation.tailrec
      def makeUnique(name: String): String =
        if (isOutputColumn(name)) makeUnique(s"_$name") else name

      // If allowed to, resolve any name conflicts between metadata and output columns by renaming
      // the conflicting metadata columns; otherwise, suppress them.
      metadataOutput.collect {
        case attr if !isOutputColumn(attr.name) => attr
        case attr if renameOnConflict => attr.withName(makeUnique(attr.name))
      }
    } else {
      metadataColFromOutput.asInstanceOf[Seq[AttributeReference]]
    }
  }

  def withMetadataColumns(): LogicalPlan
}
