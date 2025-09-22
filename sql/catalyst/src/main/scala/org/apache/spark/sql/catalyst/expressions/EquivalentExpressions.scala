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

package org.apache.spark.sql.catalyst.expressions

import java.util.Objects

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 */
//用于计算表达式（Expression）树相等性的工具类
//主要作用是将不同的表达式组合成一组等价表达式，并在这些表达式之间进行比较。如果两个表达式在相同的输入下返回相同的结果，则认为它们是等价的
//skipForShortcutEnable 是否启用快速跳过某些表达式的子表达式
class EquivalentExpressions(
    skipForShortcutEnable: Boolean = SQLConf.get.subexpressionEliminationSkipForShotcutExpr) {
  //存储等价的表达式及其相关统计信息
  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */ //将一个表达式 expr 添加到 equivalenceMap 中。如果该表达式已经存在于 equivalenceMap 中，则返回 true，表示找到了匹配的表达式；如果没有匹配，则返回 false
  def addExpr(expr: Expression): Boolean = {
    if (supportedExpression(expr)) {
      updateExprInMap(expr, equivalenceMap)
    } else {
      false
    }
  }

  /**
   * Adds or removes an expression to/from the map and updates `useCount`.
   * Returns true
   * - if there was a matching expression in the map before add or
   * - if there remained a matching expression in the map after remove (`useCount` remained > 0)
   * to indicate there is no need to recurse in `updateExprTree`.
   */ //根据表达式的等价性将其添加或删除，并更新其使用计数（useCount）
  private def updateExprInMap(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats],
      useCount: Int = 1): Boolean = {
    if (expr.deterministic) {
      val wrapper = ExpressionEquals(expr)
      map.get(wrapper) match {
        case Some(stats) =>  //如果找到了匹配的表达式，则更新使用计数。如果使用计数为 0，则从 equivalenceMap 中移除该表达式
          stats.useCount += useCount
          if (stats.useCount > 0) {
            true
          } else if (stats.useCount == 0) {
            map -= wrapper
            false
          } else {
            // Should not happen
            throw new IllegalStateException(
              s"Cannot update expression: $expr in map: $map with use count: $useCount")
          }
        case _ =>  //如果没有找到匹配的表达式，则将其添加到 equivalenceMap
          if (useCount > 0) {
            map.put(wrapper, ExpressionStats(expr)(useCount))
          }
          false
      }
    } else {
      false
    }
  }

  /**
   * Adds or removes only expressions which are common in each of given expressions, in a recursive
   * way.
   * For example, given two expressions `(a + (b + (c + 1)))` and `(d + (e + (c + 1)))`, the common
   * expression `(c + 1)` will be added into `equivalenceMap`.
   *
   * Note that as we don't know in advance if any child node of an expression will be common across
   * all given expressions, we compute local equivalence maps for all given expressions and filter
   * only the common nodes.
   * Those common nodes are then removed from the local map and added to the final map of
   * expressions.
   */ //递归地添加所有表达式中公共的子表达式。递归地遍历多个表达式，找出它们共享的子表达式，并将其添加到 equivalenceMap
  private def updateCommonExprs(
      exprs: Seq[Expression],
      map: mutable.HashMap[ExpressionEquals, ExpressionStats],
      useCount: Int): Unit = {
    assert(exprs.length > 1)
    var localEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
    updateExprTree(exprs.head, localEquivalenceMap)

    exprs.tail.foreach { expr =>
      val otherLocalEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
      updateExprTree(expr, otherLocalEquivalenceMap)
      localEquivalenceMap = localEquivalenceMap.filter { case (key, _) =>
        otherLocalEquivalenceMap.contains(key)
      }
    }

    // Start with the highest expression, remove it from `localEquivalenceMap` and add it to `map`.
    // The remaining highest expression in `localEquivalenceMap` is also common expression so loop
    // until `localEquivalenceMap` is not empty.
    var statsOption = Some(localEquivalenceMap).filter(_.nonEmpty).map(_.maxBy(_._1.height)._2)
    while (statsOption.nonEmpty) {
      val stats = statsOption.get
      updateExprTree(stats.expr, localEquivalenceMap, -stats.useCount)
      updateExprTree(stats.expr, map, useCount)

      statsOption = Some(localEquivalenceMap).filter(_.nonEmpty).map(_.maxBy(_._1.height)._2)
    }
  }
  //根据 skipForShortcutEnable 配置，决定是否跳过某些表达式的子表达式
  private def skipForShortcut(expr: Expression): Expression = {
    if (skipForShortcutEnable) {
      // The subexpression may not need to eval even if it appears more than once.
      // e.g., `if(or(a, and(b, b)))`, the expression `b` would be skipped if `a` is true.
      expr match {
        case and: And => and.left
        case or: Or => or.left
        case other => other
      }
    } else {
      expr
    }
  }

  // There are some special expressions that we should not recurse into all of its children.
  //   1. CodegenFallback: it's children will not be used to generate code (call eval() instead)
  //   2. ConditionalExpression: use its children that will always be evaluated.
  //目的是确定在遍历表达式树时，应该递归处理哪些子表达式
  private def childrenToRecurse(expr: Expression): Seq[Expression] = expr match {
    case _: CodegenFallback => Nil //子表达式不会参与生成代码，而是应该通过直接调用 eval() 来进行求值，不需要递归
    case c: ConditionalExpression => c.alwaysEvaluatedInputs.map(skipForShortcut)  //条件表达式总是被评估的表达式，就会继续递归
    case other => skipForShortcut(other).children
  }

  // For some special expressions we cannot just recurse into all of its children, but we can
  // recursively add the common expressions shared between all of its children.
  //确定哪些子表达式是公共的，需要递归遍历
  private def commonChildrenToRecurse(expr: Expression): Seq[Seq[Expression]] = expr match {
    case _: CodegenFallback => Nil
    case c: ConditionalExpression => c.branchGroups  //条件表达式的分支总是被递归
    case _ => Nil
  }
  //检查一个表达式是否支持等价性比较
  private def supportedExpression(e: Expression) = {
    !e.exists {
      // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
      // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
      case _: LambdaVariable => true

      // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
      // can cause error like NPE.
      case _: PlanExpression[_] => Utils.isInRunningSparkTask

      case _ => false
    }
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   */
  def addExprTree(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats] = equivalenceMap): Unit = {
    if (supportedExpression(expr)) {
      updateExprTree(expr, map)
    }
  }
  //递归地更新表达式树，直到找到匹配的表达式或者遍历完所有子表达式
  private def updateExprTree(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats] = equivalenceMap,
      useCount: Int = 1): Unit = {
    val skip = useCount == 0 || expr.isInstanceOf[LeafExpression]

    if (!skip && !updateExprInMap(expr, map, useCount)) {
      val uc = useCount.signum
      childrenToRecurse(expr).foreach(updateExprTree(_, map, uc))
      commonChildrenToRecurse(expr).filter(_.nonEmpty).foreach(updateCommonExprs(_, map, uc))
    }
  }

  /**
   * Returns the state of the given expression in the `equivalenceMap`. Returns None if there is no
   * equivalent expressions.
   */
  def getExprState(e: Expression): Option[ExpressionStats] = {
    if (supportedExpression(e)) {
      equivalenceMap.get(ExpressionEquals(e))
    } else {
      None
    }
  }

  // Exposed for testing.
  private[sql] def getAllExprStates(count: Int = 0): Seq[ExpressionStats] = {
    equivalenceMap.filter(_._2.useCount > count).toSeq.sortBy(_._1.height).map(_._2)
  }

  /**
   * Returns a sequence of expressions that more than one equivalent expressions.
   */
  def getCommonSubexpressions: Seq[Expression] = {
    getAllExprStates(1).map(_.expr)
  }

  /**
   * Returns the state of the data structure as a string. If `all` is false, skips sets of
   * equivalent expressions with cardinality 1.
   */
  def debugString(all: Boolean = false): String = {
    val sb = new java.lang.StringBuilder()
    sb.append("Equivalent expressions:\n")
    equivalenceMap.values.filter(stats => all || stats.useCount > 1).foreach { stats =>
      sb.append("  ").append(s"${stats.expr}: useCount = ${stats.useCount}").append('\n')
    }
    sb.toString()
  }
}

/**
 * Wrapper around an Expression that provides semantic equality.
 */
//为Expression提供了语义上的相等性（semantic equality）检查，
//能够通过语义比较来判断两个表达式是否等价，同时还引入了一个子树的高度（height）来优化比较过程
case class ExpressionEquals(e: Expression) {
  //高度是指从当前表达式（根节点）到最深的叶子节点的最长路径的长度
  private def getHeight(tree: Expression): Int = {
    //map(getHeight) 会对每个子节点递归调用 getHeight，得到子节点的高度
    //reduceOption(_ max _) 会从所有子节点的高度中选择最大的高度，并加1表示当前节点的高度
    //reduceOption是一个集合操作方法，它类似于reduce，但与reduce不同，reduceOption返回一个Option类型的结果。
    // 这意味着，如果集合为空，reduceOption会返回None，而不是抛出异常
    tree.children.map(getHeight).reduceOption(_ max _).getOrElse(0) + 1
  }

  // This is used to do a fast pre-check for child-parent relationship. For example, expr1 can
  // only be a parent of expr2 if expr1.height is larger than expr2.height.
  lazy val height = getHeight(e)

  override def equals(o: Any): Boolean = o match {
    case other: ExpressionEquals => e.semanticEquals(other.e) && height == other.height
    case _ => false
  }

  override def hashCode: Int = Objects.hash(e.semanticHash(): Integer, height: Integer)
}

/**
 * A wrapper in place of using Seq[Expression] to record a group of equivalent expressions.
 *
 * This saves a lot of memory when there are a lot of expressions in a same equivalence group.
 * Instead of appending to a mutable list/buffer of Expressions, just update the "flattened"
 * useCount in this wrapper in-place.
 */
case class ExpressionStats(expr: Expression)(var useCount: Int)
