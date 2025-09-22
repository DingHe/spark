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

package org.apache.spark.sql.catalyst.plans

import scala.collection.mutable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Empty2Null, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.internal.SQLConf

/**
 * A trait that provides functionality to handle aliases in the `outputExpressions`.
 */
// 用于处理 outputExpressions 中的别名（Alias）。它的主要作用是构建一个 Expression -> Attribute 的映射关系，
// 以便在查询计划优化过程中正确地识别和替换 Alias，优化查询表达式的处理逻辑
trait AliasAwareOutputExpression extends SQLConfHelper {
  protected val aliasCandidateLimit = conf.getConf(SQLConf.EXPRESSION_PROJECTION_CANDIDATE_LIMIT)  //表示单个表达式最多可以存储多少个别名（Alias）
  protected def outputExpressions: Seq[NamedExpression]  //一个抽象方法，子类需要提供具体实现，返回 NamedExpression（即命名表达式）序列
  /**
   * This method can be used to strip expression which does not affect the result, for example:
   * strip the expression which is ordering agnostic for output ordering.
   */
  protected def strip(expr: Expression): Expression = expr   //去除不影响计算结果的表达式包装

  // Build an `Expression` -> `Attribute` alias map.
  // There can be multiple alias defined for the same expressions but it doesn't make sense to store
  // more than `aliasCandidateLimit` attributes for an expression. In those cases the old logic
  // handled only the last alias so we need to make sure that we give precedence to that.
  // If the `outputExpressions` contain simple attributes we need to add those too to the map.
  //为了在后续的查询优化过程中，能够快速地通过 Expression 查找对应的 Attribute
  @transient
  private lazy val aliasMap = {
    val aliases = mutable.Map[Expression, mutable.ArrayBuffer[Attribute]]()
    outputExpressions.reverse.foreach {
      case a @ Alias(child, _) =>
        //如果aliases中不存在child，则创建空的ArrayBuffer，否则返回原来的ArrayBuffer
        val buffer = aliases.getOrElseUpdate(strip(child).canonicalized, mutable.ArrayBuffer.empty)
        if (buffer.size < aliasCandidateLimit) {
          buffer += a.toAttribute  //加入对应的别名
        }
      case _ =>
    }
    //如果 outputExpressions 本身包含 Attribute，并且该 Attribute 存在于 aliases 中，那么也需要存入 aliasMap，保证 Alias 处理时不会遗漏原始 Attribute
    outputExpressions.foreach {
      case a: Attribute if aliases.contains(a.canonicalized) =>
        val buffer = aliases(a.canonicalized)
        if (buffer.size < aliasCandidateLimit) {
          buffer += a
        }
      case _ =>
    }
    aliases
  }

  protected def hasAlias: Boolean = aliasMap.nonEmpty

  /**
   * Return a stream of expressions in which the original expression is projected with `aliasMap`.
   */
  protected def projectExpression(expr: Expression): Stream[Expression] = {
    val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))  //通过 outputExpressions 构建 AttributeSet，用于快速查找某个 Attribute 是否在 outputExpressions 中
    expr.multiTransformDown {
      // Mapping with aliases
      // 递归遍历 expr，如果表达式在 aliasMap 中存在，则用 aliasMap 中的 Attribute 替换该表达式
      case e: Expression if aliasMap.contains(e.canonicalized) =>
        aliasMap(e.canonicalized).toSeq ++ (if (e.containsChild.nonEmpty) Seq(e) else Seq.empty) //如果 e 具有子表达式（containsChild.nonEmpty），则保留 e 作为候选项，避免丢失原始计算逻辑

      // Prune if we encounter an attribute that we can't map and it is not in output set.
      // This prune will go up to the closest `multiTransformDown()` call and returns `Stream.empty`
      // there.
      case a: Attribute if !outputSet.contains(a) => Seq.empty  //移除不在 outputSet 中的 Attribute
    }
  }
}

/**
 * A trait that handles aliases in the `orderingExpressions` to produce `outputOrdering` that
 * satisfies ordering requirements.
 */
//用于处理 orderingExpressions（排序表达式）中的 Alias，从而生成满足排序要求的 outputOrdering
trait AliasAwareQueryOutputOrdering[T <: QueryPlan[T]]
  extends AliasAwareOutputExpression { self: QueryPlan[T] =>
  protected def orderingExpressions: Seq[SortOrder]  //子类需要实现它，返回用于排序的 SortOrder 序列

  override protected def strip(expr: Expression): Expression = expr match {
    case e: Empty2Null => strip(e.child)  //Empty2Null 是 Spark SQL 计划中的一种表达式转换，它用于将空字符串转换为 NULL，但不会影响排序逻辑，所以可以去掉
    case _ => expr
  }

  override final def outputOrdering: Seq[SortOrder] = {
    val newOrdering: Iterator[Option[SortOrder]] = if (hasAlias) {
      // Take the first `SortOrder`s only until they can be projected.
      // E.g. we have child ordering `Seq(SortOrder(a), SortOrder(b))` then
      // if only `a AS x` can be projected then we can return Seq(SortOrder(x))`
      // but if only `b AS y` can be projected we can't return `Seq(SortOrder(y))`.
      orderingExpressions.iterator.map { sortOrder =>
        val orderingSet = mutable.Set.empty[Expression]
        val sameOrderings = sortOrder.children.toStream
          .flatMap(projectExpression)
          .filter(e => orderingSet.add(e.canonicalized))
          .take(aliasCandidateLimit)
        if (sameOrderings.nonEmpty) {
          Some(sortOrder.copy(child = sameOrderings.head,
            sameOrderExpressions = sameOrderings.tail))
        } else {
          None
        }
      }
    } else {
      // Make sure the returned ordering are valid (only reference output attributes of the current
      // plan node). Same as above (the if branch), we take the first ordering expressions that are
      // all valid.
      val outputSet = AttributeSet(outputExpressions.map(_.toAttribute))
      orderingExpressions.iterator.map { order =>
        val validChildren = order.children.filter(_.references.subsetOf(outputSet))
        if (validChildren.nonEmpty) {
          Some(order.copy(child = validChildren.head, sameOrderExpressions = validChildren.tail))
        } else {
          None
        }
      }
    }
    newOrdering.takeWhile(_.isDefined).flatten.toSeq
  }
}
