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

import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Project}
import org.apache.spark.sql.types.Metadata

/**
 * Helper methods for collecting and replacing aliases.
 */
trait AliasHelper {
  // 从 Project 操作（即 SELECT 操作）中获取所有别名的映射
  protected def getAliasMap(plan: Project): AttributeMap[Alias] = {
    // Create a map of Aliases to their values from the child projection.
    // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> Alias(a + b, c)).
    getAliasMap(plan.projectList)
  }
  // 从聚合表达式中提取出所有不包含聚合函数（如 Sum、Count）或 Python UDF 的别名，并将别名及其对应的表达式映射到 AttributeMap 中
  protected def getAliasMap(plan: Aggregate): AttributeMap[Alias] = {
    // Find all the aliased expressions in the aggregate list that don't include any actual
    // AggregateExpression or PythonUDF, and create a map from the alias to the expression
    val aliasMap = plan.aggregateExpressions.collect {
      case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
        (a.toAttribute, a)
    }
    AttributeMap(aliasMap)
  }
  // 遍历表达式列表，收集所有 Alias 表达式，并将它们与对应的属性一起映射到 AttributeMap 中
  protected def getAliasMap(exprs: Seq[NamedExpression]): AttributeMap[Alias] = {
    // Create a map of Aliases to their values from the child projection.
    // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> Alias(a + b, c)).
    AttributeMap(exprs.collect { case a: Alias => (a.toAttribute, a) })
  }

  /**
   * Replace all attributes, that reference an alias, with the aliased expression
   */
  // 根据属性到别名的映射表，替换表达式中的列引用为真实表达式（或新的别名表达式）。
  // 然后再清理掉无用的 Alias。
  protected def replaceAlias(
      expr: Expression,
      aliasMap: AttributeMap[Alias]): Expression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId,
    trimAliases(expr.transformUp {
      case a: Attribute => aliasMap.getOrElse(a, a)
    })
  }

  /**
   * Replace all attributes, that reference an alias, with the aliased expression,
   * but keep the name of the outermost attribute.
   */
  // 如果属性是 Attribute 类型，保留其原始名称；否则，使用 transformUp 递归替换其子节点中的别名
  protected def replaceAliasButKeepName(
     expr: NamedExpression,
     aliasMap: AttributeMap[Alias]): NamedExpression = {
    expr match {
      // We need to keep the `Alias` if we replace a top-level Attribute, so that it's still a
      // `NamedExpression`. We also need to keep the name of the original Attribute.
      case a: Attribute => aliasMap.get(a).map(_.withName(a.name)).getOrElse(a)
      case o =>
        // Use transformUp to prevent infinite recursion when the replacement expression
        // redefines the same ExprId.
        o.mapChildren(_.transformUp {
          case a: Attribute => aliasMap.get(a).map(_.child).getOrElse(a)
        }).asInstanceOf[NamedExpression]
    }
  }
  // 返回经过“去别名”处理后的 Expression
  protected def trimAliases(e: Expression): Expression = e match {
    // The children of `CreateNamedStruct` may use `Alias` to carry metadata and we should not
    // trim them.
    // 如果 CreateNamedStruct 的子节点是 Alias(colA, "x") 且 metadata 不为空 → 保留 Alias（用于传递 metadata）
    // 如果是 Alias(colB, "y") 且 metadata 为空 → 会对 colB 继续调用 trimAliases（最终可能返回 colB 本身），也就是去掉 Alias
    case c: CreateNamedStruct => c.mapChildren {
      case a: Alias if a.metadata != Metadata.empty => a
      case other => trimAliases(other)
    }
    // 无论别名名是什么，都去掉别名包装，直接递归处理其 child 并返回 child 的处理结果
    case a @ Alias(child, _) => trimAliases(child)
    case MultiAlias(child, _) => trimAliases(child)
    // 会对 other 的每个子表达式应用 trimAliases，并返回一个新的表达式（如果子表达式有变化则构造新的节点；否则通常返回原节点）
    case other => other.mapChildren(trimAliases)
  }
  // 该方法与 trimAliases 类似，但它只去除非顶层的 Alias，对于顶层 Alias（如在 SELECT 子句中），保留它们，并保持其元数据
  protected def trimNonTopLevelAliases[T <: Expression](e: T): T = {
    val res = e match {
      case a: Alias =>
        val metadata = if (a.metadata == Metadata.empty) {
          None
        } else {
          Some(a.metadata)
        }
        a.copy(child = trimAliases(a.child))(
          exprId = a.exprId,
          qualifier = a.qualifier,
          explicitMetadata = metadata,
          nonInheritableMetadataKeys = a.nonInheritableMetadataKeys)
      case a: MultiAlias =>
        a.copy(child = trimAliases(a.child))
      case other => trimAliases(other)
    }

    res.asInstanceOf[T]
  }
}
