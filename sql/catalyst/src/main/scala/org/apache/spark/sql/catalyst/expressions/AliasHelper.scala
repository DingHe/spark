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
  //从 Project 操作（即 SELECT 操作）中获取所有别名的映射
  protected def getAliasMap(plan: Project): AttributeMap[Alias] = {
    // Create a map of Aliases to their values from the child projection.
    // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> Alias(a + b, c)).
    getAliasMap(plan.projectList)
  }
  //从聚合表达式中提取出所有不包含聚合函数（如 Sum、Count）或 Python UDF 的别名，并将别名及其对应的表达式映射到 AttributeMap 中
  protected def getAliasMap(plan: Aggregate): AttributeMap[Alias] = {
    // Find all the aliased expressions in the aggregate list that don't include any actual
    // AggregateExpression or PythonUDF, and create a map from the alias to the expression
    val aliasMap = plan.aggregateExpressions.collect {
      case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
        (a.toAttribute, a)
    }
    AttributeMap(aliasMap)
  }
  //遍历表达式列表，收集所有 Alias 表达式，并将它们与对应的属性一起映射到 AttributeMap 中
  protected def getAliasMap(exprs: Seq[NamedExpression]): AttributeMap[Alias] = {
    // Create a map of Aliases to their values from the child projection.
    // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> Alias(a + b, c)).
    AttributeMap(exprs.collect { case a: Alias => (a.toAttribute, a) })
  }

  /**
   * Replace all attributes, that reference an alias, with the aliased expression
   */
    //使用 transformUp 方法递归遍历表达式树，查找并替换其中引用了别名的 Attribute，将其替换为对应的 Alias 表达式
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
    //如果属性是 Attribute 类型，保留其原始名称；否则，使用 transformUp 递归替换其子节点中的别名
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
  //对于每个 Alias，如果其包含的元数据不为空，保留 Alias；否则，去除它。
  // 如果表达式是 MultiAlias 或 CreateNamedStruct，则递归处理其子表达式
  protected def trimAliases(e: Expression): Expression = e match {
    // The children of `CreateNamedStruct` may use `Alias` to carry metadata and we should not
    // trim them.
    case c: CreateNamedStruct => c.mapChildren {
      case a: Alias if a.metadata != Metadata.empty => a
      case other => trimAliases(other)
    }
    case a @ Alias(child, _) => trimAliases(child)
    case MultiAlias(child, _) => trimAliases(child)
    case other => other.mapChildren(trimAliases)
  }
  //该方法与 trimAliases 类似，但它只去除非顶层的 Alias，对于顶层 Alias（如在 SELECT 子句中），保留它们，并保持其元数据
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
