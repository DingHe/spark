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

package org.apache.spark.sql.connector.expressions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.annotation.Evolving;

/**
 * Base class of the public logical expression API.
 *
 * @since 3.0.0
 */
// Expression 接口是 Spark DataSource V2 API 中所有逻辑表达式的基础抽象类
// 核心职责：
// 统一模型： 为 V2 数据源（如 Pushdown 过滤器、Transform 分区表达式、SortOrder 排序表达式等）提供一个统一的、公共的、可移植的表达式模型
// 引用追踪： 提供 references() 默认实现，能够递归地识别表达式所依赖的字段或列（即 NamedReference），这对执行查询优化（如列裁剪）至关重要
// 可读性描述： 提供 describe() 方法，用于生成人类可读的 SQL 样式的表达式字符串，便于调试和日志记录
@Evolving
public interface Expression {
  // 用于返回空子表达式数组的常量
  Expression[] EMPTY_EXPRESSION = new Expression[0];

  /**
   * `EMPTY_EXPRESSION` is only used as an input when the
   * default `references` method builds the result array to avoid
   * repeatedly allocating an empty array.
   */
  // 用于返回空命名引用数组的常量
  NamedReference[] EMPTY_NAMED_REFERENCE = new NamedReference[0];

  /**
   * Format the expression as a human readable SQL-like string.
   */
  // 返回可读性描述
  default String describe() { return this.toString(); }

  /**
   * Returns an array of the children of this node. Children should not change.
   */
  // 获取子节点
  // 要求实现类返回当前表达式的直接子表达式数组。例如，对于 Add(A, B) 表达式，它将返回包含 A 和 B 的数组
  Expression[] children();

  /**
   * List of fields or columns that are referenced by this expression.
   */
  // 它的默认实现是递归地遍历所有 children()，
  // 收集这些子表达式所引用的所有命名引用（即列或字段），并使用 HashSet 进行去重，最终返回一个去重后的 NamedReference 数组。它用于确定表达式依赖于哪些列
  default NamedReference[] references() {
    // SPARK-40398: Replace `Arrays.stream()...distinct()`
    // to this for perf gain, the result order is not important.
    Set<NamedReference> set = new HashSet<>();
    for (Expression e : children()) {
      Collections.addAll(set, e.references());
    }
    return set.toArray(EMPTY_NAMED_REFERENCE);
  }
}
