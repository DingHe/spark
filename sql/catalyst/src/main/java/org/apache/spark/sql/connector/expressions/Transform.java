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

import org.apache.spark.annotation.Evolving;

/**
 * Represents a transform function in the public logical expression API.
 * <p>
 * For example, the transform date(ts) is used to derive a date value from a timestamp column. The
 * transform name is "date" and its argument is a reference to the "ts" column.
 *
 * @since 3.0.0
 */
// Transform 接口是 Spark DataSource V2 API 中用于抽象分区转换函数（Partition Transformation Functions）的专用表达式类型
// 分区抽象： 它代表了一种对一个或多个输入列（参数）应用转换函数（如 years、months、bucket、truncate 等）的操作。在 Spark SQL 中，Transform 主要用于定义表的分区规范（Partitioning）
// Transform 接口定义了如何根据表的原始数据列派生出分区列的逻辑。
@Evolving
public interface Transform extends Expression {
  /**
   * Returns the transform function name.
   */
  // 转换函数的名称。
  // 返回该转换操作的函数名称，例如 "years"（年份分区）、"months"（月份分区）、"bucket"（桶化）或 "truncate"（截断）
  String name();

  /**
   * Returns the arguments passed to the transform function.
   */
  // 转换函数的参数。
  // 返回传递给该转换函数的一组输入参数（即 Expression 数组）。
  // 这些参数通常是 FieldReference（字段引用），指向要进行转换的表的原始列
  Expression[] arguments();
  // 获取子表达式（默认实现）
  // 重写了父接口 Expression 中的 children() 方法。
  // 它直接返回 arguments() 方法的结果。这意味着对于 Transform 表达式而言，其参数即被视为其子表达式
  @Override
  default Expression[] children() { return arguments(); }
}
