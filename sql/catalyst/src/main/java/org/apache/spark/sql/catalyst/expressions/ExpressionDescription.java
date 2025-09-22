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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.annotation.DeveloperApi;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * ::DeveloperApi::
 *
 * A function description type which can be recognized by FunctionRegistry, and will be used to
 * show the usage of the function in human language.
 *
 * `usage()` will be used for the function usage in brief way.
 *
 * These below are concatenated and used for the function usage in verbose way, suppose arguments,
 * examples, note, group, source, since and deprecated will be provided.
 *
 * `arguments()` describes arguments for the expression.
 *
 * `examples()` describes examples for the expression.
 *
 * `note()` contains some notes for the expression optionally.
 *
 * `group()` describes the category that the expression belongs to. The valid value is
 * "agg_funcs", "array_funcs", "datetime_funcs", "json_funcs", "map_funcs" and "window_funcs".
 *
 * `source()` describe the source of the function. The valid value is "built-in", "hive",
 * "python_udf", "scala_udf", "java_udf".
 *
 * `since()` contains version information for the expression. Version is specified by,
 * for example, "2.2.0".
 *
 * `deprecated()` contains deprecation information for the expression optionally, for example,
 * "Deprecated since 2.2.0. Use something else instead".
 *
 * The format, in particular for `arguments()`, `examples()`,`note()`, `group()`, `source()`,
 * `since()` and `deprecated()`,  should strictly be as follows.
 *
 * <pre>
 * <code>@ExpressionDescription(
 *   ...
 *   arguments = """
 *     Arguments:
 *       * arg0 - ...
 *           ....
 *       * arg1 - ...
 *           ....
 *   """,
 *   examples = """
 *     Examples:
 *       > SELECT ...;
 *        ...
 *       > SELECT ...;
 *        ...
 *   """,
 *   note = """
 *     ...
 *   """,
 *   group = "agg_funcs",
 *   source = "built-in",
 *   since = "3.0.0",
 *   deprecated = """
 *     ...
 *   """)
 * </code>
 * </pre>
 *
 *  We can refer the function name by `_FUNC_`, in `usage()`, `arguments()`, `examples()` and
 *  `note()` as it is registered in `FunctionRegistry`.
 *
 *  Note that, if `extended()` is defined, `arguments()`, `examples()`, `note()`, `group()`,
 *  `since()` and `deprecated()` should be not defined together. `extended()` exists
 *  for backward compatibility.
 *
 *  Note this contents are used in the SparkSQL documentation for built-in functions. The contents
 *  here are considered as a Markdown text and then rendered.
 */
//用于描述 SQL 表达式（函数）的注解，主要用于函数注册和生成文档。它可以帮助函数注册系统（FunctionRegistry）识别函数，并生成该函数的使用文档
@DeveloperApi
@Retention(RetentionPolicy.RUNTIME)
public @interface ExpressionDescription {
    String usage() default ""; //提供函数的简要使用说明，通常是函数的目的或功能
    /**
     * @deprecated This field is deprecated as of Spark 3.0. Use {@link #arguments},
     *   {@link #examples}, {@link #note}, {@link #since} and {@link #deprecated} instead
     *   to document the extended usage.
     */
    @Deprecated
    String extended() default "";  //已经被弃用
    String arguments() default "";  //描述函数的参数。它提供关于每个参数的详细信息，比如每个参数的类型、意义以及约束条件
    String examples() default "";   //如何使用该函数的示例 SQL 查询。通常包括一些常见的用法，帮助用户理解如何将函数应用于实际查询中
    String note() default "";       //提供与该函数相关的额外说明或注意事项。这个字段是可选的，可以用于描述函数的特殊行为或限制
    /**
     * Valid group names are almost the same with one defined as `groupname` in
     * `sql/functions.scala`. But, `collection_funcs` is split into fine-grained three groups:
     * `array_funcs`, `map_funcs`, and `json_funcs`. See `ExpressionInfo` for the
     * detailed group names.
     */
    String group() default "";  //指定函数所属的类别。有效的类别包括 "agg_funcs"（聚合函数）、"array_funcs"（数组函数）、"datetime_funcs"（日期时间函数）、"json_funcs"（JSON 函数）、"map_funcs"（映射函数）和 "window_funcs"（窗口函数）
    String since() default "";  //指定函数在哪个版本的 Spark 中引入的。例如，since = "3.0.0" 表示这个函数是从 Spark 3.0.0 开始支持的
    String deprecated() default "";  //描述该函数的弃用信息，如果函数在某个版本中已经弃用，通常会提供替代函数的建议
    String source() default "built-in";  //指定函数的来源，表示该函数是内建函数、Hive 函数、Python UDF、Scala UDF 还是 Java UDF。默认值为 "built-in"，表示该函数是 Spark 内建函数
}
