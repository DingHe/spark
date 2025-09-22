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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Interface for a parser.
 */
//将 SQL 字符串解析成 Spark SQL 中的不同结构和对象
@DeveloperApi
trait ParserInterface extends DataTypeParserInterface {
  /**
   * Parse a string to a [[LogicalPlan]].
   */
  //将 SQL 字符串解析为一个 LogicalPlan。LogicalPlan 是 Spark SQL 中描述查询执行计划的抽象
  //用于解析任何类型的 SQL 文本，并将其转换为 LogicalPlan，不仅限于查询语句
  @throws[ParseException]("Text cannot be parsed to a LogicalPlan")
  def parsePlan(sqlText: String): LogicalPlan

  /**
   * Parse a string to an [[Expression]].
   */
  //将 SQL 字符串解析为一个 Expression。Expression 表示 SQL 中的表达式，可以是字段、操作符、函数等
  @throws[ParseException]("Text cannot be parsed to an Expression")
  def parseExpression(sqlText: String): Expression

  /**
   * Parse a string to a [[TableIdentifier]].
   */
  //将 SQL 字符串解析为一个 TableIdentifier。TableIdentifier 是 Spark SQL 中表示表名的标识符，它通常包括数据库名和表名
  @throws[ParseException]("Text cannot be parsed to a TableIdentifier")
  def parseTableIdentifier(sqlText: String): TableIdentifier

  /**
   * Parse a string to a [[FunctionIdentifier]].
   */
  //将 SQL 字符串解析为一个 FunctionIdentifier。FunctionIdentifier 是 Spark SQL 中表示函数标识符的对象
  @throws[ParseException]("Text cannot be parsed to a FunctionIdentifier")
  def parseFunctionIdentifier(sqlText: String): FunctionIdentifier

  /**
   * Parse a string to a multi-part identifier.
   */
  //将 SQL 字符串解析为一个多部分标识符（Seq[String]）。这个标识符通常表示数据库和表的组合，或者数据库、表、列的组合等
  @throws[ParseException]("Text cannot be parsed to a multi-part identifier")
  def parseMultipartIdentifier(sqlText: String): Seq[String]

  /**
   * Parse a query string to a [[LogicalPlan]].
   */
  //将 SQL 查询字符串解析为一个 LogicalPlan。LogicalPlan 描述了 SQL 查询的逻辑结构
  //专门用于解析 SQL 查询字符串（如 SELECT 语句等）并将其转换为 LogicalPlan
  @throws[ParseException]("Text cannot be parsed to a LogicalPlan")
  def parseQuery(sqlText: String): LogicalPlan
}
