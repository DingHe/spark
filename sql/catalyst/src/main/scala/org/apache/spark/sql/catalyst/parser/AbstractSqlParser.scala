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

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.errors.QueryParsingErrors

/**
 * Base class for all ANTLR4 [[ParserInterface]] implementations.
 */
//用于定义 SQL 解析的基础功能。它实现了 ParserInterface 接口，并扩展了 AbstractParser 类。
// 该类为 SQL 解析提供了通用的方法来解析不同类型的 SQL 语法元素，并将其转换为相应的 Spark Catalyst 表达式或逻辑计划
abstract class AbstractSqlParser extends AbstractParser with ParserInterface {
  //代表了 SQL 解析过程中用于构建 AST（抽象语法树）的工具。AstBuilder 是一个访问者模式的实现，它负责将解析树转换为实际的表达式和计划
  override def astBuilder: AstBuilder

  /** Creates Expression for a given SQL string. */
    //该方法解析 SQL 字符串中的表达式部分（如 SELECT 1 + 1 中的 1 + 1）并返回一个 Expression 对象
  override def parseExpression(sqlText: String): Expression = parse(sqlText) { parser =>
    val ctx = parser.singleExpression()
    withOrigin(ctx, Some(sqlText)) {
      astBuilder.visitSingleExpression(ctx)
    }
  }

  /** Creates TableIdentifier for a given SQL string. */
    //该方法解析 SQL 字符串中的表标识符（如 my_database.my_table）并返回一个 TableIdentifier 对象
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parse(sqlText) { parser =>
    astBuilder.visitSingleTableIdentifier(parser.singleTableIdentifier())
  }

  /** Creates FunctionIdentifier for a given SQL string. */
    //该方法解析 SQL 字符串中的函数标识符（如 SUM 或 my_function）并返回一个 FunctionIdentifier 对象
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    parse(sqlText) { parser =>
      astBuilder.visitSingleFunctionIdentifier(parser.singleFunctionIdentifier())
    }
  }

  /** Creates a multi-part identifier for a given SQL string */
    //该方法解析 SQL 字符串中的多部分标识符（如 my_database.my_table.my_column），并返回一个 Seq[String]
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    parse(sqlText) { parser =>
      astBuilder.visitSingleMultipartIdentifier(parser.singleMultipartIdentifier())
    }
  }

  /** Creates LogicalPlan for a given SQL string of query. */
    //该方法解析 SQL 字符串中的查询语句（如 SELECT * FROM my_table）并返回一个 LogicalPlan 对象
  override def parseQuery(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    val ctx = parser.query()
    withOrigin(ctx, Some(sqlText)) {
      astBuilder.visitQuery(ctx)
    }
  }

  /** Creates LogicalPlan for a given SQL string. */
    //该方法解析 SQL 字符串中的整个 SQL 语句，并返回一个 LogicalPlan 对象
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    val ctx = parser.singleStatement()
    withOrigin(ctx, Some(sqlText)) {
      astBuilder.visitSingleStatement(ctx) match {
        case plan: LogicalPlan => plan
        case _ =>
          val position = Origin(None, None)
          throw QueryParsingErrors.sqlStatementUnsupportedError(sqlText, position)
      }
    }
  }
}
