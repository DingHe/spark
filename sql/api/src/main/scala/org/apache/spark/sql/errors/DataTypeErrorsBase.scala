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
package org.apache.spark.sql.errors

import java.util.Locale

import org.apache.spark.QueryContext
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.catalyst.util.{AttributeNameParser, QuotingUtils}
import org.apache.spark.sql.types.{AbstractDataType, DataType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String
//将不同类型的数据转换为SQL语句中使用的格式。其目的是提供一组工具方法，将常见的数据类型、配置、查询等转换为符合SQL语法要求的格式
private[sql] trait DataTypeErrorsBase {
  //通过 AttributeNameParser.parseAttributeName 对输入的字符串进行解析，再调用 toSQLId 方法进行格式化
  def toSQLId(parts: String): String = {
    toSQLId(AttributeNameParser.parseAttributeName(parts))
  }
  //将一系列字符串（如数据库、表、列名等）转换为 SQL 格式的标识符。返回值是一个用 . 连接的标识符字符串
  def toSQLId(parts: Seq[String]): String = {
    val cleaned = parts match {
      case Seq("__auto_generated_subquery_name", rest @ _*) if rest != Nil => rest
      case other => other
    }
    cleaned.map(QuotingUtils.quoteIdentifier).mkString(".")
  }
  //将输入的 SQL 语句转换为大写格式
  def toSQLStmt(text: String): String = {
    text.toUpperCase(Locale.ROOT)
  }
  //将 SQL 配置项（如 SQLConf）转换为 SQL 格式的字符串
  def toSQLConf(conf: String): String = {
    QuotingUtils.toSQLConf(conf)
  }
  //将输入的类型名称转换为 SQL 格式的类型名称，并将其转换为大写
  def toSQLType(text: String): String = {
    quoteByDefault(text.toUpperCase(Locale.ROOT))
  }
  //将一个数据类型（AbstractDataType）转换为 SQL 格式的类型名称
  def toSQLType(t: AbstractDataType): String = t match {
    case TypeCollection(types) => types.map(toSQLType).mkString("(", " or ", ")")
    case dt: DataType => quoteByDefault(dt.sql)
    case at => quoteByDefault(at.simpleString.toUpperCase(Locale.ROOT))
  }
  //如果值为 null，则返回 SQL 中的 NULL。否则，值将被转义（处理 \ 和 ' 字符），并用单引号括起来
  def toSQLValue(value: String): String = {
    if (value == null) {
      "NULL"
    } else {
      "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    }
  }

  def toSQLValue(value: UTF8String): String = toSQLValue(value.toString)

  def toSQLValue(value: Short): String = String.valueOf(value) + "S"

  def toSQLValue(value: Int): String = String.valueOf(value)

  def toSQLValue(value: Long): String = String.valueOf(value) + "L"
  //将 Float 类型的值转换为 SQL 格式的值
  def toSQLValue(value: Float): String = {
    if (value.isNaN) "NaN"
    else if (value.isPosInfinity) "Infinity"
    else if (value.isNegInfinity) "-Infinity"
    else value.toString
  }

  def toSQLValue(value: Double): String = {
    if (value.isNaN) "NaN"
    else if (value.isPosInfinity) "Infinity"
    else if (value.isNegInfinity) "-Infinity"
    else value.toString
  }

  protected def quoteByDefault(elem: String): String = {
    "\"" + elem + "\""
  }

    def getSummary(sqlContext: SQLQueryContext): String = {
    if (sqlContext == null) "" else sqlContext.summary
  }

  def getQueryContext(sqlContext: SQLQueryContext): Array[QueryContext] = {
    if (sqlContext == null) Array.empty else Array(sqlContext.asInstanceOf[QueryContext])
  }
}
