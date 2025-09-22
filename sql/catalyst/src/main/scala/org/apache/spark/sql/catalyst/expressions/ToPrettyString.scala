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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/** 主要用于将不同类型的值转换为更“美观”的字符串表示。它与常规的类型转换为字符串的操作有所不同
 * An internal expressions which is used to generate pretty string for all kinds of values. It has
 * several differences with casting value to string:
 *  - It prints null values (either from column or struct field) as "NULL".  对于 null 值（无论是来自列还是结构字段），它会显示为 NULL
 *  - It prints binary values (either from column or struct field) using the hex format.  对于二进制值，它使用十六进制格式显示
 *///child: Expression 输入表达式，它代表需要转换为美观字符串的原始数据
case class ToPrettyString(child: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with ToStringBase {

  override def dataType: DataType = StringType  //输出的类型，固定为 StringType

  override def nullable: Boolean = false  //该表达式始终会返回非 null 的字符串
  //用于为当前的表达式设置时区 ID，生成一个新的 ToPrettyString 实例，其中 timeZoneId 设置为指定的时区 ID
  override def withTimeZone(timeZoneId: String): ToPrettyString =
    copy(timeZoneId = Some(timeZoneId))

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override protected def leftBracket: String = "{"
  override protected def rightBracket: String = "}"

  override protected def nullString: String = "NULL"

  override protected def useDecimalPlainString: Boolean = true

  override protected def useHexFormatForBinary: Boolean = true

  private[this] lazy val castFunc: Any => Any = castToString(child.dataType)
  //计算逻辑方法
  override def eval(input: InternalRow): Any = {
    val v = child.eval(input)
    if (v == null) UTF8String.fromString(nullString) else castFunc(v)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childCode = child.genCode(ctx)
    val toStringCode = castToStringCode(child.dataType, ctx).apply(childCode.value, ev.value)
    val finalCode =
      code"""
         |${childCode.code}
         |UTF8String ${ev.value};
         |if (${childCode.isNull}) {
         |  ${ev.value} = UTF8String.fromString("$nullString");
         |} else {
         |  $toStringCode
         |}
         |""".stripMargin
    ev.copy(code = finalCode, isNull = FalseLiteral)
  }
}
