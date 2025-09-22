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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators._
//表示排序操作中的排序方向（升序或降序）以及空值的排序规则
abstract sealed class SortDirection {
  def sql: String //表示排序方向的 SQL 字符串形式（如 "ASC" 或 "DESC"）
  def defaultNullOrdering: NullOrdering //表示默认的空值排序规则（空值先还是后）
}

abstract sealed class NullOrdering {
  def sql: String
}
//表示升序排序
case object Ascending extends SortDirection {
  override def sql: String = "ASC" //升序的 SQL 表达方式
  override def defaultNullOrdering: NullOrdering = NullsFirst
}
//表示降序排序
case object Descending extends SortDirection {
  override def sql: String = "DESC" //降序的 SQL 表达方式
  override def defaultNullOrdering: NullOrdering = NullsLast
}

case object NullsFirst extends NullOrdering {
  override def sql: String = "NULLS FIRST"
}

case object NullsLast extends NullOrdering {
  override def sql: String = "NULLS LAST"
}

/**
 * An expression that can be used to sort a tuple.  This class extends expression primarily so that
 * transformations over expression will descend into its child.
 * `sameOrderExpressions` is a set of expressions with the same sort order as the child. It is
 * derived from equivalence relation in an operator, e.g. left/right keys of an inner sort merge
 * join. 表示排序操作的表达式，它通常会包含一个子表达式 child 和对应的排序方向 direction。在 Spark SQL 中，排序操作是通过 SortOrder 类进行描述的
 */
case class SortOrder(
    child: Expression, //需要排序的表达式（例如列名、字段等）
    direction: SortDirection,  //排序的方向，可以是升序 (Ascending) 或降序 (Descending)
    nullOrdering: NullOrdering, //空值的排序方式
    sameOrderExpressions: Seq[Expression]) //具有相同排序规则的其他表达式，用于描述相同排序方向的多个字段
  extends Expression with Unevaluable {

  override def children: Seq[Expression] = child +: sameOrderExpressions //返回排序的所有子表达式

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(dataType, prettyName)

  override def dataType: DataType = child.dataType //返回排序字段的类型（即 child 的类型）
  override def nullable: Boolean = child.nullable //表示排序字段是否可以为空

  override def toString: String = s"$child ${direction.sql} ${nullOrdering.sql}" //返回排序表达式的字符串表示形式
  override def sql: String = child.sql + " " + direction.sql + " " + nullOrdering.sql

  def isAscending: Boolean = direction == Ascending //判断排序方向是否为升序

  def satisfies(required: SortOrder): Boolean = {  //检查当前 SortOrder 是否满足给定的排序要求（例如，比较字段和排序方向）
    children.exists(required.child.semanticEquals) &&
      direction == required.direction && nullOrdering == required.nullOrdering
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): SortOrder =
    copy(child = newChildren.head, sameOrderExpressions = newChildren.tail) //返回一个新的 SortOrder，用于替换子表达式
}

object SortOrder {
  def apply(
     child: Expression,
     direction: SortDirection,
     sameOrderExpressions: Seq[Expression] = Seq.empty): SortOrder = {
    new SortOrder(child, direction, direction.defaultNullOrdering, sameOrderExpressions)
  }

  /**
   * Returns if a sequence of SortOrder satisfies another sequence of SortOrder.
   *
   * SortOrder sequence A satisfies SortOrder sequence B if and only if B is an equivalent of A
   * or of A's prefix. Here are examples of ordering A satisfying ordering B:
   * <ul>
   *   <li>ordering A is [x, y] and ordering B is [x]</li>
   *   <li>ordering A is [x(sameOrderExpressions=x1)] and ordering B is [x1]</li>
   *   <li>ordering A is [x(sameOrderExpressions=x1), y] and ordering B is [x1]</li>
   * </ul>  判断一个排序序列（ordering1）是否满足另一个排序序列（ordering2）。排序序列满足的条件是，ordering2 是 ordering1 的前缀，或者等于 ordering1
   */
  def orderingSatisfies(ordering1: Seq[SortOrder], ordering2: Seq[SortOrder]): Boolean = {
    if (ordering2.isEmpty) {
      true
    } else if (ordering2.length > ordering1.length) {
      false
    } else {
      ordering2.zip(ordering1).forall {
        case (o2, o1) => o1.satisfies(o2)
      }
    }
  }
}

/** 用于生成排序前缀的表达式，它将用于排序操作中
 * An expression to generate a 64-bit long prefix used in sorting. If the sort must operate over
 * null keys as well, this.nullValue can be used in place of emitted null prefixes in the sort.
 */
case class SortPrefix(child: SortOrder) extends UnaryExpression {
  //根据排序规则，决定空值应排在前面还是后面，生成对应的空值前缀
  val nullValue = child.child.dataType match {
    case BooleanType | DateType | TimestampType | TimestampNTZType |
         _: IntegralType | _: AnsiIntervalType =>
      if (nullAsSmallest) Long.MinValue else Long.MaxValue
    case dt: DecimalType if dt.precision - dt.scale <= Decimal.MAX_LONG_DIGITS =>
      if (nullAsSmallest) Long.MinValue else Long.MaxValue
    case _: DecimalType =>
      if (nullAsSmallest) {
        DoublePrefixComparator.computePrefix(Double.NegativeInfinity)
      } else {
        DoublePrefixComparator.computePrefix(Double.NaN)
      }
    case _ =>
      if (nullAsSmallest) 0L else -1L
  }

  private def nullAsSmallest: Boolean = {
    (child.isAscending && child.nullOrdering == NullsFirst) ||
      (!child.isAscending && child.nullOrdering == NullsLast)
  }
  //根据排序字段的类型，生成一个适合的前缀计算方法。不同的数据类型（如布尔值、日期、数字、字符串等）会有不同的前缀计算逻辑
  private lazy val calcPrefix: Any => Long = child.child.dataType match {
    case BooleanType => (raw) =>
      if (raw.asInstanceOf[Boolean]) 1 else 0   //布尔值使用 0 或 1 作为前缀
    case DateType | TimestampType | TimestampNTZType |
         _: IntegralType | _: AnsiIntervalType => (raw) =>
      raw.asInstanceOf[java.lang.Number].longValue()  //数字类型（如整型、浮动类型）会直接转换为 Long 类型
    case FloatType | DoubleType => (raw) => {
      val dVal = raw.asInstanceOf[java.lang.Number].doubleValue()
      DoublePrefixComparator.computePrefix(dVal)
    }
    case StringType => (raw) =>
      StringPrefixComparator.computePrefix(raw.asInstanceOf[UTF8String])  //字符串类型会使用 StringPrefixComparator 来计算前缀
    case BinaryType => (raw) =>
      BinaryPrefixComparator.computePrefix(raw.asInstanceOf[Array[Byte]])
    case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS =>
      _.asInstanceOf[Decimal].toUnscaledLong
    case dt: DecimalType if dt.precision - dt.scale <= Decimal.MAX_LONG_DIGITS =>
      val p = Decimal.MAX_LONG_DIGITS
      val s = p - (dt.precision - dt.scale)
      (raw) => {
        val value = raw.asInstanceOf[Decimal]
        if (value.changePrecision(p, s)) {
          value.toUnscaledLong
        } else if (value.toBigDecimal.signum < 0) {
          Long.MinValue
        } else {
          Long.MaxValue
        }
      }
    case dt: DecimalType => (raw) =>
      DoublePrefixComparator.computePrefix(raw.asInstanceOf[Decimal].toDouble)
    case _ => (Any) => 0L
  }

  override def eval(input: InternalRow): Any = {
    val value = child.child.eval(input)
    if (value == null) {
      null
    } else {
      calcPrefix(value)
    }
  }
  //生成对应的代码片段，用于优化代码生成。在 Spark SQL 中，代码生成是优化查询性能的一种方式
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childCode = child.child.genCode(ctx)
    val input = childCode.value
    val BinaryPrefixCmp = classOf[BinaryPrefixComparator].getName
    val DoublePrefixCmp = classOf[DoublePrefixComparator].getName
    val StringPrefixCmp = classOf[StringPrefixComparator].getName
    val prefixCode = child.child.dataType match {
      case BooleanType =>
        s"$input ? 1L : 0L"
      case _: IntegralType =>
        s"(long) $input"
      case DateType | TimestampType | TimestampNTZType | _: AnsiIntervalType =>
        s"(long) $input"
      case FloatType | DoubleType =>
        s"$DoublePrefixCmp.computePrefix((double)$input)"
      case StringType => s"$StringPrefixCmp.computePrefix($input)"
      case BinaryType => s"$BinaryPrefixCmp.computePrefix($input)"
      case dt: DecimalType if dt.precision < Decimal.MAX_LONG_DIGITS =>
        s"$input.toUnscaledLong()"
      case dt: DecimalType if dt.precision - dt.scale <= Decimal.MAX_LONG_DIGITS =>
        // reduce the scale to fit in a long
        val p = Decimal.MAX_LONG_DIGITS
        val s = p - (dt.precision - dt.scale)
        s"$input.changePrecision($p, $s) ? $input.toUnscaledLong() : " +
            s"$input.toBigDecimal().signum() < 0 ? ${Long.MinValue}L : ${Long.MaxValue}L"
      case dt: DecimalType =>
        s"$DoublePrefixCmp.computePrefix($input.toDouble())"
      case _ => "0L"
    }

    ev.copy(code = childCode.code +
      code"""
         |long ${ev.value} = 0L;
         |boolean ${ev.isNull} = ${childCode.isNull};
         |if (!${childCode.isNull}) {
         |  ${ev.value} = $prefixCode;
         |}
      """.stripMargin)
  }

  override def dataType: DataType = LongType

  override protected def withNewChildInternal(newChild: Expression): SortPrefix =
    copy(child = newChild.asInstanceOf[SortOrder])
}
