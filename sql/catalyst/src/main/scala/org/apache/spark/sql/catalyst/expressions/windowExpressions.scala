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

import java.util.Locale

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedException}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, DeclarativeAggregate, NoOp}
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, TernaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, UNRESOLVED_WINDOW_EXPRESSION, WINDOW_EXPRESSION}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.types._

/**
 * The trait of the Window Specification (specified in the OVER clause or WINDOW clause) for
 * Window Functions.
 */
//定义窗口函数的规范接口，指定如何对数据进行分区、排序以及窗口框架等操作
sealed trait WindowSpec

/**
 * The specification for a window function.
 *
 * @param partitionSpec It defines the way that input rows are partitioned.
 * @param orderSpec It defines the ordering of rows in a partition.
 * @param frameSpecification It defines the window frame in a partition.
 */
//
case class WindowSpecDefinition(
    partitionSpec: Seq[Expression],  //输入行的分区方式。每个分区会在窗口函数计算时作为独立的组来处理
    orderSpec: Seq[SortOrder],  //定义窗口中的行如何排序。排序是在每个分区内进行的
    frameSpecification: WindowFrame) //定义窗口框架，描述在每个分区内，窗口函数如何处理数据的范围。窗口框架通常包括 ROWS 或 RANGE 等类型，指明窗口的起始和结束位置
  extends Expression with WindowSpec with Unevaluable {

  override def children: Seq[Expression] = partitionSpec ++ orderSpec :+ frameSpecification

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): WindowSpecDefinition =
    copy(
      partitionSpec = newChildren.take(partitionSpec.size),
      orderSpec = newChildren.drop(partitionSpec.size).dropRight(1).asInstanceOf[Seq[SortOrder]],
      frameSpecification = newChildren.last.asInstanceOf[WindowFrame])

  override lazy val resolved: Boolean =
    childrenResolved && frameSpecification.isInstanceOf[SpecifiedWindowFrame] &&
      checkInputDataTypes().isSuccess

  override def nullable: Boolean = true
  override def dataType: DataType = throw QueryCompilationErrors.dataTypeOperationUnsupportedError

  override def checkInputDataTypes(): TypeCheckResult = {
    frameSpecification match {
      case UnspecifiedFrame =>
        throw SparkException.internalError("Cannot use an UnspecifiedFrame. " +
          "This should have been converted during analysis.")
      case f: SpecifiedWindowFrame if f.frameType == RangeFrame && !f.isUnbounded &&
          orderSpec.isEmpty =>
        DataTypeMismatch(errorSubClass = "RANGE_FRAME_WITHOUT_ORDER")
      case f: SpecifiedWindowFrame if f.frameType == RangeFrame && f.isValueBound &&
          orderSpec.size > 1 =>
        DataTypeMismatch(
          errorSubClass = "RANGE_FRAME_MULTI_ORDER",
          messageParameters = Map(
            "orderSpec" -> orderSpec.mkString(",")
          )
        )
      case f: SpecifiedWindowFrame if f.frameType == RangeFrame && f.isValueBound &&
          !isValidFrameType(f.valueBoundary.head.dataType) =>
        DataTypeMismatch(
          errorSubClass = "RANGE_FRAME_INVALID_TYPE",
          messageParameters = Map(
            "orderSpecType" -> toSQLType(orderSpec.head.dataType),
            "valueBoundaryType" -> toSQLType(f.valueBoundary.head.dataType)
          )
        )
      case _ => TypeCheckSuccess
    }
  }
  //生成对应的 SQL 表达式。它将 partitionSpec、orderSpec 和 frameSpecification 转换为 SQL 语句的一部分，最终构造一个完整的窗口规范的 SQL 字符串
  override def sql: String = {
    def toSql(exprs: Seq[Expression], prefix: String): Seq[String] = {
      Seq(exprs).filter(_.nonEmpty).map(_.map(_.sql).mkString(prefix, ", ", ""))
    }

    val elements =
      toSql(partitionSpec, "PARTITION BY ") ++
        toSql(orderSpec, "ORDER BY ") ++
        Seq(frameSpecification.sql)
    elements.mkString("(", " ", ")")
  }

  private def isValidFrameType(ft: DataType): Boolean = (orderSpec.head.dataType, ft) match {
    case (DateType, IntegerType) => true
    case (DateType, _: YearMonthIntervalType) => true
    case (TimestampType | TimestampNTZType, CalendarIntervalType) => true
    case (TimestampType | TimestampNTZType, _: YearMonthIntervalType) => true
    case (TimestampType | TimestampNTZType, _: DayTimeIntervalType) => true
    case (a, b) => a == b
  }
}

/**
 * A Window specification reference that refers to the [[WindowSpecDefinition]] defined
 * under the name `name`.
 */
case class WindowSpecReference(name: String) extends WindowSpec

/**
 * The trait used to represent the type of a Window Frame.
 */
//表示窗口帧的类型。sealed 关键字意味着这个特质只能被当前文件中的类或对象所继承，防止外部扩展
sealed trait FrameType {
  def inputType: AbstractDataType   //返回帧的输入类型。它定义了该帧类型所需的输入数据的类型
  def sql: String                   //表示窗口帧的 SQL 表达形式。例如，对于 RowFrame，SQL 表达式可能是 "ROWS"
}

/**
 * RowFrame treats rows in a partition individually. Values used in a row frame are considered
 * to be physical offsets.
 * For example, `ROW BETWEEN 1 PRECEDING AND 1 FOLLOWING` represents a 3-row frame,
 * from the row that precedes the current row to the row that follows the current row.
 */
//表示 行帧，它在一个分区内将每一行当作独立的单位处理，具体来说，它使用物理偏移来确定帧的范围
case object RowFrame extends FrameType {
  override def inputType: AbstractDataType = IntegerType  //表示在定义行帧时，使用整数类型作为偏移量
  override def sql: String = "ROWS"     //RowFrame 对应的 SQL 表达式是 "ROWS"
}

/**
 * RangeFrame treats rows in a partition as groups of peers. All rows having the same `ORDER BY`
 * ordering are considered as peers. Values used in a range frame are considered to be logical
 * offsets.
 * For example, assuming the value of the current row's `ORDER BY` expression `expr` is `v`,
 * `RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING` represents a frame containing rows whose values
 * `expr` are in the range of [v-1, v+1].
 * 如果没有order by 语句，则包含所有的行
 * If `ORDER BY` clause is not defined, all rows in the partition are considered as peers
 * of the current row.
 */
// 表示 范围帧，它将分区内的行视为具有相同 ORDER BY 排序值的“同行”集合。
// 范围帧使用逻辑偏移来定义帧的范围，即与当前行的 ORDER BY 值相关的范围
case object RangeFrame extends FrameType {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval  //表示范围帧的输入可以是数值型或时间间隔型的数据
  override def sql: String = "RANGE"
}

/**
 * The trait used to represent special boundaries used in a window frame.
 */
//sealed trait，表示窗口帧中的特殊边界
sealed trait SpecialFrameBoundary extends LeafExpression with Unevaluable {
  override def dataType: DataType = NullType  //这些边界本身没有具体的数据类型，因为它们只起到标识作用
  override def nullable: Boolean = false   //所有的特殊边界都不可为空
}

/** UNBOUNDED boundary. */
//表示窗口帧的 无界前 边界，即帧从分区的起始位置开始，不限制帧的前端
case object UnboundedPreceding extends SpecialFrameBoundary {
  override def sql: String = "UNBOUNDED PRECEDING"
}
//表示窗口帧的 无界后 边界，即帧扩展到分区的末尾，不限制帧的后端。这个边界通常用于窗口函数中的 窗口帧结束位置，意味着窗口帧从当前行延伸至分区的最后一行
case object UnboundedFollowing extends SpecialFrameBoundary {
  override def sql: String = "UNBOUNDED FOLLOWING"
}

/** CURRENT ROW boundary. */
//表示窗口帧的 当前行 边界，意味着窗口帧从当前行开始或结束
case object CurrentRow extends SpecialFrameBoundary {
  override def sql: String = "CURRENT ROW"
}

/**
 * Represents a window frame.
 */
sealed trait WindowFrame extends Expression with Unevaluable {
  override def dataType: DataType = throw QueryCompilationErrors.dataTypeOperationUnsupportedError
  override def nullable: Boolean = false  //窗口帧不能返回 null
}

/** Used as a placeholder when a frame specification is not defined. */
//表示一个 未指定的窗口帧
case object UnspecifiedFrame extends WindowFrame with LeafLike[Expression]

/**
 * A specified Window Frame. The val lower/upper can be either a foldable [[Expression]] or a
 * [[SpecialFrameBoundary]].
 */
 //表示一个 指定的窗口帧，包含了具体的窗口范围
case class SpecifiedWindowFrame(
    frameType: FrameType,  //指定窗口帧的类型（如 RowFrame 或 RangeFrame），它表示如何定义窗口的边界（基于行或基于值）
    lower: Expression,  //分别表示窗口帧的下边界和上边界，可以是 Expression 类型的表达式，或者是特殊的边界（如 UnboundedPreceding 或 UnboundedFollowing）。这些边界定义了窗口帧的范围
    upper: Expression)
  extends WindowFrame with BinaryLike[Expression] {
  //分别代表窗口帧的下边界和上边界
  override def left: Expression = lower
  override def right: Expression = upper
  //获取 lower 和 upper 中不属于特殊边界（如 UnboundedPreceding 或 UnboundedFollowing）的部分。用于提取实际的计算边界
  lazy val valueBoundary: Seq[Expression] =
    children.filterNot(_.isInstanceOf[SpecialFrameBoundary])
  //用于检查窗口帧中上下边界表达式的数据类型是否合法
  override def checkInputDataTypes(): TypeCheckResult = {
    // Check lower value.
    //检查 lower 边界的表达式数据类型是否合法
    val lowerCheck = checkBoundary(lower, "lower")
    if (lowerCheck.isFailure) {
      return lowerCheck
    }

    // Check upper value.
    val upperCheck = checkBoundary(upper, "upper")
    if (upperCheck.isFailure) {
      return upperCheck
    }

    // Check combination (of expressions).
    //是检查 lower 和 upper 边界表达式的组合是否有效
    (lower, upper) match {
      case (l: Expression, u: Expression) if !isValidFrameBoundary(l, u) =>
        DataTypeMismatch(
          errorSubClass = "SPECIFIED_WINDOW_FRAME_INVALID_BOUND",
          messageParameters = Map(
            "upper" -> toSQLExpr(upper),
            "lower" -> toSQLExpr(lower)
          )
        )
      case (l: SpecialFrameBoundary, _) => TypeCheckSuccess
      case (_, u: SpecialFrameBoundary) => TypeCheckSuccess
      case (l: Expression, u: Expression) if l.dataType != u.dataType =>
        DataTypeMismatch(
          errorSubClass = "SPECIFIED_WINDOW_FRAME_DIFF_TYPES",
          messageParameters = Map(
            "lower" -> toSQLExpr(lower),
            "upper" -> toSQLExpr(upper),
            "lowerType" -> toSQLType(l.dataType),
            "upperType" -> toSQLType(u.dataType)
          )
        )
      case (l: Expression, u: Expression) if isGreaterThan(l, u) =>
        DataTypeMismatch(
          errorSubClass = "SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON",
          messageParameters = Map(
            "comparison" -> "less than or equal"
          )
        )
      case _ => TypeCheckSuccess
    }
  }

  override def sql: String = {
    val lowerSql = boundarySql(lower)
    val upperSql = boundarySql(upper)
    s"${frameType.sql} BETWEEN $lowerSql AND $upperSql"
  }

  def isUnbounded: Boolean = lower == UnboundedPreceding && upper == UnboundedFollowing

  def isValueBound: Boolean = valueBoundary.nonEmpty

  def isOffset: Boolean = (lower, upper) match {
    case (l: Expression, u: Expression) => frameType == RowFrame && l == u
    case _ => false
  }

  private def boundarySql(expr: Expression): String = expr match {
    case e: SpecialFrameBoundary => e.sql
    case UnaryMinus(n, _) => n.sql + " PRECEDING"
    case e: Expression => e.sql + " FOLLOWING"
  }

  // Check whether the left boundary value is greater than the right boundary value. It's required
  // that the both expressions have the same data type.
  // Since CalendarIntervalType is not comparable, we only compare expressions that are AtomicType.
  private def isGreaterThan(l: Expression, r: Expression): Boolean = l.dataType match {
    case _: AtomicType => GreaterThan(l, r).eval().asInstanceOf[Boolean]
    case _ => false
  }
  //验证窗口帧的边界表达式（b）是否合法
  //b：代表窗口帧的边界，类型是 Expression，即窗口帧的上下边界可以是任意的 SQL 表达式
  //location：表示当前正在检查的边界的位置（如 "lower" 或 "upper"），用于在错误消息中提供更多上下文信息
  private def checkBoundary(b: Expression, location: String): TypeCheckResult = b match {
    case _: SpecialFrameBoundary => TypeCheckSuccess  //如果边界是一个特殊的边界，直接返回 TypeCheckSuccess，表示检查通过
    case e: Expression if !e.foldable => //表达式 不可折叠（即无法在编译时计算出值，foldable 为 false），则返回一个 DataTypeMismatch 错误
      DataTypeMismatch(
        errorSubClass = "SPECIFIED_WINDOW_FRAME_WITHOUT_FOLDABLE",
        messageParameters = Map(
          "location" -> location,
          "expression" -> toSQLExpr(e)
        )
      )
    case e: Expression if !frameType.inputType.acceptsType(e.dataType) =>
      DataTypeMismatch(
        errorSubClass = "SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE",
        messageParameters = Map(
          "location" -> location,
          "exprType" -> toSQLType(e.dataType),
          "expectedType" -> toSQLType(frameType.inputType)
        )
      )
    case _ => TypeCheckSuccess
  }

  private def isValidFrameBoundary(l: Expression, u: Expression): Boolean = {
    (l, u) match {
      case (UnboundedFollowing, _) => false
      case (_, UnboundedPreceding) => false
      case _ => true
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): SpecifiedWindowFrame =
    copy(lower = newLeft, upper = newRight)
}

case class UnresolvedWindowExpression(
    child: Expression,
    windowSpec: WindowSpecReference) extends UnaryExpression with Unevaluable {

  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false

  override protected def withNewChildInternal(newChild: Expression): UnresolvedWindowExpression =
    copy(child = newChild)

  override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_WINDOW_EXPRESSION)
}

object WindowExpression {
  def hasWindowExpression(e: Expression): Boolean = {
    e.find(_.isInstanceOf[WindowExpression]).isDefined
  }
}
//Spark 中用于表示窗口函数表达式的类，通常用于 SQL 查询中窗口函数的计算
//将窗口函数和窗口规范（WindowSpecDefinition）结合起来，形成完整的窗口表达式
case class WindowExpression(
    windowFunction: Expression,//窗口函数本身的表达式。窗口函数是操作数据的函数，通常是聚合函数、排名函数、分析函数等。例如，ROW_NUMBER()、RANK()、SUM() 等
    windowSpec: WindowSpecDefinition) //窗口规范，定义了如何对数据进行分区、排序以及如何确定窗口的范围
  extends Expression with Unevaluable
  with BinaryLike[Expression] {

  override def left: Expression = windowFunction
  override def right: Expression = windowSpec

  override def dataType: DataType = windowFunction.dataType //窗口函数的返回数据类型
  override def nullable: Boolean = windowFunction.nullable

  override def toString: String = s"$windowFunction $windowSpec"
  override def sql: String = windowFunction.sql + " OVER " + windowSpec.sql

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): WindowExpression =
    copy(windowFunction = newLeft, windowSpec = newRight.asInstanceOf[WindowSpecDefinition])

  override val nodePatterns: Seq[TreePattern] = Seq(WINDOW_EXPRESSION)
}

/**
 * A window function is a function that can only be evaluated in the context of a window operator.
 */
//窗口函数
trait WindowFunction extends Expression {
  /** Frame in which the window operator must be executed. */
  def frame: WindowFrame = UnspecifiedFrame
}

/**
 * Case objects that describe whether a window function is a SQL window function or a Python
 * user-defined window function.
 */
sealed trait WindowFunctionType

object WindowFunctionType {
  case object SQL extends WindowFunctionType
  case object Python extends WindowFunctionType

  def functionType(windowExpression: NamedExpression): WindowFunctionType = {
    val t = windowExpression.collectFirst {
      case udf: PythonFuncExpression if PythonUDF.isWindowPandasUDF(udf) => Python
      // We should match `AggregateFunction` after the python function, as `PythonUDAF` extends
      // `AggregateFunction` but the function type should be Python.
      case _: WindowFunction | _: AggregateFunction => SQL
    }

    // Normally a window expression would either have a SQL window function, a SQL
    // aggregate function or a python window UDF. However, sometimes the optimizer will replace
    // the window function if the value of the window function can be predetermined.
    // For example, for query:
    //
    // select count(NULL) over () from values 1.0, 2.0, 3.0 T(a)
    //
    // The window function will be replaced by expression literal(0)
    // To handle this case, if a window expression doesn't have a regular window function, we
    // consider its type to be SQL as literal(0) is also a SQL expression.
    t.getOrElse(SQL)
  }
}
//定义了一个带有偏移量的窗口函数，通常用于像 LEAD 或 LAG 等窗口函数，它们允许访问当前行之前或之后的行数据
trait OffsetWindowFunction extends WindowFunction {
  /**
   * Input expression to evaluate against a row which a number of rows below or above (depending on
   * the value and sign of the offset) the starting row (current row if isRelative=true, or the
   * first row of the window frame otherwise).
   */
  //输入表达式，它表示对一个行进行评估的表达式。这个输入表达式的计算是基于当前行和由 offset 指定的偏移位置的行
  //如果 isRelative=true，表示偏移是相对于当前行的；如果 isRelative=false，则表示偏移是相对于窗口帧的第一行
  val input: Expression

  /**
   * (Foldable) expression that contains the number of rows between the current row and the row
   * where the input expression is evaluated. If `offset` is a positive integer, it means that
   * the direction of the `offset` is from front to back. If it is a negative integer, the direction
   * of the `offset` is from back to front. If it is zero, it means that the offset is ignored and
   * use current row.
   */
  //表示当前行与目标行之间的偏移量。这个偏移量的值可以是正整数、负整数或零
  //正整数：表示从前向后移动行
  //负整数：表示从后向前移动行
  //零：表示忽略偏移量，使用当前行的数据
  val offset: Expression

  /**
   * Default result value for the function when the `offset`th row does not exist.
   */
  //当偏移行不存在时（例如，当前行之前没有足够的行数，或者当前行之后没有足够的行数），该表达式用于返回默认值。它定义了窗口函数在没有找到目标行时的行为
  val default: Expression

  /**
   * An optional specification that indicates the offset window function should skip null values in
   * the determination of which row to use.
   */
  //指示窗口函数是否应跳过 null 值。它用于决定在计算偏移时是否忽略 null 值的行。
  // 这个属性通常用于优化 LEAD、LAG 和 NthValue 函数的性能，避免对 null 值的行进行偏移
  val ignoreNulls: Boolean

  /**
   * A fake window frame which is used to hold the offset information. It's used as a key to group
   * by offset window functions in `WindowExecBase.windowFrameExpressionFactoryPairs`, as offset
   * window functions with the same offset and same window frame can be evaluated together.
   */
    // 虚拟窗口帧，用于存储偏移信息。它的作用是为带有偏移量的窗口函数提供一个窗口帧
    //将具有相同偏移量和相同窗口帧配置的偏移窗口函数分组处理。换句话说，具有相同偏移量的窗口函数可以一起计算
  lazy val fakeFrame = SpecifiedWindowFrame(RowFrame, offset, offset)
}

/**
 * A frameless offset window function is a window function that cannot specify window frame and
 * returns the value of the input column offset by a number of rows according to the current row
 * within the partition. For instance: a FrameLessOffsetWindowFunction for value x with offset -2,
 * will get the value of x 2 rows back from the current row in the partition.
 */
//无框架偏移窗口函数（frameless offset window function）。与常规窗口函数不同，它没有指定窗口框架，而是基于当前行的偏移量来计算结果
//举例来说，如果偏移量为 -2，则函数返回的是当前行之前两行的数据
sealed abstract class FrameLessOffsetWindowFunction
  extends OffsetWindowFunction with Unevaluable with ImplicitCastInputTypes {

  /*
   * The result of an OffsetWindowFunction is dependent on the frame in which the
   * OffsetWindowFunction is executed, the input expression and the default expression. Even when
   * both the input and the default expression are foldable, the result is still not foldable due to
   * the frame.
   *
   * Note, the value of foldable is set to false in the trait Unevaluable
   *
   * override def foldable: Boolean = false
   */

  override def nullable: Boolean = default == null || default.nullable || input.nullable
  //返回窗口框架，默认情况下是 fakeFrame。由于该函数是无框架的，因此使用了一个“假”的框架 (fakeFrame)，即不对数据进行分区和排序限制，仅通过偏移来计算结果
  override lazy val frame: WindowFrame = fakeFrame

  override def checkInputDataTypes(): TypeCheckResult = {
    val check = super.checkInputDataTypes()
    if (check.isFailure) {
      check
    } else if (!offset.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "offset",
          "inputType" -> toSQLType(offset.dataType),
          "inputExpr" -> toSQLExpr(offset)
        )
      )
    } else {
      TypeCheckSuccess
    }
  }

  override def dataType: DataType = input.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(AnyDataType, IntegerType, TypeCollection(input.dataType, NullType))

  override def toString: String = s"$prettyName($input, $offset, $default)"
}

/**
 * The Lead function returns the value of `input` at the `offset`th row after the current row in
 * the window. Offsets start at 0, which is the current row. The offset must be constant
 * integer value. The default offset is 1. When the value of `input` is null at the `offset`th row,
 * null is returned. If there is no such offset row, the `default` expression is evaluated.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_(input[, offset[, default]]) - Returns the value of `input` at the `offset`th row
      after the current row in the window. The default value of `offset` is 1 and the default
      value of `default` is null. If the value of `input` at the `offset`th row is null,
      null is returned. If there is no such an offset row (e.g., when the offset is 1, the last
      row of the window does not have any subsequent row), `default` is returned.
  """,
  arguments = """
    Arguments:
      * input - a string expression to evaluate `offset` rows after the current row.
      * offset - an int expression which is rows to jump ahead in the partition.
      * default - a string expression which is to use when the offset is larger than the window.
          The default value is null.
  """,
  examples = """
    Examples:
      > SELECT a, b, _FUNC_(b) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	1
       A1	1	2
       A1	2	NULL
       A2	3	NULL
  """,
  since = "2.0.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class Lead(
    input: Expression, offset: Expression, default: Expression, ignoreNulls: Boolean)
    extends FrameLessOffsetWindowFunction with TernaryLike[Expression] {

  def this(input: Expression, offset: Expression, default: Expression) =
    this(input, offset, default, false)

  def this(input: Expression, offset: Expression) = this(input, offset, Literal(null))

  def this(input: Expression) = this(input, Literal(1))

  def this() = this(Literal(null))

  override def first: Expression = input
  override def second: Expression = offset
  override def third: Expression = default

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Lead =
    copy(input = newFirst, offset = newSecond, default = newThird)
}

/**
 * The Lag function returns the value of `input` at the `offset`th row before the current row in
 * the window. Offsets start at 0, which is the current row. The offset must be constant
 * integer value. The default offset is 1. When the value of `input` is null at the `offset`th row,
 * null is returned. If there is no such offset row, the `default` expression is evaluated.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_(input[, offset[, default]]) - Returns the value of `input` at the `offset`th row
      before the current row in the window. The default value of `offset` is 1 and the default
      value of `default` is null. If the value of `input` at the `offset`th row is null,
      null is returned. If there is no such offset row (e.g., when the offset is 1, the first
      row of the window does not have any previous row), `default` is returned.
  """,
  arguments = """
    Arguments:
      * input - a string expression to evaluate `offset` rows before the current row.
      * offset - an int expression which is rows to jump back in the partition.
      * default - a string expression which is to use when the offset row does not exist.
  """,
  examples = """
    Examples:
      > SELECT a, b, _FUNC_(b) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	NULL
       A1	1	1
       A1	2	1
       A2	3	NULL
  """,
  since = "2.0.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class Lag(
    input: Expression, inputOffset: Expression, default: Expression, ignoreNulls: Boolean)
    extends FrameLessOffsetWindowFunction with TernaryLike[Expression] {

  def this(input: Expression, inputOffset: Expression, default: Expression) =
    this(input, inputOffset, default, false)

  def this(input: Expression, inputOffset: Expression) = this(input, inputOffset, Literal(null))

  def this(input: Expression) = this(input, Literal(1))

  def this() = this(Literal(null))

  override val offset: Expression = UnaryMinus(inputOffset) match {
    case e: Expression if e.foldable => Literal.create(e.eval(EmptyRow), e.dataType)
    case o => o
  }

  override def first: Expression = input
  override def second: Expression = inputOffset
  override def third: Expression = default

  override protected def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Lag =
    copy(input = newFirst, inputOffset = newSecond, default = newThird)
}
//聚合窗口函数
abstract class AggregateWindowFunction extends DeclarativeAggregate with WindowFunction {
  self: Product =>
  override val frame: WindowFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = true
  override lazy val mergeExpressions =
    throw QueryExecutionErrors.mergeUnsupportedByWindowFunctionError(prettyName)
}

abstract class RowNumberLike extends AggregateWindowFunction {
  protected val zero = Literal(0)
  protected val one = Literal(1)
  protected val rowNumber = AttributeReference("rowNumber", IntegerType, nullable = false)()
  override val aggBufferAttributes: Seq[AttributeReference] = rowNumber :: Nil
  override val initialValues: Seq[Expression] = zero :: Nil
  override val updateExpressions: Seq[Expression] = rowNumber + one :: Nil
  override def nullable: Boolean = false
}

/**
 * A [[SizeBasedWindowFunction]] needs the size of the current window for its calculation.
 */
trait SizeBasedWindowFunction extends AggregateWindowFunction {
  // It's made a val so that the attribute created on driver side is serialized to executor side.
  // Otherwise, if it's defined as a function, when it's called on executor side, it actually
  // returns the singleton value instantiated on executor side, which has different expression ID
  // from the one created on driver side.
  val n: AttributeReference = SizeBasedWindowFunction.n
}

object SizeBasedWindowFunction {
  val n = AttributeReference("window__partition__size", IntegerType, nullable = false)()
}

/**
 * The RowNumber function computes a unique, sequential number to each row, starting with one,
 * according to the ordering of rows within the window partition.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_() - Assigns a unique, sequential number to each row, starting with one,
      according to the ordering of rows within the window partition.
  """,
  examples = """
    Examples:
      > SELECT a, b, _FUNC_() OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	1
       A1	1	2
       A1	2	3
       A2	3	1
  """,
  since = "2.0.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class RowNumber() extends RowNumberLike with LeafLike[Expression] {
  override val evaluateExpression = rowNumber
  override def prettyName: String = "row_number"
}

/**
 * The CumeDist function computes the position of a value relative to all values in the partition.
 * The result is the number of rows preceding or equal to the current row in the ordering of the
 * partition divided by the total number of rows in the window partition. Any tie values in the
 * ordering will evaluate to the same position.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_() - Computes the position of a value relative to all values in the partition.
  """,
  examples = """
    Examples:
      > SELECT a, b, _FUNC_() OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	0.6666666666666666
       A1	1	0.6666666666666666
       A1	2	1.0
       A2	3	1.0
  """,
  since = "2.0.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class CumeDist() extends RowNumberLike with SizeBasedWindowFunction with LeafLike[Expression] {
  override def dataType: DataType = DoubleType
  // The frame for CUME_DIST is Range based instead of Row based, because CUME_DIST must
  // return the same value for equal values in the partition.
  override val frame = SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
  override val evaluateExpression = rowNumber.cast(DoubleType) / n.cast(DoubleType)
  override def prettyName: String = "cume_dist"
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_(input[, offset]) - Returns the value of `input` at the row that is the `offset`th row
      from beginning of the window frame. Offset starts at 1. If ignoreNulls=true, we will skip
      nulls when finding the `offset`th row. Otherwise, every row counts for the `offset`. If
      there is no such an `offset`th row (e.g., when the offset is 10, size of the window frame
      is less than 10), null is returned.
  """,
  examples = """
    Examples:
      > SELECT a, b, _FUNC_(b, 2) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	1
       A1	1	1
       A1	2	1
       A2	3	NULL
  """,
  arguments = """
    Arguments:
      * input - the target column or expression that the function operates on.
      * offset - a positive int literal to indicate the offset in the window frame. It starts
          with 1.
      * ignoreNulls - an optional specification that indicates the NthValue should skip null
          values in the determination of which row to use.
  """,
  since = "3.1.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class NthValue(input: Expression, offset: Expression, ignoreNulls: Boolean)
    extends AggregateWindowFunction with OffsetWindowFunction with ImplicitCastInputTypes
    with BinaryLike[Expression] with QueryErrorsBase {

  def this(child: Expression, offset: Expression) = this(child, offset, false)

  override lazy val default = Literal.create(null, input.dataType)

  override def left: Expression = input
  override def right: Expression = offset

  override val frame: WindowFrame = UnspecifiedFrame

  override def dataType: DataType = input.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegerType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val check = super.checkInputDataTypes()
    if (check.isFailure) {
      check
    } else if (!offset.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "offset",
          "inputType" -> toSQLType(offset.dataType),
          "inputExpr" -> toSQLExpr(offset)
        )
      )
    } else if (offsetVal <= 0) {
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> "offset",
          "valueRange" -> s"(0, ${Long.MaxValue}]",
          "currentValue" -> toSQLValue(offsetVal, LongType)
        )
      )
    } else {
      TypeCheckSuccess
    }
  }

  private lazy val offsetVal = offset.eval().asInstanceOf[Int].toLong
  private lazy val result = AttributeReference("result", input.dataType)()
  private lazy val count = AttributeReference("count", LongType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = result :: count :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* result = */ default,
    /* count = */ Literal(1L)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (ignoreNulls) {
      Seq(
        /* result = */ If(count === offsetVal && input.isNotNull, input, result),
        /* count = */ If(input.isNull, count, count + 1L)
      )
    } else {
      Seq(
        /* result = */ If(count === offsetVal, input, result),
        /* count = */ count + 1L
      )
    }
  }

  override lazy val evaluateExpression: AttributeReference = result

  override def prettyName: String = "nth_value"
  override def sql: String =
    s"$prettyName(${input.sql}, ${offset.sql})${if (ignoreNulls) " ignore nulls" else ""}"

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): NthValue =
    copy(input = newLeft, offset = newRight)
}

/**
 * The NTile function divides the rows for each window partition into `n` buckets ranging from 1 to
 * at most `n`. Bucket values will differ by at most 1. If the number of rows in the partition does
 * not divide evenly into the number of buckets, then the remainder values are distributed one per
 * bucket, starting with the first bucket.
 *
 * The NTile function is particularly useful for the calculation of tertiles, quartiles, deciles and
 * other common summary statistics
 *
 * The function calculates two variables during initialization: The size of a regular bucket, and
 * the number of buckets that will have one extra row added to it (when the rows do not evenly fit
 * into the number of buckets); both variables are based on the size of the current partition.
 * During the calculation process the function keeps track of the current row number, the current
 * bucket number, and the row number at which the bucket will change (bucketThreshold). When the
 * current row number reaches bucket threshold, the bucket value is increased by one and the
 * threshold is increased by the bucket size (plus one extra if the current bucket is padded).
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_(n) - Divides the rows for each window partition into `n` buckets ranging
      from 1 to at most `n`.
  """,
  arguments = """
    Arguments:
      * buckets - an int expression which is number of buckets to divide the rows in.
          Default value is 1.
  """,
  examples = """
    Examples:
      > SELECT a, b, _FUNC_(2) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	1
       A1	1	1
       A1	2	2
       A2	3	1
  """,
  since = "2.0.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class NTile(buckets: Expression) extends RowNumberLike with SizeBasedWindowFunction
  with UnaryLike[Expression] with QueryErrorsBase {

  def this() = this(Literal(1))

  override def child: Expression = buckets

  // Validate buckets. Note that this could be relaxed, the bucket value only needs to constant
  // for each partition.
  override def checkInputDataTypes(): TypeCheckResult = {
    if (!buckets.foldable) {
      return DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "buckets",
          "inputType" -> toSQLType(buckets.dataType),
          "inputExpr" -> toSQLExpr(buckets)
        )
      )
    }

    if (buckets.dataType != IntegerType) {
      return DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> "1",
          "requiredType" -> toSQLType(IntegerType),
          "inputSql" -> toSQLExpr(buckets),
          "inputType" -> toSQLType(buckets.dataType))
      )
    }

    val i = buckets.eval().asInstanceOf[Int]
    if (i > 0) {
      TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> "buckets",
          "valueRange" -> s"(0, ${Int.MaxValue}]",
          "currentValue" -> toSQLValue(i, IntegerType)
        )
      )
    }
  }

  private val bucket = AttributeReference("bucket", IntegerType, nullable = false)()
  private val bucketThreshold =
    AttributeReference("bucketThreshold", IntegerType, nullable = false)()
  private val bucketSize = AttributeReference("bucketSize", IntegerType, nullable = false)()
  private val bucketsWithPadding =
    AttributeReference("bucketsWithPadding", IntegerType, nullable = false)()
  private def bucketOverflow(e: Expression) = If(rowNumber >= bucketThreshold, e, zero)

  override val aggBufferAttributes = Seq(
    rowNumber,
    bucket,
    bucketThreshold,
    bucketSize,
    bucketsWithPadding
  )

  override val initialValues = Seq(
    zero,
    zero,
    zero,
    (n div buckets).cast(IntegerType),
    (n % buckets).cast(IntegerType)
  )

  override val updateExpressions = Seq(
    rowNumber + one,
    bucket + bucketOverflow(one),
    bucketThreshold + bucketOverflow(bucketSize + If(bucket < bucketsWithPadding, one, zero)),
    NoOp,
    NoOp
  )

  override val evaluateExpression = bucket

  override protected def withNewChildInternal(
    newChild: Expression): NTile = copy(buckets = newChild)
}

/**
 * A RankLike function is a WindowFunction that changes its value based on a change in the value of
 * the order of the window in which is processed. For instance, when the value of `input` changes
 * in a window ordered by `input` the rank function also changes. The size of the change of the
 * rank function is (typically) not dependent on the size of the change in `input`.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
abstract class RankLike extends AggregateWindowFunction {

  /** Store the values of the window 'order' expressions. */
  protected val orderAttrs = children.map { expr =>
    AttributeReference(expr.sql, expr.dataType)()
  }

  /** Predicate that detects if the order attributes have changed. */
  protected val orderEquals = children.zip(orderAttrs)
    .map(EqualNullSafe.tupled)
    .reduceOption(And)
    .getOrElse(Literal(true))

  protected val orderInit = children.map(e => Literal.create(null, e.dataType))
  protected val rank = AttributeReference("rank", IntegerType, nullable = false)()
  protected val rowNumber = AttributeReference("rowNumber", IntegerType, nullable = false)()
  protected val zero = Literal(0)
  protected val one = Literal(1)
  protected val increaseRowNumber = rowNumber + one

  /**
   * Different RankLike implementations use different source expressions to update their rank value.
   * Rank for instance uses the number of rows seen, whereas DenseRank uses the number of changes.
   */
  protected def rankSource: Expression = rowNumber

  /** Increase the rank when the current rank == 0 or when the one of order attributes changes. */
  protected val increaseRank = If(orderEquals && rank =!= zero, rank, rankSource)

  override val aggBufferAttributes: Seq[AttributeReference] = rank +: rowNumber +: orderAttrs
  override val initialValues = zero +: one +: orderInit
  override val updateExpressions = increaseRank +: increaseRowNumber +: children
  override val evaluateExpression: Expression = rank

  override def nullable: Boolean = false
  override def sql: String = s"${prettyName.toUpperCase(Locale.ROOT)}()"

  def withOrder(order: Seq[Expression]): RankLike
}

/**
 * The Rank function computes the rank of a value in a group of values. The result is one plus the
 * number of rows preceding or equal to the current row in the ordering of the partition. The values
 * will produce gaps in the sequence.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_() - Computes the rank of a value in a group of values. The result is one plus the number
      of rows preceding or equal to the current row in the ordering of the partition. The values
      will produce gaps in the sequence.
  """,
  arguments = """
    Arguments:
      * children - this is to base the rank on; a change in the value of one the children will
          trigger a change in rank. This is an internal parameter and will be assigned by the
          Analyser.
  """,
  examples = """
    Examples:
      > SELECT a, b, _FUNC_(b) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	1
       A1	1	1
       A1	2	3
       A2	3	1
  """,
  since = "2.0.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class Rank(children: Seq[Expression]) extends RankLike {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): Rank = Rank(order)
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Rank =
    copy(children = newChildren)
}

/**
 * The DenseRank function computes the rank of a value in a group of values. The result is one plus
 * the previously assigned rank value. Unlike [[Rank]], [[DenseRank]] will not produce gaps in the
 * ranking sequence.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_() - Computes the rank of a value in a group of values. The result is one plus the
      previously assigned rank value. Unlike the function rank, dense_rank will not produce gaps
      in the ranking sequence.
  """,
  arguments = """
    Arguments:
      * children - this is to base the rank on; a change in the value of one the children will
          trigger a change in rank. This is an internal parameter and will be assigned by the
          Analyser.
  """,
  examples = """
    Examples:
      > SELECT a, b, _FUNC_(b) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	1
       A1	1	1
       A1	2	2
       A2	3	1
  """,
  since = "2.0.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class DenseRank(children: Seq[Expression]) extends RankLike {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): DenseRank = DenseRank(order)
  override protected def rankSource = rank + one
  override val updateExpressions = increaseRank +: children
  override val aggBufferAttributes = rank +: orderAttrs
  override val initialValues = zero +: orderInit
  override def prettyName: String = "dense_rank"
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): DenseRank =
    copy(children = newChildren)
}

/**
 * The PercentRank function computes the percentage ranking of a value in a group of values. The
 * result the rank of the minus one divided by the total number of rows in the partition minus one:
 * (r - 1) / (n - 1). If a partition only contains one row, the function will return 0.
 *
 * The PercentRank function is similar to the CumeDist function, but it uses rank values instead of
 * row counts in the its numerator.
 *
 * This documentation has been based upon similar documentation for the Hive and Presto projects.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_() - Computes the percentage ranking of a value in a group of values.
  """,
  arguments = """
    Arguments:
      * children - this is to base the rank on; a change in the value of one the children will
          trigger a change in rank. This is an internal parameter and will be assigned by the
          Analyser.
  """,
  examples = """
    Examples:
      > SELECT a, b, _FUNC_(b) OVER (PARTITION BY a ORDER BY b) FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
       A1	1	0.0
       A1	1	0.0
       A1	2	1.0
       A2	3	0.0
  """,
  since = "2.0.0",
  group = "window_funcs")
// scalastyle:on line.size.limit line.contains.tab
case class PercentRank(children: Seq[Expression]) extends RankLike with SizeBasedWindowFunction {
  def this() = this(Nil)
  override def withOrder(order: Seq[Expression]): PercentRank = PercentRank(order)
  override def dataType: DataType = DoubleType
  override val evaluateExpression =
    If(n > one, (rank - one).cast(DoubleType) / (n - one).cast(DoubleType), 0.0d)
  override def prettyName: String = "percent_rank"
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): PercentRank =
    copy(children = newChildren)
}

/**
 * Exponential Weighted Moment. This expression is dedicated only for Pandas API on Spark.
 * An exponentially weighted window is similar to an expanding window but with each prior point
 * being exponentially weighted down relative to the current point.
 * See https://pandas.pydata.org/docs/user_guide/window.html#exponentially-weighted-window
 * for details.
 * Currently, only weighted moving average is supported. In general, it is calculated as
 *    y_t = \frac{\sum_{i=0}^t w_i x_{t-i}}{\sum_{i=0}^t w_i},
 * where x_t is the input, y_t is the result and the w_i are the weights.
 */
case class EWM(input: Expression, alpha: Double, ignoreNA: Boolean)
  extends AggregateWindowFunction with UnaryLike[Expression] {
  assert(0 < alpha && alpha <= 1)

  override def dataType: DataType = DoubleType

  private val numerator = AttributeReference("numerator", DoubleType, nullable = false)()
  private val denominator = AttributeReference("denominator", DoubleType, nullable = false)()
  private val result = AttributeReference("result", DoubleType, nullable = true)()

  override def aggBufferAttributes: Seq[AttributeReference] =
    numerator :: denominator :: result :: Nil

  override val initialValues: Seq[Expression] =
    Literal(0.0) :: Literal(0.0) :: Literal.create(null, DoubleType) :: Nil

  override val updateExpressions: Seq[Expression] = {
    val beta = Literal(1.0 - alpha)
    val casted = input.cast(DoubleType)
    val isNA = IsNull(casted)
    val newNumerator = numerator * beta + casted
    val newDenominator = denominator * beta + Literal(1.0)

    if (ignoreNA) {
      /* numerator = */ If(isNA, numerator, newNumerator) ::
      /* denominator = */ If(isNA, denominator, newDenominator) ::
      /* result = */ If(isNA, result, newNumerator / newDenominator) :: Nil
    } else {
      /* numerator = */ If(isNA, numerator * beta, newNumerator) ::
      /* denominator = */ If(isNA, denominator * beta, newDenominator) ::
      /* result = */ If(isNA, result, newNumerator / newDenominator) :: Nil
    }
  }

  override val evaluateExpression: Expression = result

  override def prettyName: String = "ewm"

  override def sql: String = s"$prettyName(${input.sql}, $alpha, $ignoreNA)"

  override def child: Expression = input

  override protected def withNewChildInternal(newChild: Expression): EWM = copy(input = newChild)
}


/**
 * Keep the last non-null value seen if any. This expression is dedicated only for
 * Pandas API on Spark.
 * For example,
 *  Input: null, 1, 2, 3, null, 4, 5, null
 *  Output: null, 1, 2, 3, 3, 4, 5, 5
 */
case class LastNonNull(input: Expression)
  extends AggregateWindowFunction with UnaryLike[Expression] {

  override def dataType: DataType = input.dataType

  private lazy val last = AttributeReference("last", dataType, nullable = true)()

  override def aggBufferAttributes: Seq[AttributeReference] = last :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(Literal.create(null, dataType))

  override lazy val updateExpressions: Seq[Expression] = {
    Seq(
      /* last = */ If(IsNull(input), last, input)
    )
  }

  override lazy val evaluateExpression: Expression = last

  override def prettyName: String = "last_non_null"

  override def sql: String = s"$prettyName(${input.sql})"

  override def child: Expression = input

  override protected def withNewChildInternal(newChild: Expression): LastNonNull =
    copy(input = newChild)
}


/**
 * Return the indices for consecutive null values, for non-null values, it returns 0.
 * This expression is dedicated only for Pandas API on Spark.
 * For example,
 *  Input: null, 1, 2, 3, null, null, null, 5, null, null
 *  Output: 1, 0, 0, 0, 1, 2, 3, 0, 1, 2
 */
case class NullIndex(input: Expression)
  extends AggregateWindowFunction with UnaryLike[Expression] {

  override def dataType: DataType = IntegerType

  private val index = AttributeReference("index", IntegerType, nullable = false)()

  override def aggBufferAttributes: Seq[AttributeReference] = index :: Nil

  override val initialValues: Seq[Expression] = Seq(Literal(0))

  override val updateExpressions: Seq[Expression] = {
    Seq(
      /* index = */ If(IsNull(input), index + Literal(1), Literal(0))
    )
  }

  override val evaluateExpression: Expression = index

  override def prettyName: String = "null_index"

  override def sql: String = s"$prettyName(${input.sql})"

  override def child: Expression = input

  override protected def withNewChildInternal(newChild: Expression): NullIndex =
    copy(input = newChild)
}
