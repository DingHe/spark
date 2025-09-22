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

package org.apache.spark.sql.execution.window

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.Utils

/**
 * Holds common logic for window operators
 */
trait WindowExecBase extends UnaryExecNode {
  def windowExpression: Seq[NamedExpression]  //窗口表达式序列。每个窗口表达式通常是一个窗口函数（如 ROW_NUMBER()、RANK() 等）
  def partitionSpec: Seq[Expression]  //窗口操作的分区规范，它定义了将输入数据按哪些列进行分区
  def orderSpec: Seq[SortOrder]      //窗口操作的排序规范，它定义了在每个分区内数据的排序方式

  override def output: Seq[Attribute] =
    child.output ++ windowExpression.map(_.toAttribute)  //窗口操作的输出列。窗口操作的输出由原始输入数据的输出列（child.output）和计算的窗口表达式（windowExpression）的结果组成
  //窗口操作要求数据在某些列上进行分区。如果没有定义分区规范，则会将所有数据移动到一个分区，这可能会导致性能下降。否则，它将根据 partitionSpec 使用分区
  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else {
      ClusteredDistribution(partitionSpec) :: Nil
    }
  }
  //窗口操作要求数据在分区内根据 partitionSpec 和 orderSpec 排序
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)
  //通常与输入数据的排序方式（child.outputOrdering）相同
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  //通常与输入数据的分区方式（child.outputPartitioning）相同
  override def outputPartitioning: Partitioning = child.outputPartitioning

  /**
   * Create the resulting projection.
   *
   * This method uses Code Generation. It can only be used on the executor side.
   *
   * @param expressions unbound ordered function expressions.
   * @return the final resulting projection.
   */
    //expressions  需要进行计算的窗口函数表达式。
  protected def createResultProjection(expressions: Seq[Expression]): UnsafeProjection = {
    val references = expressions.zipWithIndex.map { case (e, i) =>
      // Results of window expressions will be on the right side of child's output
      BoundReference(child.output.size + i, e.dataType, e.nullable)
    }
    val unboundToRefMap = Utils.toMap(expressions, references)
    val patchedWindowExpression = windowExpression.map(_.transform(unboundToRefMap))
    UnsafeProjection.create(
      child.output ++ patchedWindowExpression,
      child.output)
  }

  /**
   * Create a bound ordering object for a given frame type and offset. A bound ordering object is
   * used to determine which input row lies within the frame boundaries of an output row.
   *
   * This method uses Code Generation. It can only be used on the executor side.
   *
   * @param frame to evaluate. This can either be a Row or Range frame.
   * @param bound with respect to the row.
   * @param timeZone the session local timezone for time related calculations.
   * @return a bound ordering object.
   */
    //frame: FrameType，表示窗口的框架类型
    //bound: Expression，表示窗口的边界条件
    //BoundOrdering 对象，这个对象表示如何在窗口计算中确定哪些输入行位于输出行的窗口边界内
  private def createBoundOrdering(
      frame: FrameType, bound: Expression, timeZone: String): BoundOrdering = {
    (frame, bound) match {
      case (RowFrame, CurrentRow) =>
        RowBoundOrdering(0)  //RowBoundOrdering(0) 对象，表示当前行的偏移量为 0

      case (RowFrame, IntegerLiteral(offset)) =>
        RowBoundOrdering(offset)  //如果框架是 RowFrame 且边界是一个整数值（表示行的偏移量），则创建一个 RowBoundOrdering(offset) 对象，表示该偏移量的值
      //边界类型不符合预期
      case (RowFrame, _) =>
        throw new IllegalStateException(s"Unhandled bound in windows expressions: $bound")
      //框架是 RangeFrame 且边界是 CurrentRow，则创建一个 RangeBoundOrdering 对象。
      // RangeBoundOrdering 需要一个排序对象 (ordering) 和两个投影（current 和 bound）
      case (RangeFrame, CurrentRow) =>
        val ordering = RowOrdering.create(orderSpec, child.output)
        RangeBoundOrdering(ordering, IdentityProjection, IdentityProjection)
      //如果框架是 RangeFrame 且边界是一个偏移量（offset），并且只有一个排序表达式（orderSpec.size == 1）
      case (RangeFrame, offset: Expression) if orderSpec.size == 1 =>  //只能一个排序字段
        // Use only the first order expression when the offset is non-null.
        val sortExpr = orderSpec.head
        val expr = sortExpr.child

        // Create the projection which returns the current 'value'.
        val current = MutableProjection.create(expr :: Nil, child.output)  //返回当前排序表达式的值

        // Flip the sign of the offset when processing the order is descending
        val boundOffset = sortExpr.direction match { //根据排序的方向（Ascending 或 Descending），对偏移量进行符号反转
          case Descending => UnaryMinus(offset)
          case Ascending => offset
        }

        // Create the projection which returns the current 'value' modified by adding the offset.
        //根据 expr 和 boundOffset 的数据类型匹配创建适当的边界表达式。
        // 比如，如果 expr 是日期类型且 boundOffset 是整数类型，则使用 DateAdd；如果 expr 是时间戳类型，则使用 TimeAdd 等等
        val boundExpr = (expr.dataType, boundOffset.dataType) match {
          case (DateType, IntegerType) => DateAdd(expr, boundOffset)
          case (DateType, _: YearMonthIntervalType) => DateAddYMInterval(expr, boundOffset)
          case (TimestampType | TimestampNTZType, CalendarIntervalType) =>
            TimeAdd(expr, boundOffset, Some(timeZone))
          case (TimestampType | TimestampNTZType, _: YearMonthIntervalType) =>
            TimestampAddYMInterval(expr, boundOffset, Some(timeZone))
          case (TimestampType | TimestampNTZType, _: DayTimeIntervalType) =>
            TimeAdd(expr, boundOffset, Some(timeZone))
          case (d: DecimalType, _: DecimalType) => DecimalAddNoOverflowCheck(expr, boundOffset, d)
          case (a, b) if a == b => Add(expr, boundOffset)
        }
        val bound = MutableProjection.create(boundExpr :: Nil, child.output)  //边界行的值

        // Construct the ordering. This is used to compare the result of current value projection
        // to the result of bound value projection. This is done manually because we want to use
        // Code Generation (if it is enabled).
        val boundSortExprs = sortExpr.copy(BoundReference(0, expr.dataType, expr.nullable)) :: Nil
        val ordering = RowOrdering.create(boundSortExprs, Nil)
        RangeBoundOrdering(ordering, current, bound)

      case (RangeFrame, _) =>
        throw new IllegalStateException("Non-Zero range offsets are not supported for windows " +
          "with multiple order expressions.")
    }
  }

  /**
   * Collection containing an entry for each window frame to process. Each entry contains a frame's
   * [[WindowExpression]]s and factory function for the [[WindowFunctionFrame]].
   */
  //将不同的窗口表达式（WindowExpression）与窗口帧工厂（WindowFunctionFrame）进行绑定，最终生成一个集合，存储了每种窗口帧及其对应的处理逻辑
  protected lazy val windowFrameExpressionFactoryPairs = {
    type FrameKey = (String, FrameType, Expression, Expression, Seq[Expression])
    //String：标识窗口函数的类型（例如 "AGGREGATE"）
    //FrameType：表示窗口帧的类型，可能是 RowFrame 或 RangeFrame
    //Expression：表示帧的上下边界
    //Seq[Expression]：表示窗口函数的参数
    type ExpressionBuffer = mutable.Buffer[Expression]
    val framedFunctions = mutable.Map.empty[FrameKey, (ExpressionBuffer, ExpressionBuffer)]

    // Add a function and its function to the map for a given frame.
    //用于将窗口函数和窗口帧对应起来，并存储在 framedFunctions 字典中
    //tpe：表示窗口函数的类型，通常是 "AGGREGATE" 等
    //fr：表示窗口帧的配置（类型为 SpecifiedWindowFrame），包括帧的类型、上下边界等
    //e：表示一个窗口表达式（WindowExpression），包含了要执行的窗口函数及其相关信息
    //fn：表示一个窗口函数（Expression），例如 AggregateExpression 等
    def collect(tpe: String, fr: SpecifiedWindowFrame, e: Expression, fn: Expression): Unit = {
      val key = fn match {
        // This branch is used for Lead/Lag to support ignoring null and optimize the performance
        // for NthValue ignoring null.
        // All window frames move in rows. If there are multiple Leads, Lags or NthValues acting on
        // a row and operating on different input expressions, they should not be moved uniformly
        // by row. Therefore, we put these functions in different window frames.
        case f: OffsetWindowFunction if f.ignoreNulls =>
          (tpe, fr.frameType, fr.lower, fr.upper, f.children.map(_.canonicalized))
        case _ => (tpe, fr.frameType, fr.lower, fr.upper, Nil)
      }
      val (es, fns) = framedFunctions.getOrElseUpdate(
        key, (ArrayBuffer.empty[Expression], ArrayBuffer.empty[Expression]))
      es += e
      fns += fn
    }

    // Collect all valid window functions and group them by their frame.
    //遍历所有的窗口表达式，并根据帧配置（frameSpecification）对窗口函数进行分类
    windowExpression.foreach { x =>
      x.foreach {
        case e @ WindowExpression(function, spec) =>
          val frame = spec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
          function match {
            case AggregateExpression(f, _, _, _, _) => collect("AGGREGATE", frame, e, f)
            case f: FrameLessOffsetWindowFunction =>
              collect("FRAME_LESS_OFFSET", f.fakeFrame, e, f)
            case f: OffsetWindowFunction if frame.frameType == RowFrame &&
              frame.lower == UnboundedPreceding =>
              frame.upper match {
                case UnboundedFollowing => collect("UNBOUNDED_OFFSET", f.fakeFrame, e, f)
                case CurrentRow => collect("UNBOUNDED_PRECEDING_OFFSET", f.fakeFrame, e, f)
                case _ => collect("AGGREGATE", frame, e, f)
              }
            case f: AggregateWindowFunction => collect("AGGREGATE", frame, e, f)
            case f => throw new IllegalStateException(s"Unsupported window function: $f")
          }
        case _ =>
      }
    }

    // Map the groups to a (unbound) expression and frame factory pair.
    //为每个窗口帧创建工厂函数
    var numExpressions = 0
    val timeZone = conf.sessionLocalTimeZone
    framedFunctions.toSeq.map {
      case (key, (expressions, functionSeq)) =>
        val ordinal = numExpressions
        val functions = functionSeq.toArray

        // Construct an aggregate processor if we need one.
        // Currently we don't allow mixing of Pandas UDF and SQL aggregation functions
        // in a single Window physical node. Therefore, we can assume no SQL aggregation
        // functions if Pandas UDF exists. In the future, we might mix Pandas UDF and SQL
        // aggregation function in a single physical node.
        def processor = if (functions.exists(_.isInstanceOf[PythonFuncExpression])) {
          null
        } else {
          AggregateProcessor(
            functions,
            ordinal,
            child.output,
            (expressions, schema) =>
              MutableProjection.create(expressions, schema))
        }

        // Create the factory to produce WindowFunctionFrame.
        val factory = key match {
          // Frameless offset Frame
          case ("FRAME_LESS_OFFSET", _, IntegerLiteral(offset), _, expr) =>
            target: InternalRow =>
              new FrameLessOffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunction.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                child.output,
                (expressions, schema) =>
                  MutableProjection.create(expressions, schema),
                offset,
                expr.nonEmpty)
          case ("UNBOUNDED_OFFSET", _, IntegerLiteral(offset), _, expr) =>
            target: InternalRow => {
              new UnboundedOffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunction.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                child.output,
                (expressions, schema) =>
                  MutableProjection.create(expressions, schema),
                offset,
                expr.nonEmpty)
            }
          case ("UNBOUNDED_PRECEDING_OFFSET", _, IntegerLiteral(offset), _, expr) =>
            target: InternalRow => {
              new UnboundedPrecedingOffsetWindowFunctionFrame(
                target,
                ordinal,
                // OFFSET frame functions are guaranteed be OffsetWindowFunction.
                functions.map(_.asInstanceOf[OffsetWindowFunction]),
                child.output,
                (expressions, schema) =>
                  MutableProjection.create(expressions, schema),
                offset,
                expr.nonEmpty)
            }

          // Entire Partition Frame.
          case ("AGGREGATE", _, UnboundedPreceding, UnboundedFollowing, _) =>
            target: InternalRow => {
              new UnboundedWindowFunctionFrame(target, processor)
            }

          // Growing Frame.
          case ("AGGREGATE", frameType, UnboundedPreceding, upper, _) =>
            target: InternalRow => {
              new UnboundedPrecedingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, upper, timeZone))
            }

          // Shrinking Frame.
          case ("AGGREGATE", frameType, lower, UnboundedFollowing, _) =>
            target: InternalRow => {
              new UnboundedFollowingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, lower, timeZone))
            }

          // Moving Frame.
          case ("AGGREGATE", frameType, lower, upper, _) =>
            target: InternalRow => {
              new SlidingWindowFunctionFrame(
                target,
                processor,
                createBoundOrdering(frameType, lower, timeZone),
                createBoundOrdering(frameType, upper, timeZone))
            }

          case _ =>
            throw new IllegalStateException(s"Unsupported factory: $key")
        }

        // Keep track of the number of expressions. This is a side-effect in a map...
        numExpressions += expressions.size

        // Create the Window Expression - Frame Factory pair.
        (expressions, factory)
    }
  }
}
