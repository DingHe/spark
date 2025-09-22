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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral, JavaCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._

/**
 * A bound reference points to a specific slot in the input tuple, allowing the actual value
 * to be retrieved more efficiently.  However, since operations like column pruning can change
 * the layout of intermediate tuples, BindReferences should be run after all such transformations.
 */
//ordinal 表示该 BoundReference 引用的输入元组中列的索引位置。也就是说，它指向输入数据中某一列的特定位置
//dataType 表示该列的数据类型，可能是 IntegerType、StringType 等。这决定了该列的值如何存储和访问
//nullable 表示该列是否允许为 null 值。如果为 true，则该列允许包含 null 值；如果为 false，则该列不允许为 null
case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends LeafExpression {

  override def toString: String = s"input[$ordinal, ${dataType.simpleString}, $nullable]"

  private val accessor: (InternalRow, Int) => Any = InternalRow.getAccessor(dataType, nullable)

  // Use special getter for primitive types (for UnsafeRow)
  override def eval(input: InternalRow): Any = {
    accessor(input, ordinal)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (ctx.currentVars != null && ctx.currentVars(ordinal) != null) {
      val oev = ctx.currentVars(ordinal)
      ev.isNull = oev.isNull
      ev.value = oev.value
      ev.copy(code = oev.code)
    } else {  //处理输入行的数据
      assert(ctx.INPUT_ROW != null, "INPUT_ROW and currentVars cannot both be null.")
      val javaType = JavaCode.javaType(dataType)  //返回spark数据类型对应的java数据类型
      val value = CodeGenerator.getValue(ctx.INPUT_ROW, dataType, ordinal.toString)  //从给定行的位置，获取对应的数据，例如inputadapter_row_0.getLong(0)
      if (nullable) {  //如果值允许为空，则首先利用isNullAt函数检车是否为空
        ev.copy(code =
          code"""
             |boolean ${ev.isNull} = ${ctx.INPUT_ROW}.isNullAt($ordinal);
             |$javaType ${ev.value} = ${ev.isNull} ?
             |  ${CodeGenerator.defaultValue(dataType)} : ($value);
           """.stripMargin)
      } else {
        ev.copy(code = code"$javaType ${ev.value} = $value;", isNull = FalseLiteral)
      }
    }
  }
}

object BindReferences extends Logging {
  //用于将表达式中的 AttributeReference 转换为 BoundReference，这是一种“绑定”操作，允许表达式通过访问输入元组来获取数据
  def bindReference[A <: Expression](
      expression: A,
      input: AttributeSeq, //表示输入数据的列的序列
      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference =>
      val ordinal = input.indexOf(a.exprId)  //找到表达式在input中的位置
      if (ordinal == -1) {
        if (allowFailures) {
          a
        } else {
          throw new IllegalStateException(
            s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
        }
      } else {
        BoundReference(ordinal, a.dataType, input(ordinal).nullable)
      }
    }.asInstanceOf[A] // Kind of a hack, but safe.  TODO: Tighten return type when possible.
  }

  /**
   * A helper function to bind given expressions to an input schema.
   */
    //用于批量绑定表达式序列。它接受一个表达式序列和输入列的元组，然后为每个表达式调用 bindReference 进行绑定
  def bindReferences[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[A] = {
    expressions.map(BindReferences.bindReference(_, input))
  }
}
