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
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._


/** 用于 Spark SQL 中对数据行进行排序和比较
 * A base class for generated/interpreted row ordering.
 */
class BaseOrdering extends Ordering[InternalRow] {
  def compare(a: InternalRow, b: InternalRow): Int = {
    throw new UnsupportedOperationException
  }
}

/**
 * An interpreted row ordering comparator.
 */
class InterpretedOrdering(ordering: Seq[SortOrder]) extends BaseOrdering {
  private lazy val physicalDataTypes = ordering.map(order => PhysicalDataType(order.dataType)) //dadaType是排序字段的类型

  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(bindReferences(ordering, inputSchema))  //SortOrder 的序列，表示排序顺序；inputSchema 是输入的表结构。bindReferences 方法将 SortOrder 与 inputSchema 中的字段绑定，确保排序字段能与实际的数据进行匹配

  override def compare(a: InternalRow, b: InternalRow): Int = {
    var i = 0
    val size = ordering.size
    while (i < size) { //首先循环遍历 ordering 中的每一个排序条件（SortOrder）。对于每个排序条件，比较 a 和 b 在该列的值
      val order = ordering(i)
      val left = order.child.eval(a)
      val right = order.child.eval(b)

      if (left == null && right == null) {
        // Both null, continue looking.
      } else if (left == null) {
        return if (order.nullOrdering == NullsFirst) -1 else 1 //如果某一列的值是 null，而另一列的值不是 null，则根据 nullOrdering（NullsFirst 或 NullsLast）决定其顺序
      } else if (right == null) {
        return if (order.nullOrdering == NullsFirst) 1 else -1
      } else { //如果两个值都不为 null，则根据该列的排序方向（升序或降序）使用对应的比较器（orderingFunc）进行比较
        val orderingFunc = physicalDataTypes(i).ordering.asInstanceOf[Ordering[Any]]
        val comparison = order.dataType match {
          case _ if order.direction == Ascending =>
            orderingFunc.compare(left, right)
          case _ if order.direction == Descending =>
            - orderingFunc.compare(left, right)
        }
        if (comparison != 0) {
          return comparison
        }
      }
      i += 1
    }
    0
  }
}

object InterpretedOrdering {

  /** 用于为给定的模式（dataTypes）创建一个 InterpretedOrdering，默认的排序方向是升序（Ascending）
   * Creates a [[InterpretedOrdering]] for the given schema, in natural ascending order.
   */
  def forSchema(dataTypes: Seq[DataType]): InterpretedOrdering = {
    new InterpretedOrdering(dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    })
  }
}

object RowOrdering extends CodeGeneratorWithInterpretedFallback[Seq[SortOrder], BaseOrdering] {

  /**
   * Returns true iff the data type can be ordered (i.e. can be sorted).
   */
  def isOrderable(dataType: DataType): Boolean = OrderUtils.isOrderable(dataType)

  /**
   * Returns true iff outputs from the expressions can be ordered.
   */
  def isOrderable(exprs: Seq[Expression]): Boolean = exprs.forall(e => isOrderable(e.dataType))

  override protected def createCodeGeneratedObject(in: Seq[SortOrder]): BaseOrdering = {
    GenerateOrdering.generate(in)  //通过 GenerateOrdering.generate 生成排序的代码
  }

  override protected def createInterpretedObject(in: Seq[SortOrder]): BaseOrdering = {
    new InterpretedOrdering(in)   //通过 InterpretedOrdering 解释执行排序操作
  }

  def create(order: Seq[SortOrder], inputSchema: Seq[Attribute]): BaseOrdering = {
    createObject(bindReferences(order, inputSchema))
  }

  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  def createNaturalAscendingOrdering(dataTypes: Seq[DataType]): BaseOrdering = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    create(order, Seq.empty)
  }
}
