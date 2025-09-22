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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Projection


/**
 * Function for comparing boundary values.
 */
//表示用于比较 边界值 的操作
private[window] abstract class BoundOrdering {
  //返回 0 表示两个值相等
  //返回负值表示输入行在输出行之前
  //返回正值表示输入行在输出行之后
  def compare(inputRow: InternalRow, inputIndex: Int, outputRow: InternalRow, outputIndex: Int): Int
}

/**
 * Compare the input index to the bound of the output index.
 */
//用于比较基于 行偏移 的边界，常见于 行帧（RowFrame）
//offset：构造函数中的偏移量，表示当前行相对于参考行的位置偏移，通常为正负整数
private[window] final case class RowBoundOrdering(offset: Int) extends BoundOrdering {
  override def compare(
      inputRow: InternalRow,
      inputIndex: Int,
      outputRow: InternalRow,
      outputIndex: Int): Int = {
    //输入行的索引（inputIndex）与 输出行的索引（outputIndex）的差异加上 offset，从而确定当前行是否位于窗口的有效范围内
    inputIndex - (outputIndex + offset)
  }
}

/**
 * Compare the value of the input index to the value bound of the output index.
 */
//用于比较基于 值范围 的边界，常见于 范围帧（RangeFrame）。它依赖于 ORDER BY 表达式的值来判断两个行之间的相对位置
//ordering：传入的 Ordering[InternalRow]，用于对行的 ORDER BY 表达式进行排序。它决定了如何比较两个行的值
//current 和 bound：这两个是 投影（Projection）对象，用于获取输入行和输出行的排序键。投影实际上是将 InternalRow 映射成我们需要比较的列值
private[window] final case class RangeBoundOrdering(
    ordering: Ordering[InternalRow],
    current: Projection,
    bound: Projection)
  extends BoundOrdering {

  override def compare(
      inputRow: InternalRow,
      inputIndex: Int,
      outputRow: InternalRow,
      outputIndex: Int): Int = {
    //使用提供的 ordering 对输入行的排序值（current(inputRow)）与输出行的排序边界值（bound(outputRow)）进行比较。返回的 Int 表示输入行与输出行在排序顺序中的关系
    ordering.compare(current(inputRow), bound(outputRow))
  }
}
