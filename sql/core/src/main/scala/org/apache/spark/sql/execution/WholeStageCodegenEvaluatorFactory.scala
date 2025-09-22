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

package org.apache.spark.sql.execution

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator}
import org.apache.spark.sql.execution.metric.SQLMetric
//是 PartitionEvaluatorFactory 的一个实现，用于创建基于代码生成的 PartitionEvaluator 实例。这个工厂创建的评估器将执行代码生成后的逻辑，处理每个分区的数据
class WholeStageCodegenEvaluatorFactory(
    cleanedSource: CodeAndComment,  //包含了经过格式化和清理的生成代码
    durationMs: SQLMetric,  //用于记录执行阶段的时间
    references: Array[Any]) extends PartitionEvaluatorFactory[InternalRow, InternalRow] { //代码生成过程中需要的所有引用（即使用的变量）

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] = {
    new WholeStageCodegenPartitionEvaluator()
  }

  class WholeStageCodegenPartitionEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,  //当前分区的索引
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val (clazz, _) = CodeGenerator.compile(cleanedSource) //编译代码，这将返回编译后的类（clazz）和相关的信息
      //生成的代码继承GeneratedClass，generate是generate的方法，clazz.generate(references) 会生成一个迭代器，该迭代器执行经过代码生成的查询逻辑
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.init(partitionIndex, inputs.toArray)  //在初始化代码输入数据inputs
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val v = buffer.hasNext
          if (!v) durationMs += buffer.durationMs()
          v
        }
        override def next: InternalRow = buffer.next()
      }
    }
  }
}
