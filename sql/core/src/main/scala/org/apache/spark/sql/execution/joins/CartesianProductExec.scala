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

package org.apache.spark.sql.execution.joins

import org.apache.spark._
import org.apache.spark.rdd.{CartesianPartition, CartesianRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, Predicate, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.CompletionIterator

/**
 * An optimized CartesianRDD for UnsafeRow, which will cache the rows from second child RDD,
 * will be much faster than building the right partition for every row in left RDD, it also
 * materialize the right RDD (in case of the right RDD is nondeterministic).
 */
// UnsafeCartesianRDD 是对标准 CartesianRDD 的性能优化版本，专门用于处理 Spark SQL 中的 UnsafeRow 数据格式
// 高效笛卡尔积： 计算两个输入 RDD（都包含 UnsafeRow）的笛卡尔积。
// 右侧分区缓存和物化： 在每个 Task 内部，它会完整地缓存 (Cache) 或物化 (Materialize) 第二个 RDD (right) 的对应分区数据。
class UnsafeCartesianRDD(
    left : RDD[UnsafeRow],
    right : RDD[UnsafeRow],
    inMemoryBufferThreshold: Int, // 内存缓冲阈值。
    spillThreshold: Int) // 控制内存缓冲达到限制后，可以溢出到磁盘的行数或字节数限制。
  extends CartesianRDD[UnsafeRow, UnsafeRow](left.sparkContext, left, right) {

  override def compute(split: Partition, context: TaskContext): Iterator[(UnsafeRow, UnsafeRow)] = {
    // 它使用 ExternalAppendOnlyUnsafeRowArray 将 right 分区的所有行读取并存储起来（内存或磁盘溢出）
    val rowArray = new ExternalAppendOnlyUnsafeRowArray(inMemoryBufferThreshold, spillThreshold)

    val partition = split.asInstanceOf[CartesianPartition]
    rdd2.iterator(partition.s2, context).foreach(rowArray.add)

    // Create an iterator from rowArray
    def createIter(): Iterator[UnsafeRow] = rowArray.generateIterator()

    val resultIter =
      for (x <- rdd1.iterator(partition.s1, context);
           y <- createIter()) yield (x, y)
    CompletionIterator[(UnsafeRow, UnsafeRow), Iterator[(UnsafeRow, UnsafeRow)]](
      resultIter, rowArray.clear())
  }
}

// 负责在 Spark SQL 中执行笛卡尔积（Cartesian Product）操作
// 计算所有配对： 它将左侧子计划 (left) 的每一行与右侧子计划 (right) 的所有行进行组合，生成所有可能的行配对
// 条件过滤： 尽管笛卡尔积本质上是无条件的，但如果提供了 condition（通常是由优化器从非等值连接中转换而来），它会在生成配对后应用该过滤条件，将笛卡尔积转换为非等值连接 (Non-Equi Join) 的实现方式。
case class CartesianProductExec(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) // 可选的过滤条件。
  extends BaseJoinExec {

  override def joinType: JoinType = Inner
  override def leftKeys: Seq[Expression] = Nil
  override def rightKeys: Seq[Expression] = Nil

  override def output: Seq[Attribute] = left.output ++ right.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val leftResults = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResults = right.execute().asInstanceOf[RDD[UnsafeRow]]
    // 创建优化的笛卡尔积 RDD。
    val pair = new UnsafeCartesianRDD(
      leftResults,
      rightResults,
      conf.cartesianProductExecBufferInMemoryThreshold,
      conf.cartesianProductExecBufferSpillThreshold)
    // 分区内的逻辑。
    pair.mapPartitionsWithIndexInternal { (index, iter) =>
      // 使用 Codegen 生成一个高效的 UnsafeRowJoiner，用于将左侧行 (r._1) 和右侧行 (r._2) 合并成一个新的 UnsafeRow 结果行。
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      val filtered = if (condition.isDefined) {
        val boundCondition = Predicate.create(condition.get, left.output ++ right.output)
        boundCondition.initialize(index)
        val joined = new JoinedRow

        iter.filter { r =>
          boundCondition.eval(joined(r._1, r._2))
        }
      } else {
        iter
      }
      filtered.map { r =>
        numOutputRows += 1
        joiner.join(r._1, r._2)
      }
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): CartesianProductExec =
    copy(left = newLeft, right = newRight)
}
