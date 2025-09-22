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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, JoinedRow, Predicate, Projection, RowOrdering, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
//用于创建基于排序合并连接（Sort Merge Join）的评估器（Evaluator）。排序合并连接是一种常见的连接算法，通常用于处理大数据量的连接操作，特别是当两个数据集都已排序时
class SortMergeJoinEvaluatorFactory(
    leftKeys: Seq[Expression],     //分别表示左侧和右侧数据集用于连接的键列
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression], //可选的条件表达式，用于过滤连接结果
    left: SparkPlan,               //两个参数是连接的左侧和右侧数据集
    right: SparkPlan,
    output: Seq[Attribute],        //表示连接操作后产生的输出列
    inMemoryThreshold: Int,        //内存缓存的阈值，当数据量超过这个值时，数据会溢写到磁盘
    spillThreshold: Int,           //溢写的阈值，决定何时将数据写入磁盘
    numOutputRows: SQLMetric,
    spillSize: SQLMetric,
    onlyBufferFirstMatchedRow: Boolean)  //指定是否仅缓冲第一次匹配的行（对于某些连接类型，例如左半连接或反连接，可能会用到）
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new SortMergeJoinEvaluator

  private class SortMergeJoinEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    //用于清理资源，特别是当连接完成时
    private def cleanupResources(): Unit = {
      IndexedSeq(left, right).foreach(_.cleanupResources())
    }
    //分别创建用于生成左侧和右侧连接键的投影（Projection）
    private def createLeftKeyGenerator(): Projection =
      UnsafeProjection.create(leftKeys, left.output)

    private def createRightKeyGenerator(): Projection =
      UnsafeProjection.create(rightKeys, right.output)
    //接受分区索引和两个迭代器（leftIter 和 rightIter），分别表示左侧和右侧数据集的行。它返回一个迭代器，生成连接后的行
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 2)
      val leftIter = inputs(0)
      val rightIter = inputs(1)
      //处理连接条件。如果有提供条件表达式，它会将条件绑定到左侧和右侧输出的列上。否则，返回 true，表示没有条件过滤
      val boundCondition: InternalRow => Boolean = {
        condition.map { cond =>
          Predicate.create(cond, left.output ++ right.output).eval _
        }.getOrElse {
          (r: InternalRow) => true
        }
      }

      // An ordering that can be used to compare keys from both sides.
      //如何比较连接键（即左侧和右侧的排序），这里使用的是自然升序排序
      val keyOrdering = RowOrdering.createNaturalAscendingOrdering(leftKeys.map(_.dataType))

      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

      joinType match {
        case _: InnerLike =>  //该连接类型返回两边都匹配的行
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _   //保存当前处理的左侧数据行
            private[this] var currentRightMatches: ExternalAppendOnlyUnsafeRowArray = _  //保存当前与左侧行匹配的右侧行数组
            private[this] var rightMatchesIterator: Iterator[UnsafeRow] = null  //保存右侧匹配行的迭代器，用于遍历右侧的匹配项
            private[this] val smjScanner = new SortMergeJoinScanner(  //执行排序合并连接核心逻辑的类，它负责扫描左侧和右侧的数据行并找出符合连接条件的匹配行
              createLeftKeyGenerator(),   //生成左侧和右侧连接键的投影（Projection）
              createRightKeyGenerator(),
              keyOrdering,  //指定连接键的排序规则
              RowIterator.fromScala(leftIter),  //将 Scala 的迭代器转换为 RowIterator，用于遍历左侧和右侧的行
              RowIterator.fromScala(rightIter),
              inMemoryThreshold, //这些参数控制内存溢写的阈值
              spillThreshold,
              spillSize,
              cleanupResources)
            private[this] val joinRow = new JoinedRow   //用来存储左侧和右侧匹配的连接行

            if (smjScanner.findNextInnerJoinRows()) { //查找下一组内连接匹配行
              currentRightMatches = smjScanner.getBufferedMatches  //返回右侧匹配的行
              currentLeftRow = smjScanner.getStreamedRow  //返回左侧的当前行
              rightMatchesIterator = currentRightMatches.generateIterator()  //生成右侧匹配行的迭代器
            }

            override def advanceNext(): Boolean = {
              while (rightMatchesIterator != null) {  //只要右侧匹配行迭代器不为空
                if (!rightMatchesIterator.hasNext) { //如果右侧匹配行已经没有剩余的行
                  if (smjScanner.findNextInnerJoinRows()) {
                    currentRightMatches = smjScanner.getBufferedMatches
                    currentLeftRow = smjScanner.getStreamedRow
                    rightMatchesIterator = currentRightMatches.generateIterator()
                  } else {
                    currentRightMatches = null
                    currentLeftRow = null
                    rightMatchesIterator = null
                    return false
                  }
                }
                joinRow(currentLeftRow, rightMatchesIterator.next())
                if (boundCondition(joinRow)) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            override def getRow: InternalRow = resultProj(joinRow)
          }.toScala

        case LeftOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createLeftKeyGenerator(),
            bufferedKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(leftIter),
            bufferedIter = RowIterator.fromScala(rightIter),
            inMemoryThreshold,
            spillThreshold,
            spillSize,
            cleanupResources)
          val rightNullRow = new GenericInternalRow(right.output.length)
          new LeftOuterIterator(
            smjScanner,
            rightNullRow,
            boundCondition,
            resultProj,
            numOutputRows).toScala

        case RightOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createRightKeyGenerator(),
            bufferedKeyGenerator = createLeftKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(rightIter),
            bufferedIter = RowIterator.fromScala(leftIter),
            inMemoryThreshold,
            spillThreshold,
            spillSize,
            cleanupResources)
          val leftNullRow = new GenericInternalRow(left.output.length)
          new RightOuterIterator(
            smjScanner,
            leftNullRow,
            boundCondition,
            resultProj,
            numOutputRows).toScala

        case FullOuter =>
          val leftNullRow = new GenericInternalRow(left.output.length)
          val rightNullRow = new GenericInternalRow(right.output.length)
          val smjScanner = new SortMergeFullOuterJoinScanner(
            leftKeyGenerator = createLeftKeyGenerator(),
            rightKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            leftIter = RowIterator.fromScala(leftIter),
            rightIter = RowIterator.fromScala(rightIter),
            boundCondition,
            leftNullRow,
            rightNullRow)

          new FullOuterIterator(smjScanner, resultProj, numOutputRows).toScala

        case LeftSemi =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              spillSize,
              cleanupResources,
              onlyBufferFirstMatchedRow)
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextInnerJoinRows()) {
                val currentRightMatches = smjScanner.getBufferedMatches
                currentLeftRow = smjScanner.getStreamedRow
                if (currentRightMatches != null && currentRightMatches.length > 0) {
                  val rightMatchesIterator = currentRightMatches.generateIterator()
                  while (rightMatchesIterator.hasNext) {
                    joinRow(currentLeftRow, rightMatchesIterator.next())
                    if (boundCondition(joinRow)) {
                      numOutputRows += 1
                      return true
                    }
                  }
                }
              }
              false
            }

            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case LeftAnti =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              spillSize,
              cleanupResources,
              onlyBufferFirstMatchedRow)
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) {
                currentLeftRow = smjScanner.getStreamedRow
                val currentRightMatches = smjScanner.getBufferedMatches
                if (currentRightMatches == null || currentRightMatches.length == 0) {
                  numOutputRows += 1
                  return true
                }
                var found = false
                val rightMatchesIterator = currentRightMatches.generateIterator()
                while (!found && rightMatchesIterator.hasNext) {
                  joinRow(currentLeftRow, rightMatchesIterator.next())
                  if (boundCondition(joinRow)) {
                    found = true
                  }
                }
                if (!found) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case j: ExistenceJoin =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val result: InternalRow = new GenericInternalRow(Array[Any](null))
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              spillSize,
              cleanupResources,
              onlyBufferFirstMatchedRow)
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) {
                currentLeftRow = smjScanner.getStreamedRow
                val currentRightMatches = smjScanner.getBufferedMatches
                var found = false
                if (currentRightMatches != null && currentRightMatches.length > 0) {
                  val rightMatchesIterator = currentRightMatches.generateIterator()
                  while (!found && rightMatchesIterator.hasNext) {
                    joinRow(currentLeftRow, rightMatchesIterator.next())
                    if (boundCondition(joinRow)) {
                      found = true
                    }
                  }
                }
                result.setBoolean(0, found)
                numOutputRows += 1
                return true
              }
              false
            }

            override def getRow: InternalRow = resultProj(joinRow(currentLeftRow, result))
          }.toScala

        case x =>
          throw new IllegalArgumentException(s"SortMergeJoin should not take $x as the JoinType")
      }

    }
  }
}
