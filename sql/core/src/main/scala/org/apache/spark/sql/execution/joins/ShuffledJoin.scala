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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, LeftExistence, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning, PartitioningCollection, UnknownPartitioning, UnspecifiedDistribution}

/**
 * Holds common logic for join operators by shuffling two child relations
 * using the join keys.
 */
// 定义了 Apache Spark SQL 内部执行计划中的一个特质（Trait），名为 ShuffledJoin。
// 它本身不是一个可直接实例化的类，而是一个混合了通用逻辑的接口，用于所有需要通过 Shuffle 机制将两个子关系（数据源）重新分配到不同执行节点上，然后才能进行连接操作的执行计划节点
// 进行 Join 操作时，如果左右两侧的数据没有按照 Join 键均匀地分布在相同的分区上，就需要进行数据重分布（即 Shuffle）。
// ShuffledJoin 就封装了这种操作所需的分区策略、数据分布要求和倾斜（Skew）处理逻辑
// 定义数据重分布（Shuffle）要求： 规定了左右子查询在执行 Join 前，必须按照 Join 键进行哈希聚簇分布（ClusteredDistribution）
// 处理数据倾斜： 提供了 isSkewJoin 标志和相应的逻辑，用于在检测到数据倾斜时调整分区策略，以提高性能
// 确定输出模式： 根据不同的 Join 类型（Inner、Left Outer、Full Outer 等）确定最终结果集的列（output）和它们的可空性
trait ShuffledJoin extends JoinCodegenSupport {
  // 指示是否为数据倾斜连接。
  // 具体的实现类（如 SortMergeJoin 或 ShuffledHashJoin 的变体）会根据运行时或优化器的分析结果来设置这个布尔值，以确定是否需要启用倾斜处理逻辑
  def isSkewJoin: Boolean
  // 覆写了父类的方法。如果 isSkewJoin 为 true，则会在节点名称后追加 (skew=true)，方便调试和在执行计划中识别倾斜连接
  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }
  // 返回用于节点描述的参数
  override def stringArgs: Iterator[Any] = super.stringArgs.toSeq.dropRight(1).iterator
  // 定义左右子查询所需的数据分布策略
  // 要求左右子查询的数据在进行 Join 前必须进行 Shuffle
  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) {
      // 倾斜时： 返回 UnspecifiedDistribution。
      // 这是因为 Spark 在处理倾斜时会使用更复杂的分区重排策略，此时无法用标准的 ClusteredDistribution 来描述，所以标记为“未指定”
      // We re-arrange the shuffle partitions to deal with skew join, and the new children
      // partitioning doesn't satisfy `HashClusteredDistribution`.
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil  //表示此时的数据分布无法预先确定
    } else {
      // 非倾斜时： 要求左右两侧都使用 ClusteredDistribution，即数据必须按照 leftKeys 和 rightKeys 进行哈希分区，确保具有相同 Join 键的行被分配到同一个执行分区上
      //ClusteredDistribution按照指定的键进行数据分布
      //默认采用HashPartitioning分区方式
      ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
    }
  }
  // 定义 Join 结果的输出分区策略。
  override def outputPartitioning: Partitioning = joinType match {
    // 继承自左右子查询的分区，由于 Join 键已对齐，结果仍保持聚簇性。
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    // 保持与左子查询 (left.outputPartitioning) 相同的分区策略
    case LeftOuter => left.outputPartitioning
    // 保持与右子查询 (right.outputPartitioning) 相同的分区策略
    case RightOuter => right.outputPartitioning
    // Full Outer Join 可能在没有匹配的情况下产生额外的 Null 行，打乱原有的分区顺序和边界
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    // 保持与左子查询 (left.outputPartitioning) 相同的分区策略
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"ShuffledJoin should not take $x as the JoinType")
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }
}
