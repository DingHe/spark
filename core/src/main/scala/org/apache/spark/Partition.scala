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

package org.apache.spark

/**
 * An identifier for a partition in an RDD.
 */
// 在 Spark 中，Partition（分区）是一个核心抽象概念，它代表了 RDD（弹性分布式数据集）中一个逻辑上不可再分的、最小的数据子集。
// Partition Trait 本身不存储实际数据，而是作为数据的逻辑指针和标识符
// 逻辑划分： 为 RDD 提供逻辑上的划分依据。一个 RDD 由一个或多个 Partition 组成，每个 Partition 可以在集群的一个节点上独立计算。
// 唯一标识： 通过其 index 属性，提供分区在其父 RDD 中的唯一顺序标识，这对于调度和结果收集至关重要。
// 调度单位： 每个 Partition 对应一个 Spark Task（任务）的执行单元。Spark 调度器根据 RDD 的分区列表来决定需要创建多少个任务，以及这些任务应该在哪里执行（如果存在数据本地性信息）。
// 可序列化： 继承了 Serializable 接口，意味着 Partition 对象可以在 JVM 之间传输（例如，从 Driver 传输到 Executor），这是分布式计算的基础要求。
// 简而言之，Partition 就是 “一份数据在哪里” 和 “如何识别这份数据” 的抽象定义。
trait Partition extends Serializable {
  /**
   * Get the partition's index within its parent RDD
   */
  // 分区索引/标识符
  // 代表该分区在其所属的 RDD 中的基于 0 的序号。它是分区的唯一标识，Spark 任务调度和结果重组都依赖这个索引。所有实现 Partition Trait 的类都必须提供一个具体的实现
  def index: Int

  // A better default implementation of HashCode
  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = super.equals(other)
}
