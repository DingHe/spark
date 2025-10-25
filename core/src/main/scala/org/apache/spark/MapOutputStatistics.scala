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
 * Holds statistics about the output sizes in a map stage. May become a DeveloperApi in the future.
 *
 * @param shuffleId ID of the shuffle
 * @param bytesByPartitionId approximate number of output bytes for each map output partition
 *   (may be inexact due to use of compressed map statuses)
 */
//  Spark 中用于存储和传递一个 Shuffle 阶段（即 Map 阶段）输出结果统计信息的数据结构
// 在 Spark 的 Shuffle 机制中，Map 任务执行完毕后，需要记录每个 Map 任务对每个目标 Reduce 分区写入了多少字节的数据。这些信息被称为 Map 输出统计信息。
// 记录数据分布： 精确记录数据在 Map 阶段结束后，按照 Shuffle 分区键分配到每个目标 Reduce 分区的近似大小。
// 指导调度和优化： 这些统计信息是 Spark 调度器分配 Reduce 任务资源和 Spark SQL 自适应查询执行 (AQE) 进行运行时优化的关键依据（例如，AQE 使用这些信息来决定是否可以合并小分区）
// bytesByPartitionId 按分区字节数数组。 这是一个 Long 类型的数组，其中每个元素代表一个目标 Reduce 分区（即 partitionId）接收到的总字节数。
// 这个数组的长度等于 Shuffle 的目标分区数 (numPartitions)
private[spark] class MapOutputStatistics(val shuffleId: Int, val bytesByPartitionId: Array[Long])
