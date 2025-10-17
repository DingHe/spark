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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;

/**
 * A physical representation of a data source scan for batch queries. This interface is used to
 * provide physical information, like how many partitions the scanned data has, and how to read
 * records from the partitions.
 *
 * @since 3.0.0
 */
// Spark DataSource V2 API 中对批处理查询的物理执行的表示
// 由逻辑 Scan 接口通过 toBatch() 方法生成的，用于指导 Spark 如何在集群上实际执行数据读取任务
// 定义物理分片： 负责将整个数据集定义为一组可并行处理的输入分区（InputPartitions）。每个 InputPartition 对应一个 Spark 任务，决定了 RDD 分区的数量
@Evolving
public interface Batch {

  /**
   * Returns a list of {@link InputPartition input partitions}. Each {@link InputPartition}
   * represents a data split that can be processed by one Spark task. The number of input
   * partitions returned here is the same as the number of RDD partitions this scan outputs.
   * <p>
   * If the {@link Scan} supports filter pushdown, this Batch is likely configured with a filter
   * and is responsible for creating splits for that filter, which is not a full scan.
   * <p>
   * This method will be called only once during a data source scan, to launch one Spark job.
   */
  // 规划输入分区
  // 每个 InputPartition 代表数据集的一个物理分片（Split），它将被分配给一个 Spark 任务（Task）进行处理
  InputPartition[] planInputPartitions();

  /**
   * Returns a factory to create a {@link PartitionReader} for each {@link InputPartition}.
   */
  // 创建读取器工厂。
  // 返回一个 PartitionReaderFactory 实例。
  // 这个工厂对象会被序列化并发送到集群的各个执行器（Executor）上。
  // 在执行器上，该工厂会根据传入的 InputPartition 实例，创建出真正负责从底层存储（如 HDFS、S3）读取数据的 PartitionReader 实例
  PartitionReaderFactory createReaderFactory();
}
