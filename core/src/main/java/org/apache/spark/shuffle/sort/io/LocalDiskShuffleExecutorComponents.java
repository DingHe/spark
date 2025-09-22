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

package org.apache.spark.shuffle.sort.io;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.storage.BlockManager;

// 用于本地磁盘 Shuffle 的执行器端组件。
// 它的主要作用是提供并管理与 Shuffle 写入相关的核心对象，特别是创建用于写入 Shuffle Map 输出文件的写入器（Writer）
// 扮演了一个工厂的角色。它根据不同的 Shuffle 类型（如本地磁盘、外部服务等）提供不同的实现
public class LocalDiskShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  // 块管理器实例。它负责管理执行器上的本地存储，包括内存和磁盘，Shuffle 写入器会使用它来获取本地磁盘路径
  private BlockManager blockManager;
  // 块解析器。它负责处理 Shuffle 文件，特别是管理数据文件 (.data) 和索引文件 (.index) 的创建、查找和清理
  private IndexShuffleBlockResolver blockResolver;

  public LocalDiskShuffleExecutorComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @VisibleForTesting
  public LocalDiskShuffleExecutorComponents(
      SparkConf sparkConf,
      BlockManager blockManager,
      IndexShuffleBlockResolver blockResolver) {
    this.sparkConf = sparkConf;
    this.blockManager = blockManager;
    this.blockResolver = blockResolver;
  }
  // 这个方法在执行器启动时被 SortShuffleManager 调用一次，用于准备环境
  @Override
  public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) {
    blockManager = SparkEnv.get().blockManager();
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    // 这个解析器是本地磁盘 Shuffle 的核心组件
    blockResolver =
      new IndexShuffleBlockResolver(
        sparkConf, blockManager, Collections.emptyMap() /* Shouldn't be accessed */
      );
  }
  // 创建并返回一个用于多文件 Shuffle 写入的写入器。这个方法在每个 ShuffleMapTask 启动时被 SortShuffleWriter 调用
  @Override
  public ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      long mapTaskId,
      int numPartitions) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return new LocalDiskShuffleMapOutputWriter(
        shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf);
  }
  // 创建并返回一个用于单文件 Shuffle 写入的写入器。
  // 这个方法用于旁路归并排序（Bypass Merge Sort Shuffle）等特殊场景。在这种场景下，所有分区数据都写入到一个文件中
  @Override
  public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver));
  }
}
