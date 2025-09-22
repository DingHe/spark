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

package org.apache.spark.shuffle.api;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * An interface for building shuffle support for Executors.
 * @since 3.0.0
 */
// 主要用于在 Executor 上处理 Shuffle 操作的输入输出，特别是与 Map 任务的 Shuffle 数据写入相关的部分
@Private
public interface ShuffleExecutorComponents {

  /**
   * Called once per executor to bootstrap this module with state that is specific to
   * that executor, specifically the application ID and executor ID.
   *
   * @param appId The Spark application id  Spark 应用的 ID，用于标识当前的应用程序
   * @param execId The unique identifier of the executor being initialized  当前 Executor 的唯一标识符
   * @param extraConfigs Extra configs that were returned by
   *                     {@link ShuffleDriverComponents#initializeApplication()}
   */
  // 每个 Executor 在启动时都会调用该方法进行初始化，主要是为 Executor 配置应用程序 ID、执行器 ID 以及额外的配置
  void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs);

  /**
   * Called once per map task to create a writer that will be responsible for persisting all the
   * partitioned bytes written by that map task.
   *
   * @param shuffleId Unique identifier for the shuffle the map task is a part of  唯一的 Shuffle 标识符，用于标识当前的 Shuffle 操作
   * @param mapTaskId An ID of the map task. The ID is unique within this Spark application. 当前 Map 任务的 ID，在一个 Spark 应用中是唯一的
   * @param numPartitions The number of partitions that will be written by the map task. Some of
   *                      these partitions may be empty.  该 Map 任务将写入的分区数，可能有部分分区为空
   */
  // 为每个 Map 任务创建一个写入器（ShuffleMapOutputWriter），负责将该 Map 任务的分区数据写入 Shuffle 存储
  ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      long mapTaskId,
      int numPartitions) throws IOException;

  /**
   * An optional extension for creating a map output writer that can optimize the transfer of a
   * single partition file, as the entire result of a map task, to the backing store.
   * <p>
   * Most implementations should return the default {@link Optional#empty()} to indicate that
   * they do not support this optimization. This primarily is for backwards-compatibility in
   * preserving an optimization in the local disk shuffle storage implementation.
   *
   *  返回值是一个 Optional，如果实现支持该优化，则返回一个 SingleSpillShuffleMapOutputWriter，否则返回 Optional.empty()
   * @param shuffleId Unique identifier for the shuffle the map task is a part of
   * @param mapId An ID of the map task. The ID is unique within this Spark application.
   */
  // 这是一个可选的扩展方法，用于优化某些 Map 任务的输出数据，特别是将所有分区数据作为一个单一文件写入存储（而不是多个分区文件）。
  default Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) throws IOException {
    return Optional.empty();
  }
}
