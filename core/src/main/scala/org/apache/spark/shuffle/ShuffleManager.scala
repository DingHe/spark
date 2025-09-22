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

package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE:
 * 1. This will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 * 2. This contains a method ShuffleBlockResolver which interacts with External Shuffle Service
 * when it is enabled. Need to pay attention to that, if implementing a custom ShuffleManager, to
 * make sure the custom ShuffleManager could co-exist with External Shuffle Service.
 */
private[spark] trait ShuffleManager {

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */

  // 用于在 ShuffleManager 中注册一个新的 shuffle 操作。注册时，
  // 需要提供一个 shuffleId （唯一标识该 Shuffle 操作的 ID）和一个 ShuffleDependency （描述 Shuffle 依赖关系的对象）。
  // 注册完成后，返回一个 ShuffleHandle，它是后续任务访问 Shuffle 数据的句柄

  def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks. */
  // 用于获取一个写入器 ShuffleWriter，以便在执行 map 任务时将数据写入到 shuffle 中。
  // 它会返回一个 ShuffleWriter，用于将分区数据写入磁盘或其他存储
  def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V]


  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from all map outputs of the shuffle.
   * 用于获取一个读取器 ShuffleReader，用于 reduce 任务从 shuffle 中读取指定范围的分区数据。它允许读取从 startPartition 到 endPartition-1 的分区数据
   * Called on executors by reduce tasks.
   */
  final def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    getReader(handle, 0, Int.MaxValue, startPartition, endPartition, context, metrics)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from a range of map outputs(startMapIndex to endMapIndex-1, inclusive).
   * If endMapIndex=Int.MaxValue, the actual endMapIndex will be changed to the length of total map
   * outputs of the shuffle in `getMapSizesByExecutorId`.
   *
   * Called on executors by reduce tasks.
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   * @return true if the metadata removed successfully, otherwise false.
   */
  //该方法用于注销一个已注册的 shuffle 操作。
  // 提供 shuffleId 后，它会将对应的 shuffle 元数据从 ShuffleManager 中删除。如果成功删除，返回 true，否则返回 false
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   */
  //性返回一个 ShuffleBlockResolver，它用于根据给定的 block 坐标（如 shuffle ID 和分区 ID）获取 Shuffle 数据块。
  // 对于启用了外部 Shuffle 服务的情况，ShuffleBlockResolver 可以与外部 Shuffle 服务交互来获取数据
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}
