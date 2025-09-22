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

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus

/**
 * The interface for customizing shuffle write process. The driver create a ShuffleWriteProcessor
 * and put it into [[ShuffleDependency]], and executors use it in each ShuffleMapTask.
 */
// 用于定制 Shuffle 写入过程的接口。它允许开发者或 Spark 内部的优化组件对 ShuffleMapTask 的写出行为进行精细控制
// 在 ShuffleMapTask 中，实际的计算逻辑由 ShuffleWriteProcessor 的 write 方法来执行。
// 这个设计模式使得 Spark 框架可以将核心的 Shuffle 写出逻辑与具体的 Shuffle 实现（如 SortShuffleWriter、UnsafeShuffleWriter 等）解耦
// 职责是：
// 创建和管理一个 ShuffleWriter 的生命周期
// 触发 RDD 分区的计算
// 驱动 ShuffleWriter 将计算结果写入磁盘
// 处理与新特性相关的逻辑，例如推送式 Shuffle（Push-based shuffle）
// 在任务成功或失败时，停止 ShuffleWriter 并返回最终状态
private[spark] class ShuffleWriteProcessor extends Serializable with Logging {

  /**
   * Create a [[ShuffleWriteMetricsReporter]] from the task context. As the reporter is a
   * per-row operator, here need a careful consideration on performance.
   */
  // 度量报告器用于在 Shuffle 写入过程中记录度量信息，如写入字节数、写入时间等
  protected def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter = {
    context.taskMetrics().shuffleWriteMetrics
  }

  /**
   * The write process for particular partition, it controls the life circle of [[ShuffleWriter]]
   * get from [[ShuffleManager]] and triggers rdd compute, finally return the [[MapStatus]] for
   * this task.
   */
  def write(
      rdd: RDD[_], // 需要进行 Shuffle 计算的 RDD
      dep: ShuffleDependency[_, _, _], // ShuffleDependency 对象，包含了所有 Shuffle 相关的配置
      mapId: Long, // Map 任务的唯一 ID
      context: TaskContext,
      partition: Partition): MapStatus = { // 任务要处理的 RDD 分区

    // 声明一个 ShuffleWriter 变量，用于执行实际的写入操作

    var writer: ShuffleWriter[Any, Any] = null
    try {
      // 获取 Spark 环境中的 ShuffleManager 实例
      val manager = SparkEnv.get.shuffleManager
      // 创建一个 ShuffleWriter 实例
      writer = manager.getWriter[Any, Any](
        dep.shuffleHandle,
        mapId,
        context,
        createMetricsReporter(context))
      //触发 RDD 计算
      writer.write(
        rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])

      // 当写入成功后，调用 writer.stop(true) 方法。
      // writer 会在这里关闭文件、清理资源，并返回一个 MapStatus 对象。
      // 这个 MapStatus 包含了 Map 输出文件的元数据（如块大小和位置），它会被发送回驱动程序

      val mapStatus = writer.stop(success = true)
      if (mapStatus.isDefined) {
        // Check if sufficient shuffle mergers are available now for the ShuffleMapTask to push
        // 这两段代码处理推送式 Shuffle 的逻辑。
        // 如果该功能启用，它会检查并获取 Shuffle 合并服务的位置，然后调用 ShuffleBlockPusher 来异步地将 Shuffle 块推送到这些合并服务
        if (dep.shuffleMergeAllowed && dep.getMergerLocs.isEmpty) {
          val mapOutputTracker = SparkEnv.get.mapOutputTracker
          val mergerLocs =
            mapOutputTracker.getShufflePushMergerLocations(dep.shuffleId)
          if (mergerLocs.nonEmpty) {
            dep.setMergerLocs(mergerLocs)
          }
        }
        // Initiate shuffle push process if push based shuffle is enabled
        // The map task only takes care of converting the shuffle data file into multiple
        // block push requests. It delegates pushing the blocks to a different thread-pool -
        // ShuffleBlockPusher.BLOCK_PUSHER_POOL.
        if (!dep.shuffleMergeFinalized) {
          manager.shuffleBlockResolver match {
            case resolver: IndexShuffleBlockResolver =>
              logInfo(s"Shuffle merge enabled with ${dep.getMergerLocs.size} merger locations " +
                s" for stage ${context.stageId()} with shuffle ID ${dep.shuffleId}")
              logDebug(s"Starting pushing blocks for the task ${context.taskAttemptId()}")
              val dataFile = resolver.getDataFile(dep.shuffleId, mapId)
              new ShuffleBlockPusher(SparkEnv.get.conf)
                .initiateBlockPush(dataFile, writer.getPartitionLengths(), dep, partition.index)
            case _ =>
          }
        }
      }
      mapStatus.get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
}
