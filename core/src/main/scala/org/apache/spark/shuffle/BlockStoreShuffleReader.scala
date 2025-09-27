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

import scala.collection

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Fetches and reads the blocks from a shuffle by requesting them from other nodes' block stores.
 */
// Spark 中 Reduce Task 读取 Shuffle 数据的核心实现类。它的主要作用是：
// 调度和获取数据块： 根据 Map Task 的输出位置信息（由 MapOutputTracker 提供），负责向远程或本地的 BlockManager 发送请求，获取所需的 Shuffle 数据块
// 数据流处理： 将获取到的原始输入流进行处理，包括解压缩、解密（如果启用）和反序列化，将其还原为键值对记录
// 核心业务逻辑应用： 如果 Shuffle 依赖中定义了 聚合器（Aggregator） 或 排序规则（Ordering），它会在读取数据后，执行最终的 Combine（聚合）或 Sort（排序）操作
//简而言之，它是一个高性能、可配置的 “数据获取、反序列化和后处理” 管道，是连接 Map Stage 和 Reduce Stage 的桥梁
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],// 包含 Shuffle 的元信息，最重要的是其 依赖关系 (dependency)，由此可获取序列化器、分区器、聚合器和排序规则等
    blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])], //包含 Reduce Task 需要读取的所有 Shuffle 数据块的信息。每个元素是三元组：(BlockManagerId, Seq[(BlockId, Long, Int)])，即数据块所在的执行器 ID，以及该执行器上存储的一系列数据块 ID、长度和 Map 索引
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager, //负责管理序列化和反序列化操作，以及处理 I/O 加密（如果启用）的流封装
    blockManager: BlockManager = SparkEnv.get.blockManager, // 负责本地存储和客户端通信。用于获取 blockStoreClient 来发起远程数据块请求
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker, // 获取 Map 输出位置信息。尽管信息已通过 blocksByAddress 传入，但在 ShuffleBlockFetcherIterator 内部仍可能用到
    shouldBatchFetch: Boolean = false) //指示是否尝试使用批量抓取（Batch Fetch）优化，即一次网络请求获取多个连续的 Shuffle 块，以提高网络效率
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency
  //检查是否满足连续块批量抓取（Batch Fetch） 的所有条件
  private def fetchContinuousBlocksInBatch: Boolean = {
    val conf = SparkEnv.get.conf
    // 序列化器是否支持对象重定位 (serializerRelocatable)
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    //压缩是否启用，以及压缩编解码器是否支持串联
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(CompressionCodec.createCodec(conf))
    } else {
      true
    }
    //是否使用旧的获取协议
    val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)
    // SPARK-34790: Fetching continuous blocks in batch is incompatible with io encryption.
    //I/O 加密是否启用 (ioEncryption)
    val ioEncryption = conf.get(config.IO_ENCRYPTION_ENABLED)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol && !ioEncryption
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug("The feature tag of continuous shuffle block fetching is set to true, but " +
        "we can not enable the feature because other conditions are not satisfied. " +
        s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
        s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
        s"$useOldFetchProtocol, io encryption: $ioEncryption.")
    }
    doBatchFetch
  }

  /** Read the combined key-values for this reduce task */
  // 建立了从远程存储获取原始 Shuffle 数据，到在本地执行反序列化、聚合、排序并最终返回结果的完整数据处理管道，
  override def read(): Iterator[Product2[K, C]] = {
    // 构造一个迭代器，负责向远程 BlockManager 实际发送请求获取 Shuffle 块，并返回一个包含原始输入流的迭代器。它将读取器的所有配置（包括批量抓取、流控、防腐败检测等）传递给这个获取器
    // 实例化 ShuffleBlockFetcherIterator。这是负责通过网络（通常是 Netty）向其他执行器请求 Shuffle 数据块的组件
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      mapOutputTracker,
      blocksByAddress,
      serializerManager.wrapStream,// 传入一个函数，用于封装接收到的原始输入流。这个封装通常用于处理 I/O 加密和解密
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      // 从配置中获取并设置同时在网络传输中（in flight）的块数据的最大总大小（以字节为单位），用于网络流控
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      //从配置中获取并设置针对单个远程执行器，允许同时抓取的最大块数量，防止单个执行器负载过高
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      //设置当 Netty 发生 OOM 错误时，尝试重试的最大次数。
      SparkEnv.get.conf.get(config.SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ENABLED),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM),
      readMetrics,
      fetchContinuousBlocksInBatch).toCompletionIterator

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    //开始反序列化记录
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      //将流转换为键值迭代器
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    //构造一个 CompletionIterator，它的目的是在底层迭代器（recordIter）耗尽时执行收尾操作
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      //在 recordIter 外部包裹一个 map 操作，每读取一条记录就调用 readMetrics.incRecordsRead(1) 递增记录数
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    //使用 InterruptibleIterator 包装，将 TaskContext 关联到迭代器上。这确保了如果 Task 被 Driver 取消，正在执行 next() 的线程会被中断，实现任务取消
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    //判断 Shuffle 依赖中是否定义了聚合器 (dep.aggregator.isDefined)
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      // 如果 Map 端已经执行了局部聚合
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        // 则 Reduce 端需要调用聚合器的 combineCombinersByKey 来合并来自不同 Map Task 的局部聚合结果
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        //Reduce 端需要调用聚合器的 combineValuesByKey 对所有原始值进行首次聚合。
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      // 如果没有定义聚合器，则直接使用可中断的记录迭代器
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter: Iterator[Product2[K, C]] = dep.keyOrdering match {
      //判断 Shuffle 依赖中是否定义了键排序规则
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        //创建外部排序器
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAllAndUpdateMetrics(aggregatedIter)
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}
