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

package org.apache.spark.storage

import java.io.{InputStream, IOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CheckedInputStream
import javax.annotation.concurrent.GuardedBy

import scala.collection
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.util.{Failure, Success}

import io.netty.util.internal.OutOfDirectMemoryError
import org.apache.commons.io.IOUtils
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.{MapOutputTracker, SparkException, TaskContext}
import org.apache.spark.MapOutputTracker.SHUFFLE_PUSH_MAP_ID
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.shuffle.checksum.{Cause, ShuffleChecksumHelper}
import org.apache.spark.network.util.{NettyUtils, TransportConf}
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.util.{Clock, CompletionIterator, SystemClock, TaskCompletionListener, Utils}

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[BlockStoreClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require two info: 1. the size (in bytes as a long
 *                        field) in order to throttle the memory usage; 2. the mapIndex for this
 *                        block, which indicate the index in the map stage.
 *                        Note that zero-sized blocks are already excluded, which happened in
 *                        [[org.apache.spark.MapOutputTracker.convertMapStatuses]].
 * @param mapOutputTracker [[MapOutputTracker]] for falling back to fetching the original blocks if
 *                         we fail to fetch shuffle chunks when push based shuffle is enabled.
 * @param streamWrapper A function to wrap the returned input stream.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
 * @param maxAttemptsOnNettyOOM The max number of a block could retry due to Netty OOM before
 *                              throwing the fetch failure.
 * @param detectCorrupt         whether to detect any corruption in fetched blocks.
 * @param checksumEnabled whether the shuffle checksum is enabled. When enabled, Spark will try to
 *                        diagnose the cause of the block corruption.
 * @param checksumAlgorithm the checksum algorithm that is used when calculating the checksum value
 *                         for the block data.
 * @param shuffleMetrics used to report shuffle metrics.
 * @param doBatchFetch fetch continuous shuffle blocks from same executor in batch if the server
 *                     side supports.
 */
//  Reduce Task 用于从 Map Task 输出中高效、并行、受控地抓取（Fetch）Shuffle 数据块 的迭代器
// 数据抓取管道化 (Pipelining): 它将网络数据传输（异步操作）转换为一个同步的 Scala 迭代器。调用者可以通过迭代器连续获取 (BlockId, InputStream) 对，从而在接收到下一个块的同时处理前一个块，实现数据流的管道化
// 流量控制 (Throttling): 它严格限制同时在网络中传输的远程数据块总大小 (maxBytesInFlight)、并发请求数 (maxReqsInFlight) 和针对单个执行器的并发块数 (maxBlocksInFlightPerAddress)，以防止 Reduce Task 节点因同时拉取过多数据而导致内存溢出（OOM）
// 本地/远程块统一处理: 它能智能区分要获取的块是本地（同一执行器）还是远程（其他执行器），并使用不同的机制进行获取
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: BlockStoreClient, //用于向远程执行器发起网络请求，抓取远程 Shuffle 数据块
    blockManager: BlockManager,  //用于读取本地（同一执行器）存储的 Shuffle 数据块
    mapOutputTracker: MapOutputTracker, //用于在 Push-Based Shuffle 模式下，如果抓取块失败，可以回退（fallback）到抓取原始块的场景
    //Reduce Task 需要抓取的所有 Shuffle 块信息，按存储它们的 BlockManagerId 分组，包含 BlockId、大小（用于流控）和 Map 索引
    blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
    //用于对获取到的原始输入流进行封装处理，主要用于I/O 加密/解密和压缩/解压
    streamWrapper: (BlockId, InputStream) => InputStream,
    //流量控制核心。限制网络中同时传输的远程数据块总大小（字节）
    maxBytesInFlight: Long,
    //限制同时发出的远程网络请求的最大数量
    maxReqsInFlight: Int,
    //限制针对单个远程执行器同时抓取的最大块数量
    maxBlocksInFlightPerAddress: Int,
    //远程请求的总大小若超过此值，则会将数据先下载到磁盘（DownloadFile），否则直接下载到内存
    val maxReqSizeShuffleToMem: Long,
    //当 Netty 因内存不足失败时，允许重试该块的最大次数
    maxAttemptsOnNettyOOM: Int,
    detectCorrupt: Boolean,
    detectCorruptUseExtraMemory: Boolean,
    checksumEnabled: Boolean,
    checksumAlgorithm: String,
    shuffleMetrics: ShuffleReadMetricsReporter,
    //是否尝试在一次请求中从同一执行器抓取多个连续的 Shuffle 数据块
    doBatchFetch: Boolean,
  clock: Clock = new SystemClock())
  extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
  // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
  // nodes, rather than blocking on reading output from one node.
  //每次远程抓取请求的目标大小，通常是 maxBytesInFlight / 5，以允许更高的并行度
  private val targetRemoteRequestSize = math.max(maxBytesInFlight / 5, 1L)

  /**
   * Total number of blocks to fetch.
   */
  //任务总共需要抓取的 Shuffle 数据块数量
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  //已经由调用者（Reduce Task）处理完毕的块数量。当它等于 numBlocksToFetch 时，迭代器结束。
  private[this] var numBlocksProcessed = 0

  private[this] val startTimeNs = System.nanoTime()

  /** Host local blocks to fetch, excluding zero-sized blocks. */
  //存储需要在本地（同一执行器）获取的 Shuffle 块的 BlockId 和 Map 索引
  private[this] val hostLocalBlocks = scala.collection.mutable.LinkedHashSet[(BlockId, Int)]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  //异步转同步的核心。用于存储已成功获取或失败的 Shuffle 块结果 (FetchResult)。网络抓取线程将结果放入此队列，主线程通过迭代器从队列中取出
  //next 方法从这里取出处理的结果，逐个处理
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
   //记录当前正在被 Reduce Task 处理的块的结果。用于在处理过程中发生异常时，能够释放其关联的内存缓冲区
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
    //存储所有等待发送的远程抓取请求，这些请求将按照流控限制逐步发出
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * Queue of fetch requests which could not be issued the first time they were dequeued. These
   * requests are tried again when the fetch constraints are satisfied.
   */
  //存储因当前的流控限制未满足（例如，bytesInFlight 过高）而被暂时推迟发送的远程抓取请求
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  /** Current bytes in flight from our requests */
  //当前正在网络中传输的远程数据块总大小，用于实施 maxBytesInFlight 限制
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  //当前正在执行的远程网络请求数量，用于实施 maxReqsInFlight 限制
  private[this] var reqsInFlight = 0

  /** Current number of blocks in flight per host:port */
  //记录每个远程执行器当前正在传输的块数量，用于实施 maxBlocksInFlightPerAddress 限制
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
   * Count the retry times for the blocks due to Netty OOM. The block will stop retry if
   * retry times has exceeded the [[maxAttemptsOnNettyOOM]].
   */
  //记录每个块因 Netty OOM 错误而重试的次数，防止无限重试
  private[this] val blockOOMRetryCounts = new HashMap[String, Int]

  /**
   * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
   * at most once for those corrupted blocks.
   */
  //存储已被识别为数据损坏的块 ID，用于避免对同一块进行重复重试
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  //标志迭代器是否已停止或清理。如果为 true，后续网络抓取回调将不再向 results 队列中放置结果
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * A set to store the files used for shuffling remote huge blocks. Files in this set will be
   * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
   */
  //存储因 maxReqSizeShuffleToMem 限制而下载到磁盘的临时文件句柄，用于在清理时删除这些文件
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[DownloadFile]()
  ////当任务完成时执行ShuffleBlockFetcherIterator的清理操作，具体时调用cleanup方法
  private[this] val onCompleteCallback = new ShuffleFetchCompletionListener(this)
  //在 Push-Based Shuffle 模式下，用于协助处理块抓取逻辑，包括获取本地合并块等
  private[this] val pushBasedFetchHelper = new PushBasedFetchHelper(
    this, shuffleClient, blockManager, mapOutputTracker, shuffleMetrics)

  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(transportConf: TransportConf): DownloadFile = {
    // we never need to do any encryption or decryption here, regardless of configs, because that
    // is handled at another layer in the code.  When encryption is enabled, shuffle data is written
    // to disk encrypted in the first place, and sent over the network still encrypted.
    new SimpleDownloadFile(
      blockManager.diskBlockManager.createTempLocalBlock()._2, transportConf)
  }

  override def registerTempFileToClean(file: DownloadFile): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[storage] def cleanup(): Unit = {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(blockId, mapIndex, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            if (pushBasedFetchHelper.isLocalPushMergedBlockAddress(address) ||
              hostLocalBlocks.contains(blockId -> mapIndex)) {
              shuffleMetricsUpdate(blockId, buf, local = true)
            } else {
              shuffleMetricsUpdate(blockId, buf, local = false)
            }
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.path())
      }
    }
  }
 //实际执行远程网络抓取请求的核心方法。它负责向远程 BlockManager 发送请求、更新流量控制状态，并定义了处理请求成功和失败的异步回调逻辑
  private[this] def sendRequest(req: FetchRequest): Unit = {
    //打印调试日志，记录正在发送的请求信息，包括块的数量、总大小以及目标地址
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    reqsInFlight += 1

    // so we can look up the block info of each blockID
    //将请求中包含的每个块 ID 字符串映射到它的 (大小, Map 索引)，方便在异步回调中通过 ID 查找块的详细信息
    val infoMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, mapIndex) => (blockId.toString, (size, mapIndex))
    }.toMap
    //初始化一个集合，包含该请求中所有块的 ID。在异步回调中，每成功获取一个块，就将其从该集合中移除
    val remainingBlocks = new HashSet[String]() ++= infoMap.keys
    //用于在 Netty OOM 失败时，临时存储因失败需要延迟重试的块 ID
    val deferredBlocks = new ArrayBuffer[String]()
    //将请求中的块 ID 转换为字符串列表，这是底层 shuffleClient.fetchBlocks 方法所需的参数格式
    val blockIds = req.blocks.map(_.blockId.toString)
    //提取请求目标地址
    val address = req.address
    val requestStartTime = clock.nanoTime()
    //用于检查是否需要将 Netty OOM 失败的块打包成新的延迟请求
    @inline def enqueueDeferredFetchRequestIfNecessary(): Unit = {
      if (remainingBlocks.isEmpty && deferredBlocks.nonEmpty) {
        val blocks = deferredBlocks.map { blockId =>
          val (size, mapIndex) = infoMap(blockId)
          FetchBlockInfo(BlockId(blockId), size, mapIndex)
        }
        results.put(DeferFetchRequestResult(FetchRequest(address, blocks)))
        deferredBlocks.clear()
      }
    }
    //用于更新网络请求持续时间指标
    @inline def updateMergedReqsDuration(wasReqForMergedChunks: Boolean = false): Unit = {
      //只有当当前请求中的所有块都已处理完毕 (remainingBlocks.isEmpty) 时，才更新指标
      if (remainingBlocks.isEmpty) {
        val durationMs = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - requestStartTime)
        //如果请求包含合并块片段 (ShuffleChunk)，则更新专门的远程合并请求持续时间指标
        if (wasReqForMergedChunks) {
          shuffleMetrics.incRemoteMergedReqsDuration(durationMs)
        }
        shuffleMetrics.incRemoteReqsDuration(durationMs)
      }
    }
    //关键的内部类，它定义了异步网络请求成功和失败时的处理逻辑
    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        ShuffleBlockFetcherIterator.this.synchronized {
          //确保并发安全。同时检查 isZombie 标志，如果迭代器已被清理，则不处理结果
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            //由于 ManagedBuffer 将从网络线程传递给主任务线程，需要调用 retain() 增加引用计数，防止底层内存被提前释放
            buf.retain()
            //从 remainingBlocks 集合中移除已成功获取的块 ID
            remainingBlocks -= blockId
            blockOOMRetryCounts.remove(blockId)
            updateMergedReqsDuration(BlockId(blockId).isShuffleChunk)
            //创建一个 SuccessFetchResult 对象，其中包含块 ID、Map 索引、内存缓冲区等信息，并将其放入阻塞队列 results 中，供主迭代器 (next()) 消费
            results.put(SuccessFetchResult(BlockId(blockId), infoMap(blockId)._2,
              address, infoMap(blockId)._1, buf, remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
            enqueueDeferredFetchRequestIfNecessary()
          }
        }
        logTrace(s"Got remote block $blockId after ${Utils.getUsedTimeNs(startTimeNs)}")
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        ShuffleBlockFetcherIterator.this.synchronized {
          // 记录获取块失败的错误信息
          logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
          e match {
            // SPARK-27991: Catch the Netty OOM and set the flag `isNettyOOMOnShuffle` (shared among
            // tasks) to true as early as possible. The pending fetch requests won't be sent
            // afterwards until the flag is set to false on:
            // 1) the Netty free memory >= maxReqSizeShuffleToMem
            //    - we'll check this whenever there's a fetch request succeeds.
            // 2) the number of in-flight requests becomes 0
            //    - we'll check this in `fetchUpToMaxBytes` whenever it's invoked.
            // Although Netty memory is shared across multiple modules, e.g., shuffle, rpc, the flag
            // only takes effect for the shuffle due to the implementation simplicity concern.
            // And we'll buffer the consecutive block failures caused by the OOM error until there's
            // no remaining blocks in the current request. Then, we'll package these blocks into
            // a same fetch request for the retry later. In this way, instead of creating the fetch
            // request per block, it would help reduce the concurrent connections and data loads
            // pressure at remote server.
            // Note that catching OOM and do something based on it is only a workaround for
            // handling the Netty OOM issue, which is not the best way towards memory management.
            // We can get rid of it when we find a way to manage Netty's memory precisely.
            //处理 Netty OOM 错误：如果异常是 OutOfDirectMemoryError 并且该块的重试次数未达到上限
            case _: OutOfDirectMemoryError
                if blockOOMRetryCounts.getOrElseUpdate(blockId, 0) < maxAttemptsOnNettyOOM =>
              if (!isZombie) {
                val failureTimes = blockOOMRetryCounts(blockId)
                blockOOMRetryCounts(blockId) += 1
                // 核心背压机制：如果 OOM 标志尚未设置，则将其原子性地设置为 true，从而暂停所有其他任务的 Shuffle 抓取请求
                if (isNettyOOMOnShuffle.compareAndSet(false, true)) {
                  // The fetcher can fail remaining blocks in batch for the same error. So we only
                  // log the warning once to avoid flooding the logs.
                  logInfo(s"Block $blockId has failed $failureTimes times " +
                    s"due to Netty OOM, will retry")
                }
                remainingBlocks -= blockId
                deferredBlocks += blockId
                enqueueDeferredFetchRequestIfNecessary()
              }
            //处理非 OOM 错误（如网络错误、文件不存在等）
            //如果失败的是 Push Shuffle 的块片段，则不将本次失败视为致命错误，而是放入 FallbackOnPushMergedFailureResult 结果，指示主迭代器回退（Fallback）到抓取原始块
            case _ =>
              val block = BlockId(blockId)
              if (block.isShuffleChunk) {
                remainingBlocks -= blockId
                updateMergedReqsDuration(wasReqForMergedChunks = true)
                results.put(FallbackOnPushMergedFailureResult(
                  block, address, infoMap(blockId)._1, remainingBlocks.isEmpty))
              } else {
                results.put(FailureFetchResult(block, infoMap(blockId)._2, address, e))
              }
          }
        }
      }
    }

    // Fetch remote shuffle blocks to disk when the request is too large. Since the shuffle data is
    // already encrypted and compressed over the wire(w.r.t. the related configs), we can just fetch
    // the data and write it to file directly.
    if (req.size > maxReqSizeShuffleToMem) {
      // 如果请求的总大小超过了阈值 (maxReqSizeShuffleToMem)，则调用 shuffleClient.fetchBlocks 发起请求，
      // 并将 this (ShuffleBlockFetcherIterator，作为 DownloadFileManager 接口的实现) 作为最后一个参数传入。这会指示底层网络层将数据直接写入磁盘上的临时文件
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }

  /**
   * This is called from initialize and also from the fallback which is triggered from
   * [[PushBasedFetchHelper]].
   */
  // 作用是遍历所有待抓取的 Shuffle 块，并根据它们的存储位置（本地、主机本地、远程等）将它们进行分类，同时将远程块打包成受流控限制的抓取请求 (FetchRequest)
  private[this] def partitionBlocksByFetchMode(
      // 待处理的块信息迭代器，按 BlockManagerId 分组
      blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
      //用于存储需要从当前执行器读取的块，这是出参
      localBlocks: mutable.LinkedHashSet[(BlockId, Int)],
      //用于存储需要从同一主机上其他执行器读取的块，这是出参
      hostLocalBlocksByExecutor:
        mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]],
      //用于存储 Push-Based Shuffle 模式下，已在本地执行器上合并的块，这是出参
      pushMergedLocalBlocks: mutable.LinkedHashSet[BlockId]): ArrayBuffer[FetchRequest] = {
    logDebug(s"maxBytesInFlight: $maxBytesInFlight, targetRemoteRequestSize: "
      + s"$targetRemoteRequestSize, maxBlocksInFlightPerAddress: $maxBlocksInFlightPerAddress")

    // Partition to local, host-local, push-merged-local, remote (includes push-merged-remote)
    // blocks.Remote blocks are further split into FetchRequests of size at most maxBytesInFlight
    // in order to limit the amount of data in flight
    //用于收集所有打包好的、准备发送的远程抓取请求
    val collectedRemoteRequests = new ArrayBuffer[FetchRequest]
    var localBlockBytes = 0L
    var hostLocalBlockBytes = 0L
    var numHostLocalBlocks = 0
    var pushMergedLocalBlockBytes = 0L
    val prevNumBlocksToFetch = numBlocksToFetch
    //获取备用存储（Fallback Storage）的执行器 ID。该 ID 通常与本地执行器 ID 一起被视为“本地”范围
    val fallback = FallbackStorage.FALLBACK_BLOCK_MANAGER_ID.executorId
    // 集合包含当前执行器的 ID 和备用存储的 ID。如果块存储在这两个 ID 之一的执行器上，则视为“本地”块
    val localExecIds = Set(blockManager.blockManagerId.executorId, fallback)
    for ((address, blockInfos) <- blocksByAddress) {
      //循环遍历输入迭代器，每次获取一个 (BlockManagerId, 块信息列表) 对
      checkBlockSizes(blockInfos)
      if (pushBasedFetchHelper.isPushMergedShuffleBlockAddress(address)) {
        //检查该地址是否是一个推式合并（Push-Merged）Shuffle 块的地址
        // These are push-merged blocks or shuffle chunks of these blocks.
        if (address.host == blockManager.blockManagerId.host) {
          //如果地址是推式合并块并且主机名与当前执行器的主机名相同，则该块是推式合并本地块
          numBlocksToFetch += blockInfos.size
          pushMergedLocalBlocks ++= blockInfos.map(_._1)
          pushMergedLocalBlockBytes += blockInfos.map(_._2).sum
        } else {
          //如果推式合并块地址与当前主机不同，则将其视为远程块
          collectFetchRequests(address, blockInfos, collectedRemoteRequests)
        }
      } else if (localExecIds.contains(address.executorId)) {
        //如果块的执行器 ID 属于 localExecIds（即当前执行器或备用存储），则视为本地块
        //如果启用了批量抓取 (doBatchFetch)，尝试将连续的 Shuffle 块 ID 合并成一个逻辑上的块，以减少 I/O 操作数
        val mergedBlockInfos = mergeContinuousShuffleBlockIdsIfNeeded(
          blockInfos.map(info => FetchBlockInfo(info._1, info._2, info._3)), doBatchFetch)
        numBlocksToFetch += mergedBlockInfos.size
        localBlocks ++= mergedBlockInfos.map(info => (info.blockId, info.mapIndex))
        localBlockBytes += mergedBlockInfos.map(_.size).sum
      } else if (blockManager.hostLocalDirManager.isDefined &&
        address.host == blockManager.blockManagerId.host) {
        //如果当前执行器配置了 hostLocalDirManager 并且块存储在同一主机上的其他执行器，则视为主机本地块（Host-Local）
        //尝试合并主机本地块的 ID
        val mergedBlockInfos = mergeContinuousShuffleBlockIdsIfNeeded(
          blockInfos.map(info => FetchBlockInfo(info._1, info._2, info._3)), doBatchFetch)
        numBlocksToFetch += mergedBlockInfos.size
        val blocksForAddress =
          mergedBlockInfos.map(info => (info.blockId, info.size, info.mapIndex))
        hostLocalBlocksByExecutor += address -> blocksForAddress
        numHostLocalBlocks += blocksForAddress.size
        hostLocalBlockBytes += mergedBlockInfos.map(_.size).sum
      } else {
        //如果不满足以上任何条件，则该块是需要通过网络抓取的远程块
        val (_, timeCost) = Utils.timeTakenMs[Unit] {
          collectFetchRequests(address, blockInfos, collectedRemoteRequests)
        }
        logDebug(s"Collected remote fetch requests for $address in $timeCost ms")
      }
    }
    //遍历 collectedRemoteRequests 列表，计算远程块的总字节数 (remoteBlockBytes) 和总数量 (numRemoteBlocks)
    val (remoteBlockBytes, numRemoteBlocks) =
      collectedRemoteRequests.foldLeft((0L, 0))((x, y) => (x._1 + y.size, x._2 + y.blocks.size))

    //累加所有四种类型块的总字节数
    val totalBytes = localBlockBytes + remoteBlockBytes + hostLocalBlockBytes +
      pushMergedLocalBlockBytes
    val blocksToFetchCurrentIteration = numBlocksToFetch - prevNumBlocksToFetch
    //断言本次划分出的块总数（本地、主机本地、推式本地、远程）必须等于 numBlocksToFetch 的增量。这保证了所有非零大小的块都被正确分类
    assert(blocksToFetchCurrentIteration == localBlocks.size +
      numHostLocalBlocks + numRemoteBlocks + pushMergedLocalBlocks.size,
        s"The number of non-empty blocks $blocksToFetchCurrentIteration doesn't equal to the sum " +
        s"of the number of local blocks ${localBlocks.size} + " +
        s"the number of host-local blocks ${numHostLocalBlocks} " +
        s"the number of push-merged-local blocks ${pushMergedLocalBlocks.size} " +
        s"+ the number of remote blocks ${numRemoteBlocks} ")
    logInfo(s"Getting $blocksToFetchCurrentIteration " +
      s"(${Utils.bytesToString(totalBytes)}) non-empty blocks including " +
      s"${localBlocks.size} (${Utils.bytesToString(localBlockBytes)}) local and " +
      s"${numHostLocalBlocks} (${Utils.bytesToString(hostLocalBlockBytes)}) " +
      s"host-local and ${pushMergedLocalBlocks.size} " +
      s"(${Utils.bytesToString(pushMergedLocalBlockBytes)}) " +
      s"push-merged-local and $numRemoteBlocks (${Utils.bytesToString(remoteBlockBytes)}) " +
      s"remote blocks")
    this.hostLocalBlocks ++= hostLocalBlocksByExecutor.values
      .flatMap { infos => infos.map(info => (info._1, info._3)) }
    collectedRemoteRequests
  }

  private def createFetchRequest(
      blocks: collection.Seq[FetchBlockInfo],
      address: BlockManagerId,
      forMergedMetas: Boolean): FetchRequest = {
    logDebug(s"Creating fetch request of ${blocks.map(_.size).sum} at $address "
      + s"with ${blocks.size} blocks")
    FetchRequest(address, blocks, forMergedMetas)
  }
  //用于将一批 Shuffle 块封装成一个或多个 FetchRequest 对象的核心工具
  private def createFetchRequests(
      curBlocks: collection.Seq[FetchBlockInfo], // 当前需要被打包的 FetchBlockInfo 序列
      address: BlockManagerId, //块所属的远程执行器的 ID
      isLast: Boolean, // 标志这批块是否是来自该 address 的所有块中的最后一批。这会影响流控限制的应用
      collectedRemoteRequests: ArrayBuffer[FetchRequest], //出参，用于收集新创建的 FetchRequest 对象的列表
      enableBatchFetch: Boolean,  // 标志是否应尝试进行连续块 ID 的合并（批量抓取优化）
      forMergedMetas: Boolean = false): ArrayBuffer[FetchBlockInfo] = { // 标志请求是否针对合并块的元数据。方法返回一个剩余未打包的块列表
    //如果 enableBatchFetch 为 true，该方法会将连续的 Shuffle 块 ID 合并成一个逻辑上的 MergedBlock ID，从而减少网络 I/O 次数
    val mergedBlocks = mergeContinuousShuffleBlockIdsIfNeeded(curBlocks, enableBatchFetch)
    numBlocksToFetch += mergedBlocks.size
    val retBlocks = new ArrayBuffer[FetchBlockInfo]
    if (mergedBlocks.length <= maxBlocksInFlightPerAddress) {
      //情况 1： 如果合并后的块数量没有超过单个地址的最大在途块数限制
      //将所有块打包成一个 FetchRequest，并将其添加到收集器中
      collectedRemoteRequests += createFetchRequest(mergedBlocks, address, forMergedMetas)
    } else {
      //情况 2： 如果合并后的块数量超过了限制，需要将其分割成多个请求
      mergedBlocks.grouped(maxBlocksInFlightPerAddress).foreach { blocks =>
        if (blocks.length == maxBlocksInFlightPerAddress || isLast) {
          collectedRemoteRequests += createFetchRequest(blocks, address, forMergedMetas)
        } else {
          // The last group does not exceed `maxBlocksInFlightPerAddress`. Put it back
          // to `curBlocks`.
          retBlocks ++= blocks
          numBlocksToFetch -= blocks.size
        }
      }
    }
    retBlocks
  }
  // 用于打包远程抓取请求的关键逻辑。它的核心目标是接收来自单个远程执行器的 Shuffle 块列表，并根据流量控制限制（如最大请求大小和最大块数）将它们分割成多个 FetchRequest 对象
  // address: BlockManagerId 传入远程地址
  // blockInfos: collection.Seq[...]	  存储在该远程执行器上的、待抓取的所有 Shuffle 块的信息序列。
  private def collectFetchRequests(
      address: BlockManagerId,
      blockInfos: collection.Seq[(BlockId, Long, Int)],
      collectedRemoteRequests: ArrayBuffer[FetchRequest]): Unit = {
    //将块信息序列转换为迭代器，以便逐个处理
    val iterator = blockInfos.iterator
    //累加当前正在打包的 FetchRequest 中所有块的总字节数
    var curRequestSize = 0L
    //存储当前正在打包的 FetchRequest 中包含的 FetchBlockInfo 列表
    var curBlocks = new ArrayBuffer[FetchBlockInfo]()

    while (iterator.hasNext) {
      val (blockId, size, mapIndex) = iterator.next()
      curBlocks += FetchBlockInfo(blockId, size, mapIndex)
      curRequestSize += size
      blockId match {
        // Either all blocks are push-merged blocks, shuffle chunks, or original blocks.
        // Based on these types, we decide to do batch fetch and create FetchRequests with
        // forMergedMetas set.
        // 处理 Shuffle 块片段： 这通常是 Push-Based Shuffle 中合并块的片段
        case ShuffleBlockChunkId(_, _, _, _) =>
          if (curRequestSize >= targetRemoteRequestSize ||
            curBlocks.size >= maxBlocksInFlightPerAddress) {
            //打包并重置
            curBlocks = createFetchRequests(curBlocks, address, isLast = false,
              collectedRemoteRequests, enableBatchFetch = false)
            curRequestSize = curBlocks.map(_.size).sum
          }
          //处理 Shuffle 合并块： 通常是 Push-Based Shuffle 中完全合并的大块
        case ShuffleMergedBlockId(_, _, _) =>
          if (curBlocks.size >= maxBlocksInFlightPerAddress) {
            curBlocks = createFetchRequests(curBlocks, address, isLast = false,
              collectedRemoteRequests, enableBatchFetch = false, forMergedMetas = true)
          }
        case _ =>
          // For batch fetch, the actual block in flight should count for merged block.
          //处理普通 Shuffle 块： 这是最常见的 Shuffle 块类型
          val mayExceedsMaxBlocks = !doBatchFetch && curBlocks.size >= maxBlocksInFlightPerAddress
          if (curRequestSize >= targetRemoteRequestSize || mayExceedsMaxBlocks) {
            curBlocks = createFetchRequests(curBlocks, address, isLast = false,
              collectedRemoteRequests, doBatchFetch)
            curRequestSize = curBlocks.map(_.size).sum
          }
      }
    }
    // Add in the final request
    //检查 curBlocks 中是否还有未打包的块（即循环中未触发打包条件的剩余块）
    if (curBlocks.nonEmpty) {
      //确定最后一个请求的配置
      val (enableBatchFetch, forMergedMetas) = {
        curBlocks.head.blockId match {
          case ShuffleBlockChunkId(_, _, _, _) => (false, false)
          case ShuffleMergedBlockId(_, _, _) => (false, true)
          case _ => (doBatchFetch, false)
        }
      }
      createFetchRequests(curBlocks, address, isLast = true, collectedRemoteRequests,
        enableBatchFetch = enableBatchFetch, forMergedMetas = forMergedMetas)
    }
  }
  //检查块大小，不能小于等于0
  private def assertPositiveBlockSize(blockId: BlockId, blockSize: Long): Unit = {
    if (blockSize < 0) {
      throw BlockException(blockId, "Negative block size " + size)
    } else if (blockSize == 0) {
      throw BlockException(blockId, "Zero-sized blocks should be excluded.")
    }
  }
  //检查一个集合中每个块的大小
  private def checkBlockSizes(blockInfos: collection.Seq[(BlockId, Long, Int)]): Unit = {
    blockInfos.foreach { case (blockId, size, _) => assertPositiveBlockSize(blockId, size) }
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchLocalBlocks(
      localBlocks: mutable.LinkedHashSet[(BlockId, Int)]): Unit = {
    logDebug(s"Start fetching local blocks: ${localBlocks.mkString(", ")}")
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val (blockId, mapIndex) = iter.next()
      try {
        val buf = blockManager.getLocalBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(SuccessFetchResult(blockId, mapIndex, blockManager.blockManagerId,
          buf.size(), buf, false))
      } catch {
        // If we see an exception, stop immediately.
        case e: Exception =>
          e match {
            // ClosedByInterruptException is an excepted exception when kill task,
            // don't log the exception stack trace to avoid confusing users.
            // See: SPARK-28340
            case ce: ClosedByInterruptException =>
              logError("Error occurred while fetching local blocks, " + ce.getMessage)
            case ex: Exception => logError("Error occurred while fetching local blocks", ex)
          }
          results.put(FailureFetchResult(blockId, mapIndex, blockManager.blockManagerId, e))
          return
      }
    }
  }

  private[this] def fetchHostLocalBlock(
      blockId: BlockId,
      mapIndex: Int,
      localDirs: Array[String],
      blockManagerId: BlockManagerId): Boolean = {
    try {
      val buf = blockManager.getHostLocalShuffleData(blockId, localDirs)
      buf.retain()
      results.put(SuccessFetchResult(blockId, mapIndex, blockManagerId, buf.size(), buf,
        isNetworkReqDone = false))
      true
    } catch {
      case e: Exception =>
        // If we see an exception, stop immediately.
        logError(s"Error occurred while fetching local blocks", e)
        results.put(FailureFetchResult(blockId, mapIndex, blockManagerId, e))
        false
    }
  }

  /**
   * Fetch the host-local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchHostLocalBlocks(
      hostLocalDirManager: HostLocalDirManager,
      hostLocalBlocksByExecutor:
        mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]]):
    Unit = {
    val cachedDirsByExec = hostLocalDirManager.getCachedHostLocalDirs
    val (hostLocalBlocksWithCachedDirs, hostLocalBlocksWithMissingDirs) = {
      val (hasCache, noCache) = hostLocalBlocksByExecutor.partition { case (hostLocalBmId, _) =>
        cachedDirsByExec.contains(hostLocalBmId.executorId)
      }
      (hasCache.toMap, noCache.toMap)
    }

    if (hostLocalBlocksWithMissingDirs.nonEmpty) {
      logDebug(s"Asynchronous fetching host-local blocks without cached executors' dir: " +
        s"${hostLocalBlocksWithMissingDirs.mkString(", ")}")

      // If the external shuffle service is enabled, we'll fetch the local directories for
      // multiple executors from the external shuffle service, which located at the same host
      // with the executors, in once. Otherwise, we'll fetch the local directories from those
      // executors directly one by one. The fetch requests won't be too much since one host is
      // almost impossible to have many executors at the same time practically.
      val dirFetchRequests = if (blockManager.externalShuffleServiceEnabled) {
        val host = blockManager.blockManagerId.host
        val port = blockManager.externalShuffleServicePort
        Seq((host, port, hostLocalBlocksWithMissingDirs.keys.toArray))
      } else {
        hostLocalBlocksWithMissingDirs.keys.map(bmId => (bmId.host, bmId.port, Array(bmId))).toSeq
      }

      dirFetchRequests.foreach { case (host, port, bmIds) =>
        hostLocalDirManager.getHostLocalDirs(host, port, bmIds.map(_.executorId)) {
          case Success(dirsByExecId) =>
            fetchMultipleHostLocalBlocks(
              hostLocalBlocksWithMissingDirs.filterKeys(bmIds.contains).toMap,
              dirsByExecId,
              cached = false)

          case Failure(throwable) =>
            logError("Error occurred while fetching host local blocks", throwable)
            val bmId = bmIds.head
            val blockInfoSeq = hostLocalBlocksWithMissingDirs(bmId)
            val (blockId, _, mapIndex) = blockInfoSeq.head
            results.put(FailureFetchResult(blockId, mapIndex, bmId, throwable))
        }
      }
    }

    if (hostLocalBlocksWithCachedDirs.nonEmpty) {
      logDebug(s"Synchronous fetching host-local blocks with cached executors' dir: " +
          s"${hostLocalBlocksWithCachedDirs.mkString(", ")}")
      fetchMultipleHostLocalBlocks(hostLocalBlocksWithCachedDirs, cachedDirsByExec, cached = true)
    }
  }

  private def fetchMultipleHostLocalBlocks(
      bmIdToBlocks: Map[BlockManagerId, collection.Seq[(BlockId, Long, Int)]],
      localDirsByExecId: Map[String, Array[String]],
      cached: Boolean): Unit = {
    // We use `forall` because once there's a failed block fetch, `fetchHostLocalBlock` will put
    // a `FailureFetchResult` immediately to the `results`. So there's no reason to fetch the
    // remaining blocks.
    val allFetchSucceeded = bmIdToBlocks.forall { case (bmId, blockInfos) =>
      blockInfos.forall { case (blockId, _, mapIndex) =>
        fetchHostLocalBlock(blockId, mapIndex, localDirsByExecId(bmId.executorId), bmId)
      }
    }
    if (allFetchSucceeded) {
      logDebug(s"Got host-local blocks from ${bmIdToBlocks.keys.mkString(", ")} " +
        s"(${if (cached) "with" else "without"} cached executors' dir) " +
        s"in ${Utils.getUsedTimeNs(startTimeNs)}")
    }
  }
 //Shuffle 数据抓取过程的启动函数。这个方法负责设置清理机制、划分数据块、初始化远程请求队列和发出首批网络请求
  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    //清理机制设置：为当前 TaskContext 注册一个完成回调函数 onCompleteCallback。
    // 这个回调函数会在任务成功或失败时被调用，用于执行 ShuffleBlockFetcherIterator 的 cleanup() 逻辑，释放内存和删除临时文件
    context.addTaskCompletionListener(onCompleteCallback)
    // Local blocks to fetch, excluding zero-sized blocks.
    //用于存储需要从当前执行器的 BlockManager 读取的 Shuffle 块 ID 和 Map 索引
    val localBlocks = mutable.LinkedHashSet[(BlockId, Int)]()
    //用于存储需要从同一物理主机上的其他执行器读取的块。这利用了主机本地性优化
    val hostLocalBlocksByExecutor =
      mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]]()
    //用于存储 Push-Based Shuffle 模式下，已在本地执行器上合并完成的 Shuffle 块
    val pushMergedLocalBlocks = mutable.LinkedHashSet[BlockId]()
    // Partition blocks by the different fetch modes: local, host-local, push-merged-local and
    // remote blocks.
    //根据块的存储位置，将所有待抓取的块划分成四种类型：本地块、主机本地块、推式合并本地块和远程请求（需要通过网络抓取的块）
    val remoteRequests = partitionBlocksByFetchMode(
      blocksByAddress, localBlocks, hostLocalBlocksByExecutor, pushMergedLocalBlocks)
    // Add the remote requests into our queue in a random order
    //将所有需要远程抓取的请求列表 (remoteRequests) 随机打乱，然后加入到待发送的请求队列 (fetchRequests) 中。随机化是为了避免集中请求少数几个 Map 输出服务器，实现负载均衡
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    // 开始网络抓取：调用此方法，根据流量控制参数 (maxBytesInFlight, maxReqsInFlight 等)，从 fetchRequests 队列中取出请求并发送到网络
    fetchUpToMaxBytes()
    // 计算在 fetchUpToMaxBytes() 执行后，有多少个请求因流控限制而进入了延迟请求队列
    val numDeferredRequest = deferredFetchRequests.values.map(_.size).sum
    //计算实际抓取数
    val numFetches = remoteRequests.size - fetchRequests.size - numDeferredRequest
    logInfo(s"Started $numFetches remote fetches in ${Utils.getUsedTimeNs(startTimeNs)}" +
      (if (numDeferredRequest > 0 ) s", deferred $numDeferredRequest requests" else ""))

    // Get Local Blocks
    //处理本地块：调用此方法，同步地从当前执行器的 BlockManager 中读取本地 Shuffle 块
    fetchLocalBlocks(localBlocks)
    logDebug(s"Got local blocks in ${Utils.getUsedTimeNs(startTimeNs)}")
    // Get host local blocks if any
    //处理主机本地块：调用此方法，同步地从同一主机上的其他执行器读取 Shuffle 块（通常通过特殊的本地通信路径）
    fetchAllHostLocalBlocks(hostLocalBlocksByExecutor)
    //处理推式合并本地块：如果启用了 Push-Based Shuffle，调用辅助器抓取已在本地合并的 Shuffle 块
    pushBasedFetchHelper.fetchAllPushMergedLocalBlocks(pushMergedLocalBlocks)
  }

  private def fetchAllHostLocalBlocks(
      hostLocalBlocksByExecutor:
        mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]]):
    Unit = {
    if (hostLocalBlocksByExecutor.nonEmpty) {
      blockManager.hostLocalDirManager.foreach(fetchHostLocalBlocks(_, hostLocalBlocksByExecutor))
    }
  }

  private def shuffleMetricsUpdate(
      blockId: BlockId,
      buf: ManagedBuffer,
      local: Boolean): Unit = {
    if (local) {
      shuffleLocalMetricsUpdate(blockId, buf)
    } else {
      shuffleRemoteMetricsUpdate(blockId, buf)
    }
  }

  private def shuffleLocalMetricsUpdate(blockId: BlockId, buf: ManagedBuffer): Unit = {
    blockId match {
      case chunkId: ShuffleBlockChunkId =>
        val chunkCardinality = pushBasedFetchHelper.getShuffleChunkCardinality(chunkId)
        shuffleMetrics.incLocalMergedChunksFetched(1)
        shuffleMetrics.incLocalMergedBlocksFetched(chunkCardinality)
        shuffleMetrics.incLocalMergedBytesRead(buf.size)
        shuffleMetrics.incLocalBlocksFetched(chunkCardinality)
      case _ =>
        shuffleMetrics.incLocalBlocksFetched(1)
    }
    shuffleMetrics.incLocalBytesRead(buf.size)
  }

  private def shuffleRemoteMetricsUpdate(blockId: BlockId, buf: ManagedBuffer): Unit = {
    blockId match {
      case chunkId: ShuffleBlockChunkId =>
        val chunkCardinality = pushBasedFetchHelper.getShuffleChunkCardinality(chunkId)
        shuffleMetrics.incRemoteMergedChunksFetched(1)
        shuffleMetrics.incRemoteMergedBlocksFetched(chunkCardinality)
        shuffleMetrics.incRemoteMergedBytesRead(buf.size)
        shuffleMetrics.incRemoteBlocksFetched(chunkCardinality)
      case _ =>
        shuffleMetrics.incRemoteBlocksFetched(1)
    }
    shuffleMetrics.incRemoteBytesRead(buf.size)
    if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
      shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
    }
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  // 是 Shuffle 抓取流程中消费结果的关键点。
  // 它是一个阻塞操作，会等待异步网络请求的结果 (FetchResult)，对结果进行处理、解压、检查数据损坏，并最终返回一个可供用户读取的 (BlockId, InputStream) 对
  // 返回一个 Shuffle 块 ID 和其对应的输入流
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw SparkCoreErrors.noSuchElementError()
    }
    // 统计已开始处理的块数量
    numBlocksProcessed += 1

    var result: FetchResult = null
    //初始化用于最终返回给用户的 InputStream，以及用于数据校验的 CheckedInputStream 等辅助变量
    var input: InputStream = null
    // This's only initialized when shuffle checksum is enabled.
    var checkedIn: CheckedInputStream = null
    //标记数据流是否经过压缩或加密，这会影响数据损坏检测逻辑
    var streamCompressedOrEncrypted: Boolean = false
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.nanoTime()
      //从阻塞队列 results 中取出下一个抓取结果。这是阻塞点
      result = results.take()
      val fetchWaitTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait)
      shuffleMetrics.incFetchWaitTime(fetchWaitTime)

      result match {
        //成功获取块
        case SuccessFetchResult(blockId, mapIndex, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            // 如果块不是来自本地 BlockManager，则需要更新远程抓取指标。
            if (hostLocalBlocks.contains(blockId -> mapIndex) ||
              pushBasedFetchHelper.isLocalPushMergedBlockAddress(address)) {
              // It is a host local block or a local shuffle chunk
              shuffleMetricsUpdate(blockId, buf, local = true)
            } else {
              numBlocksInFlightPerAddress(address) -= 1
              shuffleMetricsUpdate(blockId, buf, local = false)
              bytesInFlight -= size
            }
          }
          //如果当前请求包含的所有块都已成功返回 (isNetworkReqDone=true)，则减少全局在途请求数 (reqsInFlight)，并尝试重置 Netty OOM 标志
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            resetNettyOOMFlagIfPossible(maxReqSizeShuffleToMem)
            logDebug("Number of requests in flight " + reqsInFlight)
          }
          //检查 ManagedBuffer 是否为零大小。零大小块被视为错误
          val in = if (buf.size == 0) {
            // We will never legitimately receive a zero-size block. All blocks with zero records
            // have zero size and all zero-size blocks have no records (and hence should never
            // have been requested in the first place). This statement relies on behaviors of the
            // shuffle writers, which are guaranteed by the following test cases:
            //
            // - BypassMergeSortShuffleWriterSuite: "write with some empty partitions"
            // - UnsafeShuffleWriterSuite: "writeEmptyIterator"
            // - DiskBlockObjectWriterSuite: "commit() and close() without ever opening or writing"
            //
            // There is not an explicit test for SortShuffleWriter but the underlying APIs that
            // uses are shared by the UnsafeShuffleWriter (both writers use DiskBlockObjectWriter
            // which returns a zero-size from commitAndGet() in case no records were written
            // since the last call.
            val msg = s"Received a zero-size buffer for block $blockId from $address " +
              s"(expectedApproxSize = $size, isNetworkReqDone=$isNetworkReqDone)"
            if (blockId.isShuffleChunk) {
              // Zero-size block may come from nodes with hardware failures, For shuffle chunks,
              // the original shuffle blocks that belong to that zero-size shuffle chunk is
              // available and we can opt to fallback immediately.
              logWarning(msg)
              pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
              shuffleMetrics.incCorruptMergedBlockChunks(1)
              // Set result to null to trigger another iteration of the while loop to get either.
              result = null
              null
            } else {
              throwFetchFailedException(blockId, mapIndex, address, new IOException(msg))
            }
          } else {
            try {
              val bufIn = buf.createInputStream()
              if (checksumEnabled) {
                val checksum = ShuffleChecksumHelper.getChecksumByAlgorithm(checksumAlgorithm)
                checkedIn = new CheckedInputStream(bufIn, checksum)
                checkedIn
              } else {
                bufIn
              }
            } catch {
              // The exception could only be throwed by local shuffle block
              case e: IOException =>
                assert(buf.isInstanceOf[FileSegmentManagedBuffer])
                e match {
                  case ce: ClosedByInterruptException =>
                    logError("Failed to create input stream from local block, " +
                      ce.getMessage)
                  case e: IOException =>
                    logError("Failed to create input stream from local block", e)
                }
                buf.release()
                if (blockId.isShuffleChunk) {
                  pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
                  // Set result to null to trigger another iteration of the while loop to get
                  // either.
                  result = null
                  null
                } else {
                  throwFetchFailedException(blockId, mapIndex, address, e)
                }
            }
          }

          if (in != null) {
            try {
              //应用解压、解密等流包装器。input 此时是解压/解密后的流
              input = streamWrapper(blockId, in)
              // If the stream is compressed or wrapped, then we optionally decompress/unwrap the
              // first maxBytesInFlight/3 bytes into memory, to check for corruption in that portion
              // of the data. But even if 'detectCorruptUseExtraMemory' configuration is off, or if
              // the corruption is later, we'll still detect the corruption later in the stream.
              //检查 input 是否与原始流 in 是同一个对象，判断是否进行了压缩/加密/其他包装操作
              streamCompressedOrEncrypted = !input.eq(in)
              if (streamCompressedOrEncrypted && detectCorruptUseExtraMemory) {
                // TODO: manage the memory used here, and spill it into disk in case of OOM.
                input = Utils.copyStreamUpTo(input, maxBytesInFlight / 3)
              }
            } catch {
              case e: IOException =>
                // When shuffle checksum is enabled, for a block that is corrupted twice,
                // we'd calculate the checksum of the block by consuming the remaining data
                // in the buf. So, we should release the buf later.
                if (!(checksumEnabled && corruptedBlocks.contains(blockId))) {
                  buf.release()
                }

                if (blockId.isShuffleChunk) {
                  shuffleMetrics.incCorruptMergedBlockChunks(1)
                  // TODO (SPARK-36284): Add shuffle checksum support for push-based shuffle
                  // Retrying a corrupt block may result again in a corrupt block. For shuffle
                  // chunks, we opt to fallback on the original shuffle blocks that belong to that
                  // corrupt shuffle chunk immediately instead of retrying to fetch the corrupt
                  // chunk. This also makes the code simpler because the chunkMeta corresponding to
                  // a shuffle chunk is always removed from chunksMetaMap whenever a shuffle chunk
                  // gets processed. If we try to re-fetch a corrupt shuffle chunk, then it has to
                  // be added back to the chunksMetaMap.
                  pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
                  // Set result to null to trigger another iteration of the while loop.
                  result = null
                } else if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
                  throwFetchFailedException(blockId, mapIndex, address, e)
                } else if (corruptedBlocks.contains(blockId)) {
                  // It's the second time this block is detected corrupted
                  if (checksumEnabled) {
                    // Diagnose the cause of data corruption if shuffle checksum is enabled
                    val diagnosisResponse = diagnoseCorruption(checkedIn, address, blockId)
                    buf.release()
                    logError(diagnosisResponse)
                    throwFetchFailedException(
                      blockId, mapIndex, address, e, Some(diagnosisResponse))
                  } else {
                    throwFetchFailedException(blockId, mapIndex, address, e)
                  }
                } else {
                  // It's the first time this block is detected corrupted
                  logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                  corruptedBlocks += blockId
                  fetchRequests += FetchRequest(
                    address, Array(FetchBlockInfo(blockId, size, mapIndex)))
                  result = null
                }
            } finally {
              if (blockId.isShuffleChunk) {
                pushBasedFetchHelper.removeChunk(blockId.asInstanceOf[ShuffleBlockChunkId])
              }
              // TODO: release the buf here to free memory earlier
              if (input == null) {
                // Close the underlying stream if there was an issue in wrapping the stream using
                // streamWrapper
                in.close()
              }
            }
          }
        // 致命失败
        case FailureFetchResult(blockId, mapIndex, address, e) =>
          var errorMsg: String = null
          if (e.isInstanceOf[OutOfDirectMemoryError]) {
            errorMsg = s"Block $blockId fetch failed after $maxAttemptsOnNettyOOM " +
              s"retries due to Netty OOM"
            logError(errorMsg)
          }
          throwFetchFailedException(blockId, mapIndex, address, e, Some(errorMsg))
        // 延迟请求
        case DeferFetchRequestResult(request) =>
          val address = request.address
          numBlocksInFlightPerAddress(address) -= request.blocks.size
          bytesInFlight -= request.size
          reqsInFlight -= 1
          logDebug("Number of requests in flight " + reqsInFlight)
          val defReqQueue =
            deferredFetchRequests.getOrElseUpdate(address, new Queue[FetchRequest]())
          defReqQueue.enqueue(request)
          result = null

        case FallbackOnPushMergedFailureResult(blockId, address, size, isNetworkReqDone) =>
          // We get this result in 3 cases:
          // 1. Failure to fetch the data of a remote shuffle chunk. In this case, the
          //    blockId is a ShuffleBlockChunkId.
          // 2. Failure to read the push-merged-local meta. In this case, the blockId is
          //    ShuffleBlockId.
          // 3. Failure to get the push-merged-local directories from the external shuffle service.
          //    In this case, the blockId is ShuffleBlockId.
          if (pushBasedFetchHelper.isRemotePushMergedBlockAddress(address)) {
            numBlocksInFlightPerAddress(address) -= 1
            bytesInFlight -= size
          }
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }
          pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(blockId, address)
          // Set result to null to trigger another iteration of the while loop to get either
          // a SuccessFetchResult or a FailureFetchResult.
          result = null

          case PushMergedLocalMetaFetchResult(
            shuffleId, shuffleMergeId, reduceId, bitmaps, localDirs) =>
            // Fetch push-merged-local shuffle block data as multiple shuffle chunks
            val shuffleBlockId = ShuffleMergedBlockId(shuffleId, shuffleMergeId, reduceId)
            try {
              val bufs: Seq[ManagedBuffer] = blockManager.getLocalMergedBlockData(shuffleBlockId,
                localDirs)
              // Since the request for local block meta completed successfully, numBlocksToFetch
              // is decremented.
              numBlocksToFetch -= 1
              // Update total number of blocks to fetch, reflecting the multiple local shuffle
              // chunks.
              numBlocksToFetch += bufs.size
              bufs.zipWithIndex.foreach { case (buf, chunkId) =>
                buf.retain()
                val shuffleChunkId = ShuffleBlockChunkId(shuffleId, shuffleMergeId, reduceId,
                  chunkId)
                pushBasedFetchHelper.addChunk(shuffleChunkId, bitmaps(chunkId))
                results.put(SuccessFetchResult(shuffleChunkId, SHUFFLE_PUSH_MAP_ID,
                  pushBasedFetchHelper.localShuffleMergerBlockMgrId, buf.size(), buf,
                  isNetworkReqDone = false))
              }
            } catch {
              case e: Exception =>
                // If we see an exception with reading push-merged-local index file, we fallback
                // to fetch the original blocks. We do not report block fetch failure
                // and will continue with the remaining local block read.
                logWarning(s"Error occurred while reading push-merged-local index, " +
                  s"prepare to fetch the original blocks", e)
                pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(
                  shuffleBlockId, pushBasedFetchHelper.localShuffleMergerBlockMgrId)
            }
            result = null

        case PushMergedRemoteMetaFetchResult(
          shuffleId, shuffleMergeId, reduceId, blockSize, bitmaps, address) =>
          // The original meta request is processed so we decrease numBlocksToFetch and
          // numBlocksInFlightPerAddress by 1. We will collect new shuffle chunks request and the
          // count of this is added to numBlocksToFetch in collectFetchReqsFromMergedBlocks.
          numBlocksInFlightPerAddress(address) -= 1
          numBlocksToFetch -= 1
          val blocksToFetch = pushBasedFetchHelper.createChunkBlockInfosFromMetaResponse(
            shuffleId, shuffleMergeId, reduceId, blockSize, bitmaps)
          val additionalRemoteReqs = new ArrayBuffer[FetchRequest]
          collectFetchRequests(address, blocksToFetch.toSeq, additionalRemoteReqs)
          fetchRequests ++= additionalRemoteReqs
          // Set result to null to force another iteration.
          result = null

        case PushMergedRemoteMetaFailedFetchResult(
          shuffleId, shuffleMergeId, reduceId, address) =>
          // The original meta request failed so we decrease numBlocksInFlightPerAddress by 1.
          numBlocksInFlightPerAddress(address) -= 1
          // If we fail to fetch the meta of a push-merged block, we fall back to fetching the
          // original blocks.
          pushBasedFetchHelper.initiateFallbackFetchForPushMergedBlock(
            ShuffleMergedBlockId(shuffleId, shuffleMergeId, reduceId), address)
          // Set result to null to force another iteration.
          result = null
      }

      // Send fetch requests up to maxBytesInFlight
      //在处理完一个结果后，调用此方法以检查当前的流控限制，并尽可能地发送更多的延迟和常规请求
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId,
      //这个特殊的 InputStream 在关闭时会自动调用 buf.release()，释放底层的 ManagedBuffer 内存
      new BufferReleasingInputStream(
        input,
        this,
        currentResult.blockId,
        currentResult.mapIndex,
        currentResult.address,
        detectCorrupt && streamCompressedOrEncrypted,
        currentResult.isNetworkReqDone,
        Option(checkedIn)))
  }

  /**
   * Get the suspect corruption cause for the corrupted block. It should be only invoked
   * when checksum is enabled and corruption was detected at least once.
   *
   * This will firstly consume the rest of stream of the corrupted block to calculate the
   * checksum of the block. Then, it will raise a synchronized RPC call along with the
   * checksum to ask the server(where the corrupted block is fetched from) to diagnose the
   * cause of corruption and return it.
   *
   * Any exception raised during the process will result in the [[Cause.UNKNOWN_ISSUE]] of the
   * corruption cause since corruption diagnosis is only a best effort.
   *
   * @param checkedIn the [[CheckedInputStream]] which is used to calculate the checksum.
   * @param address the address where the corrupted block is fetched from.
   * @param blockId the blockId of the corrupted block.
   * @return The corruption diagnosis response for different causes.
   */
  private[storage] def diagnoseCorruption(
      checkedIn: CheckedInputStream,
      address: BlockManagerId,
      blockId: BlockId): String = {
    logInfo("Start corruption diagnosis.")
    blockId match {
      case shuffleBlock: ShuffleBlockId =>
        val startTimeNs = System.nanoTime()
        val buffer = new Array[Byte](ShuffleChecksumHelper.CHECKSUM_CALCULATION_BUFFER)
        // consume the remaining data to calculate the checksum
        var cause: Cause = null
        try {
          while (checkedIn.read(buffer) != -1) {}
          val checksum = checkedIn.getChecksum.getValue
          cause = shuffleClient.diagnoseCorruption(address.host, address.port, address.executorId,
            shuffleBlock.shuffleId, shuffleBlock.mapId, shuffleBlock.reduceId, checksum,
            checksumAlgorithm)
        } catch {
          case e: Exception =>
            logWarning("Unable to diagnose the corruption cause of the corrupted block", e)
            cause = Cause.UNKNOWN_ISSUE
        }
        val duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
        val diagnosisResponse = cause match {
          case Cause.UNSUPPORTED_CHECKSUM_ALGORITHM =>
            s"Block $blockId is corrupted but corruption diagnosis failed due to " +
              s"unsupported checksum algorithm: $checksumAlgorithm"

          case Cause.CHECKSUM_VERIFY_PASS =>
            s"Block $blockId is corrupted but checksum verification passed"

          case Cause.UNKNOWN_ISSUE =>
            s"Block $blockId is corrupted but the cause is unknown"

          case otherCause =>
            s"Block $blockId is corrupted due to $otherCause"
        }
        logInfo(s"Finished corruption diagnosis in $duration ms. $diagnosisResponse")
        diagnosisResponse
      case shuffleBlockChunk: ShuffleBlockChunkId =>
        // TODO SPARK-36284 Add shuffle checksum support for push-based shuffle
        val diagnosisResponse = s"BlockChunk $shuffleBlockChunk is corrupted but corruption " +
          s"diagnosis is skipped due to lack of shuffle checksum support for push-based shuffle."
        logWarning(diagnosisResponse)
        diagnosisResponse
      case shuffleBlockBatch: ShuffleBlockBatchId =>
          val diagnosisResponse = s"BlockBatch $shuffleBlockBatch is corrupted " +
          s"but corruption diagnosis is skipped due to lack of shuffle checksum support for " +
          s"ShuffleBlockBatchId"
        logWarning(diagnosisResponse)
        diagnosisResponse
      case unexpected: BlockId =>
        throw SparkException.internalError(
          s"Unexpected type of BlockId, $unexpected", category = "STORAGE")
    }
  }

  def toCompletionIterator: Iterator[(BlockId, InputStream)] = {
    CompletionIterator[(BlockId, InputStream), this.type](this,
      onCompleteCallback.onComplete(context))
  }
  //实现流量控制（Throttling）和并发控制的核心逻辑。它的作用是循环检查当前的在途数据量和请求数，并尽可能地从待发送队列中取出请求并发往远程执行器，直到达到预设的流量上限
  private def fetchUpToMaxBytes(): Unit = {
    //判断 Shuffle 网络层是否正在经历 Netty 内存溢出（OOM）错误
    if (isNettyOOMOnShuffle.get()) {
      if (reqsInFlight > 0) {
        //这是一种背压机制：当网络层因 OOM 崩溃时，必须等待所有正在途中的请求完成后，才能尝试重置并发送新请求
        // Return immediately if Netty is still OOMed and there're ongoing fetch requests
        return
      } else {
        //尝试重置 OOM 标志。参数 0 表示不延迟立即重置
        resetNettyOOMFlagIfPossible(0)
      }
    }

    // Send fetch requests up to maxBytesInFlight. If you cannot fetch from a remote host
    // immediately, defer the request until the next time it can be processed.

    // Process any outstanding deferred fetch requests if possible.
    // 优先处理因前一次流控限制而被推迟 (deferred) 的请求
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        //核心检查： 循环检查：1. 当前全局流量限制是否允许发送下一个请求 (isRemoteBlockFetchable)。2. 当前远程地址的单地址块数限制是否允许发送下一个请求 (!isRemoteAddressMaxedOut)
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          //调用内部 send 函数发送请求
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // Process any regular fetch requests if possible.
    //处理首次从主队列中取出的常规请求
    //循环检查全局流量限制是否允许发送主请求队列 (fetchRequests) 中的下一个请求
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      //检查单地址块数限制是否被超过
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress, new Queue[FetchRequest]())
        // 将当前请求放入该地址的延迟请求队列中
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        //调用内部 send 函数发送请求
        send(remoteAddress, request)
      }
    }

    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      if (request.forMergedMetas) {
        //如果请求是针对合并块的元数据的（通常用于 Push Shuffle 模式），则调用辅助器处理
        pushBasedFetchHelper.sendFetchMergedStatusRequest(request)
      } else {
        //通过网络发送常规的 Shuffle 块抓取请求
        sendRequest(request)
      }
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }
    //检查全局流量限制是否允许发送请求队列中的第一个请求
    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
    // given remote address.
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private[storage] def throwFetchFailedException(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable,
      message: Option[String] = None) = {
    val msg = message.getOrElse(e.getMessage)
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw SparkCoreErrors.fetchFailedError(address, shufId, mapId, mapIndex, reduceId, msg, e)
      case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, _) =>
        throw SparkCoreErrors.fetchFailedError(address, shuffleId, mapId, mapIndex, startReduceId,
          msg, e)
      case ShuffleBlockChunkId(shuffleId, _, reduceId, _) =>
        throw SparkCoreErrors.fetchFailedError(address, shuffleId,
          SHUFFLE_PUSH_MAP_ID.toLong, SHUFFLE_PUSH_MAP_ID, reduceId, msg, e)
      case _ => throw SparkCoreErrors.failToGetNonShuffleBlockError(blockId, e)
    }
  }

  /**
   * All the below methods are used by [[PushBasedFetchHelper]] to communicate with the iterator
   */
  private[storage] def addToResultsQueue(result: FetchResult): Unit = {
    results.put(result)
  }

  private[storage] def decreaseNumBlocksToFetch(blocksFetched: Int): Unit = {
    numBlocksToFetch -= blocksFetched
  }

  /**
   * Currently used by [[PushBasedFetchHelper]] to fetch fallback blocks when there is a fetch
   * failure related to a push-merged block or shuffle chunk.
   * This is executed by the task thread when the `iterator.next()` is invoked and if that initiates
   * fallback.
   */
  private[storage] def fallbackFetch(
      originalBlocksByAddr:
        Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])]): Unit = {
    val originalLocalBlocks = mutable.LinkedHashSet[(BlockId, Int)]()
    val originalHostLocalBlocksByExecutor =
      mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]]()
    val originalMergedLocalBlocks = mutable.LinkedHashSet[BlockId]()
    val originalRemoteReqs = partitionBlocksByFetchMode(originalBlocksByAddr,
      originalLocalBlocks, originalHostLocalBlocksByExecutor, originalMergedLocalBlocks)
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(originalRemoteReqs)
    logInfo(s"Created ${originalRemoteReqs.size} fallback remote requests for push-merged")
    // fetch all the fallback blocks that are local.
    fetchLocalBlocks(originalLocalBlocks)
    // Merged local blocks should be empty during fallback
    assert(originalMergedLocalBlocks.isEmpty,
      "There should be zero push-merged blocks during fallback")
    // Some of the fallback local blocks could be host local blocks
    fetchAllHostLocalBlocks(originalHostLocalBlocksByExecutor)
  }

  /**
   * Removes all the pending shuffle chunks that are on the same host and have the same reduceId as
   * the current chunk that had a fetch failure.
   * This is executed by the task thread when the `iterator.next()` is invoked and if that initiates
   * fallback.
   *
   * @return set of all the removed shuffle chunk Ids.
   */
  private[storage] def removePendingChunks(
      failedBlockId: ShuffleBlockChunkId,
      address: BlockManagerId): mutable.HashSet[ShuffleBlockChunkId] = {
    val removedChunkIds = new mutable.HashSet[ShuffleBlockChunkId]()

    def sameShuffleReducePartition(block: BlockId): Boolean = {
      val chunkId = block.asInstanceOf[ShuffleBlockChunkId]
      chunkId.shuffleId == failedBlockId.shuffleId && chunkId.reduceId == failedBlockId.reduceId
    }

    def filterRequests(queue: mutable.Queue[FetchRequest]): Unit = {
      val fetchRequestsToRemove = new mutable.Queue[FetchRequest]()
      fetchRequestsToRemove ++= queue.dequeueAll { req =>
        val firstBlock = req.blocks.head
        firstBlock.blockId.isShuffleChunk && req.address.equals(address) &&
          sameShuffleReducePartition(firstBlock.blockId)
      }
      fetchRequestsToRemove.foreach { _ =>
        removedChunkIds ++=
          fetchRequestsToRemove.flatMap(_.blocks.map(_.blockId.asInstanceOf[ShuffleBlockChunkId]))
      }
    }

    filterRequests(fetchRequests)
    deferredFetchRequests.get(address).foreach { defRequests =>
      filterRequests(defRequests)
      if (defRequests.isEmpty) deferredFetchRequests.remove(address)
    }
    removedChunkIds
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close() and
 * also detects stream corruption if streamCompressedOrEncrypted is true
 */
private class BufferReleasingInputStream(
    // This is visible for testing
    private[storage] val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator,
    private val blockId: BlockId,
    private val mapIndex: Int,
    private val address: BlockManagerId,
    private val detectCorruption: Boolean,
    private val isNetworkReqDone: Boolean,
    private val checkedInOpt: Option[CheckedInputStream])
  extends InputStream {
  private[this] var closed = false

  override def read(): Int =
    tryOrFetchFailedException(delegate.read())

  override def close(): Unit = {
    if (!closed) {
      try {
        delegate.close()
        iterator.releaseCurrentResultBuffer()
      } finally {
        // Unset the flag when a remote request finished and free memory is fairly enough.
        if (isNetworkReqDone) {
          ShuffleBlockFetcherIterator.resetNettyOOMFlagIfPossible(iterator.maxReqSizeShuffleToMem)
        }
        closed = true
      }
    }
  }

  override def available(): Int =
    tryOrFetchFailedException(delegate.available())

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long =
    tryOrFetchFailedException(delegate.skip(n))

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int =
    tryOrFetchFailedException(delegate.read(b))

  override def read(b: Array[Byte], off: Int, len: Int): Int =
    tryOrFetchFailedException(delegate.read(b, off, len))

  override def reset(): Unit = tryOrFetchFailedException(delegate.reset())

  /**
   * Execute a block of code that returns a value, close this stream quietly and re-throwing
   * IOException as FetchFailedException when detectCorruption is true. This method is only
   * used by the `available`, `read` and `skip` methods inside `BufferReleasingInputStream`
   * currently.
   */
  private def tryOrFetchFailedException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException if detectCorruption =>
        val diagnosisResponse = checkedInOpt.map { checkedIn =>
          iterator.diagnoseCorruption(checkedIn, address, blockId)
        }
        IOUtils.closeQuietly(this)
        // We'd never retry the block whatever the cause is since the block has been
        // partially consumed by downstream RDDs.
        iterator.throwFetchFailedException(blockId, mapIndex, address, e, diagnosisResponse)
    }
  }
}

/**
 * A listener to be called at the completion of the ShuffleBlockFetcherIterator
 * @param data the ShuffleBlockFetcherIterator to process
 */
//当任务完成时执行ShuffleBlockFetcherIterator的清理操作，具体时调用cleanup方法
private class ShuffleFetchCompletionListener(var data: ShuffleBlockFetcherIterator)
  extends TaskCompletionListener {

  override def onTaskCompletion(context: TaskContext): Unit = {
    if (data != null) {
      data.cleanup()
      // Null out the referent here to make sure we don't keep a reference to this
      // ShuffleBlockFetcherIterator, after we're done reading from it, to let it be
      // collected during GC. Otherwise we can hold metadata on block locations(blocksByAddress)
      data = null
    }
  }

  // Just an alias for onTaskCompletion to avoid confusing
  def onComplete(context: TaskContext): Unit = this.onTaskCompletion(context)
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A flag which indicates whether the Netty OOM error has raised during shuffle.
   * If true, unless there's no in-flight fetch requests, all the pending shuffle
   * fetch requests will be deferred until the flag is unset (whenever there's a
   * complete fetch request).
   */
  val isNettyOOMOnShuffle = new AtomicBoolean(false)

  //重置OOM的标志
  def resetNettyOOMFlagIfPossible(freeMemoryLowerBound: Long): Unit = {
    if (isNettyOOMOnShuffle.get() && NettyUtils.freeDirectMemory() >= freeMemoryLowerBound) {
      isNettyOOMOnShuffle.compareAndSet(true, false)
    }
  }

  /**
   * This function is used to merged blocks when doBatchFetch is true. Blocks which have the
   * same `mapId` can be merged into one block batch. The block batch is specified by a range
   * of reduceId, which implies the continuous shuffle blocks that we can fetch in a batch.
   * For example, input blocks like (shuffle_0_0_0, shuffle_0_0_1, shuffle_0_1_0) can be
   * merged into (shuffle_0_0_0_2, shuffle_0_1_0_1), and input blocks like (shuffle_0_0_0_2,
   * shuffle_0_0_2, shuffle_0_0_3) can be merged into (shuffle_0_0_0_4).
   *
   * @param blocks blocks to be merged if possible. May contains already merged blocks.
   * @param doBatchFetch whether to merge blocks.
   * @return the input blocks if doBatchFetch=false, or the merged blocks if doBatchFetch=true.
   */
  //据配置（doBatchFetch），将来自同一个 Map Task 且 Reduce 分区 ID 连续的多个 Shuffle 块 ID，合并成一个批量抓取块 ID (ShuffleBlockBatchId)。这个优化可以减少网络请求次数和开销
  //blocks: collection.Seq[FetchBlockInfo] 待合并的块信息序列，可能包含已合并的批量块 ID
  def mergeContinuousShuffleBlockIdsIfNeeded(
      blocks: collection.Seq[FetchBlockInfo],
      doBatchFetch: Boolean): collection.Seq[FetchBlockInfo] = {
    //如果 doBatchFetch 为 false，则跳过整个 if 块，直接返回原始 blocks
    val result = if (doBatchFetch) {
      //用于临时存储连续的、来自同一个 Map Task 的块信息，等待被合并
      val curBlocks = new ArrayBuffer[FetchBlockInfo]
      //用于存储最终合并后的 FetchBlockInfo 结果
      val mergedBlockInfo = new ArrayBuffer[FetchBlockInfo]
      //该内部函数负责将 toBeMerged 缓冲区中的块合并成一个新的 ShuffleBlockBatchId
      def mergeFetchBlockInfo(toBeMerged: ArrayBuffer[FetchBlockInfo]): FetchBlockInfo = {
        val startBlockId = toBeMerged.head.blockId.asInstanceOf[ShuffleBlockId]

        // The last merged block may comes from the input, and we can merge more blocks
        // into it, if the map id is the same.
        //定义一个判断条件：检查上一个已合并到 mergedBlockInfo 中的块（如果它是 ShuffleBlockBatchId）的 mapId 是否与当前待合并的第一个块的 mapId 相同。
        // 如果相同，意味着这些块是连续的，可以进一步合并
        def shouldMergeIntoPreviousBatchBlockId =
          mergedBlockInfo.last.blockId.asInstanceOf[ShuffleBlockBatchId].mapId == startBlockId.mapId

        val (startReduceId, size) =
          if (mergedBlockInfo.nonEmpty && shouldMergeIntoPreviousBatchBlockId) {
            // Remove the previous batch block id as we will add a new one to replace it.
            val removed = mergedBlockInfo.remove(mergedBlockInfo.length - 1)
            (removed.blockId.asInstanceOf[ShuffleBlockBatchId].startReduceId,
              removed.size + toBeMerged.map(_.size).sum)
          } else {
            (startBlockId.reduceId, toBeMerged.map(_.size).sum)
          }

        FetchBlockInfo(
          ShuffleBlockBatchId(
            startBlockId.shuffleId,
            startBlockId.mapId,
            startReduceId,
            toBeMerged.last.blockId.asInstanceOf[ShuffleBlockId].reduceId + 1),
          size,
          toBeMerged.head.mapIndex)
      }

      val iter = blocks.iterator
      while (iter.hasNext) {
        val info = iter.next()
        // It's possible that the input block id is already a batch ID. For example, we merge some
        // blocks, and then make fetch requests with the merged blocks according to "max blocks per
        // request". The last fetch request may be too small, and we give up and put the remaining
        // merged blocks back to the input list.
        if (info.blockId.isInstanceOf[ShuffleBlockBatchId]) {
          mergedBlockInfo += info
        } else {
          if (curBlocks.isEmpty) {
            curBlocks += info
          } else {
            val curBlockId = info.blockId.asInstanceOf[ShuffleBlockId]
            val currentMapId = curBlocks.head.blockId.asInstanceOf[ShuffleBlockId].mapId
            if (curBlockId.mapId != currentMapId) {
              mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
              curBlocks.clear()
            }
            curBlocks += info
          }
        }
      }
      if (curBlocks.nonEmpty) {
        mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
      }
      mergedBlockInfo
    } else {
      blocks
    }
    result
  }

  /**
   * The block information to fetch used in FetchRequest.
   * @param blockId block id
   * @param size estimated size of the block. Note that this is NOT the exact bytes.
   *             Size of remote block is used to calculate bytesInFlight.
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage.
   */
  private[storage] case class FetchBlockInfo(
    blockId: BlockId,
    size: Long,
    mapIndex: Int) //代表这个BlockId属于哪个Map任务的输出

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of the information for blocks to fetch from the same address.
   * @param forMergedMetas true if this request is for requesting push-merged meta information;
   *                       false if it is for regular or shuffle chunks.
   */
  case class FetchRequest(
      address: BlockManagerId,
      blocks: collection.Seq[FetchBlockInfo],
      forMergedMetas: Boolean = false) {
    val size = blocks.map(_.size).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  //请求结果
  private[storage] sealed trait FetchResult

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage.
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block. Note that this is NOT the exact bytes.
   *             Size of remote block is used to calculate bytesInFlight.
   * @param buf `ManagedBuffer` for the content.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   */
  //成功请求
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  //请求失败
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult

  /**
   * Result of a fetch request that should be deferred for some reasons, e.g., Netty OOM
   */
   //需要延迟处理的请求
  private[storage]
  case class DeferFetchRequestResult(fetchRequest: FetchRequest) extends FetchResult

  /**
   * Result of an un-successful fetch of either of these:
   * 1) Remote shuffle chunk.
   * 2) Local push-merged block.
   *
   * Instead of treating this as a [[FailureFetchResult]], we fallback to fetch the original blocks.
   *
   * @param blockId block id
   * @param address BlockManager that the push-merged block was attempted to be fetched from
   * @param size size of the block, used to update bytesInFlight.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch
   *                         request. Used to update reqsInFlight.
   */
  private[storage] case class FallbackOnPushMergedFailureResult(blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      isNetworkReqDone: Boolean) extends FetchResult

  /**
   * Result of a successful fetch of meta information for a remote push-merged block.
   *
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param blockSize size of each push-merged block.
   * @param bitmaps bitmaps for every chunk.
   * @param address BlockManager that the meta was fetched from.
   */
  private[storage] case class PushMergedRemoteMetaFetchResult(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      blockSize: Long,
      bitmaps: Array[RoaringBitmap],
      address: BlockManagerId) extends FetchResult

  /**
   * Result of a failure while fetching the meta information for a remote push-merged block.
   *
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param address BlockManager that the meta was fetched from.
   */
  private[storage] case class PushMergedRemoteMetaFailedFetchResult(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      address: BlockManagerId) extends FetchResult

  /**
   * Result of a successful fetch of meta information for a push-merged-local block.
   *
   * @param shuffleId shuffle id.
   * @param shuffleMergeId shuffleMergeId is used to uniquely identify merging process
   *                       of shuffle by an indeterminate stage attempt.
   * @param reduceId reduce id.
   * @param bitmaps bitmaps for every chunk.
   * @param localDirs local directories where the push-merged shuffle files are storedl
   */
  private[storage] case class PushMergedLocalMetaFetchResult(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      bitmaps: Array[RoaringBitmap],
      localDirs: Array[String]) extends FetchResult
}
