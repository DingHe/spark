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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockBatchId, ShuffleBlockId, StorageLevel}

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 * 主要用于处理基于 Netty 的远程过程调用（RPC）请求。它提供了处理不同类型的消息和数据流的功能，
 * 特别是在 Spark 中用于块存储（Block Storage）和 Shuffle 数据的管理。具体来说，
 * 这个服务器负责处理 OpenBlocks、FetchShuffleBlocks、UploadBlock 等请求，并通过 Netty 将数据块的传输过程串联起来
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class NettyBlockRpcServer(
    appId: String, //应用程序 ID，用于标识当前的 Spark 应用程序
    serializer: Serializer,
    blockManager: BlockDataManager) //负责处理 Spark 数据块的存储和管理。
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()  //每个请求的块都对应一个独立的流。

  override def receive(
      client: TransportClient, //发起请求的 TransportClient
      rpcMessage: ByteBuffer, //包含请求数据的 ByteBuffer，是客户端发送的请求
      responseContext: RpcResponseCallback): Unit = {
    val message = try {
      BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    } catch {
      case e: IllegalArgumentException if e.getMessage.startsWith("Unknown message type") =>
        logWarning(s"This could be a corrupted RPC message (capacity: ${rpcMessage.capacity()}) " +
          s"from ${client.getSocketAddress}. Please use `spark.authenticate.*` configurations " +
          "in case of security incidents.")
        throw e

      case _: IndexOutOfBoundsException | _: NegativeArraySizeException =>
        // Netty may throw non-'IOException's for corrupted buffers. In this case,
        // we ignore the entire message with warnings because we cannot trust any contents.
        logWarning(s"Ignored a corrupted RPC message (capacity: ${rpcMessage.capacity()}) " +
          s"from ${client.getSocketAddress}. Please use `spark.authenticate.*` configurations " +
          "in case of security incidents.")
        return
    }
    logTrace(s"Received request: $message")

    message match {
      case openBlocks: OpenBlocks =>  //处理打开块的请求，将请求中的块 ID 映射到 BlockManager 上的实际数据块，并注册一个流进行传输
        val blocksNum = openBlocks.blockIds.length
        val blocks = (0 until blocksNum).map { i =>
          val blockId = BlockId.apply(openBlocks.blockIds(i))
          assert(!blockId.isInstanceOf[ShuffleBlockBatchId],
            "Continuous shuffle block fetching only works for new fetch protocol.")
          blockManager.getLocalBlockData(blockId)
        }
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)
        logTrace(s"Registered streamId $streamId with $blocksNum buffers")
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)

      case fetchShuffleBlocks: FetchShuffleBlocks =>  //处理 Shuffle 块的获取请求，支持批量获取 Shuffle 块
        val blocks = fetchShuffleBlocks.mapIds.zipWithIndex.flatMap { case (mapId, index) =>
          if (!fetchShuffleBlocks.batchFetchEnabled) {
            fetchShuffleBlocks.reduceIds(index).map { reduceId =>
              blockManager.getLocalBlockData(
                ShuffleBlockId(fetchShuffleBlocks.shuffleId, mapId, reduceId))
            }
          } else {
            val startAndEndId = fetchShuffleBlocks.reduceIds(index)
            if (startAndEndId.length != 2) {
              throw SparkException.internalError("Invalid shuffle fetch request when batch mode " +
                s"is enabled: $fetchShuffleBlocks", category = "NETWORK")
            }
            Array(blockManager.getLocalBlockData(
              ShuffleBlockBatchId(
                fetchShuffleBlocks.shuffleId, mapId, startAndEndId(0), startAndEndId(1))))
          }
        }

        val numBlockIds = if (fetchShuffleBlocks.batchFetchEnabled) {
          fetchShuffleBlocks.mapIds.length
        } else {
          fetchShuffleBlocks.reduceIds.map(_.length).sum
        }

        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)
        logTrace(s"Registered streamId $streamId with $numBlockIds buffers")
        responseContext.onSuccess(
          new StreamHandle(streamId, numBlockIds).toByteBuffer)

      case uploadBlock: UploadBlock => //处理上传块的请求，将接收到的块数据存储在 BlockManager 中。如果存储失败，返回错误信息
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        val (level, classTag) = deserializeMetadata(uploadBlock.metadata)
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        val blockId = BlockId(uploadBlock.blockId)
        logDebug(s"Receiving replicated block $blockId with level ${level} " +
          s"from ${client.getSocketAddress}")
        val blockStored = blockManager.putBlockData(blockId, data, level, classTag)
        if (blockStored) {
          responseContext.onSuccess(ByteBuffer.allocate(0))
        } else {
          val exception = SparkException.internalError(
            s"Upload block for $blockId failed. This mostly happens " +
            "when there is not sufficient space available to store the block.",
            category = "NETWORK")
          responseContext.onFailure(exception)
        }

      case getLocalDirs: GetLocalDirsForExecutors =>   //返回指定执行器的本地磁盘目录，用于存储数据块
        val isIncorrectAppId = getLocalDirs.appId != appId
        val execNum = getLocalDirs.execIds.length
        if (isIncorrectAppId || execNum != 1) {
          val errorMsg = "Invalid GetLocalDirsForExecutors request: " +
            s"${if (isIncorrectAppId) s"incorrect application id: ${getLocalDirs.appId};"}" +
            s"${if (execNum != 1) s"incorrect executor number: $execNum (expected 1);"}"
          responseContext.onFailure(
            SparkException.internalError(errorMsg, category = "NETWORK"))
        } else {
          val expectedExecId = blockManager.asInstanceOf[BlockManager].executorId
          val actualExecId = getLocalDirs.execIds.head
          if (actualExecId != expectedExecId) {
            responseContext.onFailure(SparkException.internalError(
              s"Invalid executor id: $actualExecId, expected $expectedExecId.",
              category = "NETWORK"))
          } else {
            responseContext.onSuccess(new LocalDirsForExecutors(
              Map(actualExecId -> blockManager.getLocalDiskDirs).asJava).toByteBuffer)
          }
        }

      case diagnose: DiagnoseCorruption =>  //用于诊断 Shuffle 块的损坏问题，检查块的校验和是否一致
        val cause = blockManager.diagnoseShuffleBlockCorruption(
          ShuffleBlockId(diagnose.shuffleId, diagnose.mapId, diagnose.reduceId ),
          diagnose.checksum,
          diagnose.algorithm)
        responseContext.onSuccess(new CorruptionCause(cause).toByteBuffer)
    }
  }
  //用于接收数据流（上传的数据流）。客户端通过流的形式上传大块数据时会调用此方法。
  override def receiveStream(
      client: TransportClient,
      messageHeader: ByteBuffer,
      responseContext: RpcResponseCallback): StreamCallbackWithID = {
    val message =
      BlockTransferMessage.Decoder.fromByteBuffer(messageHeader).asInstanceOf[UploadBlockStream]
    val (level, classTag) = deserializeMetadata(message.metadata)
    val blockId = BlockId(message.blockId)
    logDebug(s"Receiving replicated block $blockId with level ${level} as stream " +
      s"from ${client.getSocketAddress}")
    // This will return immediately, but will setup a callback on streamData which will still
    // do all the processing in the netty thread.
    blockManager.putBlockDataAsStream(blockId, level, classTag)
  }
  //用于反序列化上传块的元数据（例如存储级别和类标签），并返回一个元组
  private def deserializeMetadata[T](metadata: Array[Byte]): (StorageLevel, ClassTag[T]) = {
    serializer
      .newInstance()
      .deserialize(ByteBuffer.wrap(metadata))
      .asInstanceOf[(StorageLevel, ClassTag[T])]
  }

  override def getStreamManager(): StreamManager = streamManager
}
