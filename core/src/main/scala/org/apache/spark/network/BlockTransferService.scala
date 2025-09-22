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

package org.apache.spark.network

import java.nio.ByteBuffer

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.storage.{BlockId, EncryptedManagedBuffer, StorageLevel}
import org.apache.spark.util.ThreadUtils

/** 用于处理块传输的抽象类，它同时作为客户端和服务端的一部分，用于管理数据的上传和下载
 * The BlockTransferService that used for fetching a set of blocks at time. Each instance of
 * BlockTransferService contains both client and server inside.
 */
private[spark]
abstract class BlockTransferService extends BlockStoreClient {

  /** blockDataManager 是一个管理本地数据块的组件，可以用于获取或存储数据块
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks. The fetchBlocks method in [[BlockStoreClient]] also
   * available only after this is invoked.
   */
  def init(blockDataManager: BlockDataManager): Unit

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  def port: Int  //此属性只有在调用init方法后才能访问。

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  def hostName: String //此属性只有在调用init方法后才能访问。

  /** 上传一个数据块到远程节点。调用时，必须在init方法完成初始化后才能使用。它返回一个Future[Unit]，表示上传操作的异步执行
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   */
  def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit]

  /** 同步地获取一个数据块。此方法是fetchBlocks的特例，它只获取一个块并且是阻塞操作。调用时，必须先调用init方法
   * A special case of [[fetchBlocks]], as it fetches only one block and is blocking.
   *
   * It is also only available after [[init]] is invoked.
   */
  def fetchBlockSync(
      host: String,
      port: Int,
      execId: String,
      blockId: String,
      tempFileManager: DownloadFileManager): ManagedBuffer = {
    // A monitor for the thread to wait on.
    val result = Promise[ManagedBuffer]() //用于将异步操作的结果包装起来并在未来提供一个值
    fetchBlocks(host, port, execId, Array(blockId),
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          result.failure(exception)
        }
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          data match {
            case f: FileSegmentManagedBuffer =>
              result.success(f)
            case e: EncryptedManagedBuffer =>
              result.success(e)
            case _ =>
              try {
                val ret = ByteBuffer.allocate(data.size.toInt)
                ret.put(data.nioByteBuffer())
                ret.flip()
                result.success(new NioManagedBuffer(ret))
              } catch {
                case e: Throwable => result.failure(e)
              }
          }
        }
      }, tempFileManager)
    ThreadUtils.awaitResult(result.future, Duration.Inf)
  }

  /** 同步地上传一个数据块到远程节点。该方法与uploadBlock类似，只是它会阻塞当前线程，直到上传操作完成。
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   *
   * This method is similar to [[uploadBlock]], except this one blocks the thread
   * until the upload finishes.
   */
  @throws[java.io.IOException]
  def uploadBlockSync(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Unit = {
    val future = uploadBlock(hostname, port, execId, blockId, blockData, level, classTag)
    ThreadUtils.awaitResult(future, Duration.Inf)
  }
}
