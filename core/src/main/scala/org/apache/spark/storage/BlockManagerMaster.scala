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

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.Iterable
import scala.concurrent.Future

import org.apache.spark.SparkConf
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

//管理存储块（Block）的驱动端（Driver-side）类，负责和各个 BlockManager 进行通信，管理数据块的存储和生命周期。
// 该类的功能包括注册、查询、删除存储块，处理执行器（Executor）的相关信息等
private[spark] class BlockManagerMaster(
    var driverEndpoint: RpcEndpointRef, //表示与driver端通信的远程RPC端点，用于发送和接收来自驱动端的消息
    var driverHeartbeatEndPoint: RpcEndpointRef, //表示与driver端心跳相关的远程RPC端点，用于检查执行器的健康状态
    conf: SparkConf,
    isDriver: Boolean) //表示当前是否是驱动节点。用于判断是否执行与驱动端相关的操作
  extends Logging {

  val timeout = RpcUtils.askRpcTimeout(conf)
  //向驱动端发送 RemoveExecutor 消息，要求移除指定的Executor
  /** Remove a dead executor from the driver endpoint. This is only called on the driver side. */
  def removeExecutor(execId: String): Unit = {
    tell(RemoveExecutor(execId))
    logInfo("Removed " + execId + " successfully in removeExecutor")
  }

  /** Decommission block managers corresponding to given set of executors
   * Non-blocking.
   * 向driver发送DecommissionBlockManagers消息，通知driver停用指定的执行器
   */
  def decommissionBlockManagers(executorIds: Seq[String]): Unit = {
    driverEndpoint.ask[Boolean](DecommissionBlockManagers(executorIds))
  }
  //向驱动端查询，获取指定 BlockManagerId 存储的 RDD 块的复制信息
  /** Get Replication Info for all the RDD blocks stored in given blockManagerId */
  def getReplicateInfoForRDDBlocks(blockManagerId: BlockManagerId): Seq[ReplicateBlock] = {
    driverEndpoint.askSync[Seq[ReplicateBlock]](GetReplicateInfoForRDDBlocks(blockManagerId))
  }

  /** Request removal of a dead executor from the driver endpoint.
   *  This is only called on the driver side. Non-blocking
   *  向驱动端发送 RemoveExecutor 消息，异步请求移除执行器
   */
  def removeExecutorAsync(execId: String): Unit = {
    driverEndpoint.ask[Boolean](RemoveExecutor(execId))
    logInfo("Removal of executor " + execId + " requested")
  }

  /** 向驱动端发送 RegisterBlockManager 消息，注册 BlockManager，并获得包含拓扑信息的更新 BlockManagerId
   * Register the BlockManager's id with the driver. The input BlockManagerId does not contain
   * topology information. This information is obtained from the master and we respond with an
   * updated BlockManagerId fleshed out with this information.
   */
  def registerBlockManager(
      id: BlockManagerId,
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      storageEndpoint: RpcEndpointRef,
      isReRegister: Boolean = false): BlockManagerId = {
    logInfo(s"Registering BlockManager $id")
    val updatedId = driverEndpoint.askSync[BlockManagerId](
      RegisterBlockManager(
        id,
        localDirs,
        maxOnHeapMemSize,
        maxOffHeapMemSize,
        storageEndpoint,
        isReRegister
      )
    )
    if (updatedId.executorId == BlockManagerId.INVALID_EXECUTOR_ID) {
      assert(isReRegister, "Got invalid executor id from non re-register case")
      logInfo(s"Re-register BlockManager $id failed")
    } else {
      logInfo(s"Registered BlockManager $updatedId")
    }
    updatedId
  }
  //向驱动端发送 UpdateBlockInfo 消息，更新指定块的存储级别、内存大小和磁盘大小
  def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {
    val res = driverEndpoint.askSync[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    logDebug(s"Updated info of block $blockId")
    res
  }
  //向驱动端发送 UpdateRDDBlockTaskInfo 消息，更新 RDD 块的任务信息
  def updateRDDBlockTaskInfo(blockId: RDDBlockId, taskId: Long): Unit = {
    driverEndpoint.askSync[Unit](UpdateRDDBlockTaskInfo(blockId, taskId))
  }
  //向驱动端发送 UpdateRDDBlockVisibility 消息，更新指定任务的 RDD 块可见性
  def updateRDDBlockVisibility(taskId: Long, visible: Boolean): Unit = {
    driverEndpoint.ask[Unit](UpdateRDDBlockVisibility(taskId, visible))
  }
  //向驱动端查询 RDD 块是否可见，返回布尔值
  /** Check whether a block is visible */
  def isRDDBlockVisible(blockId: RDDBlockId): Boolean = {
    driverEndpoint.askSync[Boolean](GetRDDBlockVisibility(blockId))
  }
 //向驱动端请求块的位置，返回包含块所在 BlockManagerId 的列表
  /** Get locations of the blockId from the driver */
  def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](GetLocations(blockId))
  }
  //向驱动端查询块的位置信息和状态，返回 BlockLocationsAndStatus（包含位置和状态信息）
  /** Get locations as well as status of the blockId from the driver */
  def getLocationsAndStatus(
      blockId: BlockId,
      requesterHost: String): Option[BlockLocationsAndStatus] = {
    driverEndpoint.askSync[Option[BlockLocationsAndStatus]](
      GetLocationsAndStatus(blockId, requesterHost))
  }
  //向驱动端查询多个块的位置，返回每个块对应的 BlockManagerId 列表
  /** Get locations of multiple blockIds from the driver */
  def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    driverEndpoint.askSync[IndexedSeq[Seq[BlockManagerId]]](
      GetLocationsMultipleBlockIds(blockIds))
  }

  /**检查 BlockManagerMaster 是否拥有指定块
   * Check if block manager master has a block. Note that this can be used to check for only
   * those blocks that are reported to block manager master.
   */
  def contains(blockId: BlockId): Boolean = {
    getLocations(blockId).nonEmpty
  }
  //向驱动端查询指定 BlockManagerId 的同伴节点（其它 BlockManager）信息
  /** Get ids of other nodes in the cluster from the driver */
  def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](GetPeers(blockManagerId))
  }

  /** 获取执行器在 shuffle 时推送/合并的服务位置
   * Get a list of unique shuffle service locations where an executor is successfully
   * registered in the past for block push/merge with push based shuffle.
   */
  def getShufflePushMergerLocations(
      numMergersNeeded: Int,
      hostsToFilter: Set[String]): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](
      GetShufflePushMergerLocations(numMergersNeeded, hostsToFilter))
  }

  /** 移除某个主机的 shuffle 推送合并位置
   * Remove the host from the candidate list of shuffle push mergers. This can be
   * triggered if there is a FetchFailedException on the host
   * @param host
   */
  def removeShufflePushMergerLocation(host: String): Unit = {
    driverEndpoint.askSync[Unit](RemoveShufflePushMergerLocation(host))
  }
  //获取执行器的 RPC 端点引用
  def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    driverEndpoint.askSync[Option[RpcEndpointRef]](GetExecutorEndpointRef(executorId))
  }

  /** 向驱动端请求移除指定的存储块
   * Remove a block from the storage endpoints that have it. This can only be used to remove
   * blocks that the driver knows about.
   */
  def removeBlock(blockId: BlockId): Unit = {
    driverEndpoint.askSync[Boolean](RemoveBlock(blockId))
  }
  //向驱动端请求移除指定 RDD 的所有块，并根据 blocking 参数决定是否阻塞
  /** Remove all blocks belonging to the given RDD. */
  def removeRdd(rddId: Int, blocking: Boolean): Unit = {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](RemoveRdd(rddId))
    future.failed.foreach(e =>
      logWarning(s"Failed to remove RDD $rddId - ${e.getMessage}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
      // the underlying Futures will timeout anyway, so it's safe to use infinite timeout here
      RpcUtils.INFINITE_TIMEOUT.awaitResult(future)
    }
  }
 //向驱动端请求移除指定 Shuffle 的所有块，并根据 blocking 参数决定是否阻塞
  /** Remove all blocks belonging to the given shuffle. */
  def removeShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    val future = driverEndpoint.askSync[Future[Seq[Boolean]]](RemoveShuffle(shuffleId))
    future.failed.foreach(e =>
      logWarning(s"Failed to remove shuffle $shuffleId - ${e.getMessage}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
      // the underlying Futures will timeout anyway, so it's safe to use infinite timeout here
      RpcUtils.INFINITE_TIMEOUT.awaitResult(future)
    }
  }
  //向驱动端请求移除指定广播的所有块，并根据 blocking 参数决定是否阻塞
  /** Remove all blocks belonging to the given broadcast. */
  def removeBroadcast(broadcastId: Long, removeFromMaster: Boolean, blocking: Boolean): Unit = {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](
      RemoveBroadcast(broadcastId, removeFromMaster))
    future.failed.foreach(e =>
      logWarning(s"Failed to remove broadcast $broadcastId" +
        s" with removeFromMaster = $removeFromMaster - ${e.getMessage}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
      // the underlying Futures will timeout anyway, so it's safe to use infinite timeout here
      RpcUtils.INFINITE_TIMEOUT.awaitResult(future)
    }
  }

  /**向驱动端查询所有 BlockManager 的内存使用情况，返回内存分配和剩余内存的映射
   * Return the memory status for each block manager, in the form of a map from
   * the block manager's id to two long values. The first value is the maximum
   * amount of memory allocated for the block manager, while the second is the
   * amount of remaining memory.
   */
  def getMemoryStatus: Map[BlockManagerId, (Long, Long)] = {
    if (driverEndpoint == null) return Map.empty
    driverEndpoint.askSync[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }
  //向驱动端查询存储状态，返回 StorageStatus 数组
  def getStorageStatus: Array[StorageStatus] = {
    if (driverEndpoint == null) return Array.empty
    driverEndpoint.askSync[Array[StorageStatus]](GetStorageStatus)
  }

  /**
   * Return the block's status on all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   * 向驱动端查询指定块的状态，返回每个 BlockManager 上的块状态
   * If askStorageEndpoints is true, this invokes the master to query each block manager for the
   * most updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
   */
  def getBlockStatus(
      blockId: BlockId,
      askStorageEndpoints: Boolean = true): Map[BlockManagerId, BlockStatus] = {
    val msg = GetBlockStatus(blockId, askStorageEndpoints)
    /*
     * To avoid potential deadlocks, the use of Futures is necessary, because the master endpoint
     * should not block on waiting for a block manager, which can in turn be waiting for the
     * master endpoint for a response to a prior message.
     */
    val response = driverEndpoint.
      askSync[Map[BlockManagerId, Future[Option[BlockStatus]]]](msg)
    val (blockManagerIds, futures) = response.unzip
    val cbf =
      implicitly[
        CanBuildFrom[Iterable[Future[Option[BlockStatus]]],
        Option[BlockStatus],
        Iterable[Option[BlockStatus]]]]
    val blockStatus = timeout.awaitResult(
      Future.sequence(futures)(cbf, ThreadUtils.sameThread))
    if (blockStatus == null) {
      throw SparkCoreErrors.blockStatusQueryReturnedNullError(blockId)
    }
    blockManagerIds.zip(blockStatus).flatMap { case (blockManagerId, status) =>
      status.map { s => (blockManagerId, s) }
    }.toMap
  }

  /**
   * Return a list of ids of existing blocks such that the ids match the given filter. NOTE: This
   * is a potentially expensive operation and should only be used for testing.
   * 向驱动端查询符合给定过滤条件的块 ID 列表
   * If askStorageEndpoints is true, this invokes the master to query each block manager for the
   * most updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
   */
  def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askStorageEndpoints: Boolean): Seq[BlockId] = {
    val msg = GetMatchingBlockIds(filter, askStorageEndpoints)
    val future = driverEndpoint.askSync[Future[Seq[BlockId]]](msg)
    timeout.awaitResult(future)
  }
  //停止驱动端的 BlockManagerMaster，并释放相关资源
  /** Stop the driver endpoint, called only on the Spark driver node */
  def stop(): Unit = {
    if (driverEndpoint != null && isDriver) {
      tell(StopBlockManagerMaster)
      driverEndpoint = null
      if (driverHeartbeatEndPoint.askSync[Boolean](StopBlockManagerMaster)) {
        driverHeartbeatEndPoint = null
      } else {
        logWarning("Failed to stop BlockManagerMasterHeartbeatEndpoint")
      }
      logInfo("BlockManagerMaster stopped")
    }
  }
  //通过driverEndpoint发送消息
  /** Send a one-way message to the master endpoint, to which we expect it to reply with true. */
  private def tell(message: Any): Unit = {
    if (!driverEndpoint.askSync[Boolean](message)) {
      throw SparkCoreErrors.unexpectedBlockManagerMasterEndpointResultError()
    }
  }

}

private[spark] object BlockManagerMaster {
  val DRIVER_ENDPOINT_NAME = "BlockManagerMaster"
  val DRIVER_HEARTBEAT_ENDPOINT_NAME = "BlockManagerMasterHeartbeat"
}
