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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.file.Files
import java.util.{Collections, Map => JMap}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.{ExecutorDiskUtils, MergedBlockMeta}
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashSet

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 */

// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
//  Spark 中负责管理和解析本地磁盘上的 Shuffle 数据块的核心类。它扮演着一个“文件系统管理器”和“数据索引器”的角色
//  主要工作原理是索引文件（.index） 和 数据文件（.data） 的分离存储
//  数据文件 (.data)：一个 Map 任务的所有 Shuffle 输出数据（对应多个分区）被写入到一个大的文件中
//  索引文件 (.index)：一个单独的文件，存储了数据文件中每个分区的起始和结束偏移量
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    // var for testing
    var _blockManager: BlockManager = null, // 块管理器实例，用于管理数据块的存储和访问
    // 用于记录每个 shuffleId 对应哪些 Map 任务（mapId）。这在数据迁移和清理等场景下很有用
    val taskIdMapsForShuffle: JMap[Int, OpenHashSet[Long]] = Collections.emptyMap())
  extends ShuffleBlockResolver
  with Logging with MigratableResolver {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)
  // 网络传输配置，用于创建 ManagedBuffer 时配置网络传输属性
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
  // 指定最大允许的 shuffle 磁盘使用量，超过该值会抛出异常。
  private val remoteShuffleMaxDisk: Option[Long] =
    conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_MAX_DISK_SIZE)
  //获取指定 shuffleId 和 mapId 的 shuffle 数据文件。如果不指定目录，则从默认目录获取
  def getDataFile(shuffleId: Int, mapId: Long): File = getDataFile(shuffleId, mapId, None)

  /** 获取当前存储的所有 shuffle 数据块信息，返回匹配的 ShuffleBlockInfo
   *  主要用于块迁移
   * Get the shuffle files that are stored locally. Used for block migrations.
   */
  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = {
    val allBlocks = blockManager.diskBlockManager.getAllBlocks()
    allBlocks.flatMap {
      case ShuffleIndexBlockId(shuffleId, mapId, _) =>
        Some(ShuffleBlockInfo(shuffleId, mapId))
      case _ =>
        None
    }
  }

  // 获取全部shuffle文件的大小
  private def getShuffleBytesStored(): Long = {
    val shuffleFiles: Seq[File] = getStoredShuffles().map {
      si => getDataFile(si.shuffleId, si.mapId)
    }
    shuffleFiles.map(_.length()).sum
  }
  //创建一个临时文件，用于后续的文件重命名操作
  /** Create a temporary file that will be renamed to the final resulting file */
  def createTempFile(file: File): File = {
    blockManager.diskBlockManager.createTempFileWith(file)
  }

  /**
   * Get the shuffle data file.
   * When the dirs parameter is None then use the disk manager's local directories. Otherwise,
   * read from the specified directories.
   */
   //获取指定 shuffleId 和 mapId 的 shuffle 数据文件。如果不指定目录，则从默认目录获取
   def getDataFile(shuffleId: Int, mapId: Long, dirs: Option[Array[String]]): File = {
    val blockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    dirs
      .map(d =>
        new File(ExecutorDiskUtils.getFilePath(d, blockManager.subDirsPerLocalDir, blockId.name)))
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }

  /**
   * Get the shuffle index file.
   * When the dirs parameter is None then use the disk manager's local directories. Otherwise,
   * read from the specified directories.
   */
  // 获取shuffle的索引文件
  def getIndexFile(
      shuffleId: Int,
      mapId: Long,
      dirs: Option[Array[String]] = None): File = {
    val blockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    dirs
      .map(d =>
        new File(ExecutorDiskUtils.getFilePath(d, blockManager.subDirsPerLocalDir, blockId.name)))
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }

  //获取合并数据块文件
  private def getMergedBlockDataFile(
      appId: String,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      dirs: Option[Array[String]] = None): File = {
    blockManager.diskBlockManager.getMergedShuffleFile(
      ShuffleMergedDataBlockId(appId, shuffleId, shuffleMergeId, reduceId), dirs)
  }
  //获取合并快索引文件
  private def getMergedBlockIndexFile(
      appId: String,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      dirs: Option[Array[String]] = None): File = {
    blockManager.diskBlockManager.getMergedShuffleFile(
      ShuffleMergedIndexBlockId(appId, shuffleId, shuffleMergeId, reduceId), dirs)
  }
  //获取合并快元数据文件
  private def getMergedBlockMetaFile(
      appId: String,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      dirs: Option[Array[String]] = None): File = {
    blockManager.diskBlockManager.getMergedShuffleFile(
      ShuffleMergedMetaBlockId(appId, shuffleId, shuffleMergeId, reduceId), dirs)
  }

  /** 删除数据、索引文件
   * Remove data file and index file that contain the output data from one map.
   */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists() && !file.delete()) {
      logWarning(s"Error deleting data ${file.getPath()}")
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists() && !file.delete()) {
      logWarning(s"Error deleting index ${file.getPath()}")
    }

    file = getChecksumFile(shuffleId, mapId, conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM))
    if (file.exists() && !file.delete()) {
      logWarning(s"Error deleting checksum ${file.getPath()}")
    }
  }

  /** 检查索引文件和数据文件是否匹配。检查通过比较索引文件的偏移量与数据文件的大小，确保两者的关系一致
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // the index file should have `block + 1` longs as offset.
    if (index.length() != (blocks + 1) * 8L) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in = try {
      new DataInputStream(new NioBufferedFileInputStream(index))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file
    if (data.length() == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  /** 将 shuffle 块以流的形式存储
   * Write a provided shuffle block as a stream. Used for block migrations.
   * ShuffleBlockBatchIds must contain the full range represented in the ShuffleIndexBlock.
   * Requires the caller to delete any shuffle index blocks where the shuffle block fails to
   * put.
   */
  override def putShuffleBlockAsStream(blockId: BlockId, serializerManager: SerializerManager):
      StreamCallbackWithID = {
    // Throw an exception if we have exceeded maximum shuffle files stored
    remoteShuffleMaxDisk.foreach { maxBytes =>
      val bytesUsed = getShuffleBytesStored()
      if (maxBytes < bytesUsed) {
        throw SparkException.internalError(
          s"Not storing remote shuffles $bytesUsed exceeds $maxBytes", category = "SHUFFLE")
      }
    }
    val file = blockId match {
      case ShuffleIndexBlockId(shuffleId, mapId, _) =>
        getIndexFile(shuffleId, mapId)
      case ShuffleDataBlockId(shuffleId, mapId, _) =>
        getDataFile(shuffleId, mapId)
      case _ =>
        throw SparkException.internalError(s"Unexpected shuffle block transfer $blockId as " +
          s"${blockId.getClass().getSimpleName()}", category = "SHUFFLE")
    }
    val fileTmp = createTempFile(file)

    // Shuffle blocks' file bytes are being sent directly over the wire, so there is no need to
    // serializerManager.wrapStream() on it. Meaning if it was originally encrypted, then
    // it will stay encrypted when being written out to the file here.
    val channel = Channels.newChannel(new FileOutputStream(fileTmp))

    new StreamCallbackWithID {

      override def getID: String = blockId.name

      override def onData(streamId: String, buf: ByteBuffer): Unit = {
        while (buf.hasRemaining) {
          channel.write(buf)
        }
      }

      override def onComplete(streamId: String): Unit = {
        logTrace(s"Done receiving shuffle block $blockId, now storing on local disk.")
        channel.close()
        val diskSize = fileTmp.length()
        this.synchronized {
          if (file.exists()) {
            file.delete()
          }
          if (!fileTmp.renameTo(file)) {
            throw SparkCoreErrors.failedRenameTempFileError(fileTmp, file)
          }
        }
        blockId match {
          case ShuffleIndexBlockId(shuffleId, mapId, _) =>
            val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
              shuffleId, _ => new OpenHashSet[Long](8)
            )
            mapTaskIds.add(mapId)

          case ShuffleDataBlockId(shuffleId, mapId, _) =>
            val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
              shuffleId, _ => new OpenHashSet[Long](8)
            )
            mapTaskIds.add(mapId)

          case _ => // Unreachable
        }
        blockManager.reportBlockStatus(blockId, BlockStatus(StorageLevel.DISK_ONLY, 0, diskSize))
      }

      override def onFailure(streamId: String, cause: Throwable): Unit = {
        // the framework handles the connection itself, we just need to do local cleanup
        logWarning(s"Error while uploading $blockId", cause)
        channel.close()
        fileTmp.delete()
      }
    }
  }

  /** 获取要迁移的 shuffle 块的数据和索引。它返回一对 (BlockId, ManagedBuffer)，表示每个 shuffle 块的数据和索引
   * Get the index & data block for migration.
   */
  def getMigrationBlocks(shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    try {
      val shuffleId = shuffleBlockInfo.shuffleId
      val mapId = shuffleBlockInfo.mapId
      // Load the index block
      val indexFile = getIndexFile(shuffleId, mapId)
      val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
      val indexFileSize = indexFile.length()
      val indexBlockData = new FileSegmentManagedBuffer(
        transportConf, indexFile, 0, indexFileSize)

      // Load the data block
      val dataFile = getDataFile(shuffleId, mapId)
      val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
      val dataBlockData = new FileSegmentManagedBuffer(
        transportConf, dataFile, 0, dataFile.length())

      // Make sure the index exist.
      if (!indexFile.exists()) {
        throw SparkException.internalError("Index file is deleted already.", category = "SHUFFLE")
      }
      if (dataFile.exists()) {
        List((dataBlockId, dataBlockData), (indexBlockId, indexBlockData))
      } else {
        List((indexBlockId, indexBlockData))
      }
    } catch {
      case _: Exception => // If we can't load the blocks ignore them.
        logWarning(s"Failed to resolve shuffle block ${shuffleBlockInfo}. " +
          "This is expected to occur if a block is removed after decommissioning has started.")
        List.empty[(BlockId, ManagedBuffer)]
    }
  }


  /**
   * Commit the data and metadata files as an atomic operation, use the existing ones, or
   * replace them with new ones. Note that the metadata parameters (`lengths`, `checksums`)
   * will be updated to match the existing ones if use the existing ones.
   *
   * There're two kinds of metadata files:
   *
   * - index file
   * An index file contains the offsets of each block, plus a final offset at the end
   * for the end of the output file. It will be used by [[getBlockData]] to figure out
   * where each block begins and ends.
   *
   * - checksum file (optional)
   * An checksum file contains the checksum of each block. It will be used to diagnose
   * the cause when a block is corrupted. Note that empty `checksums` indicate that
   * checksum is disabled.
   */
    //将 shuffle 的元数据（索引和校验和）写入临时文件，并在写入完成后提交（重命名为目标文件）。确保写入是原子性的
  def writeMetadataFileAndCommit(
      shuffleId: Int,
      mapId: Long,
      lengths: Array[Long],
      checksums: Array[Long],
      dataTmp: File): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = createTempFile(indexFile)

    val checksumEnabled = checksums.nonEmpty
    val (checksumFileOpt, checksumTmpOpt) = if (checksumEnabled) {
      assert(lengths.length == checksums.length,
        "The size of partition lengths and checksums should be equal")
      val checksumFile =
        getChecksumFile(shuffleId, mapId, conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM))
      (Some(checksumFile), Some(createTempFile(checksumFile)))
    } else {
      (None, None)
    }

    try {
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      this.synchronized {
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (checksumEnabled) {
            val existingChecksums = getChecksums(checksumFileOpt.get, checksums.length)
            if (existingChecksums != null) {
              System.arraycopy(existingChecksums, 0, checksums, 0, lengths.length)
            } else {
              // It's possible that the previous task attempt succeeded writing the
              // index file and data file but failed to write the checksum file. In
              // this case, the current task attempt could write the missing checksum
              // file by itself.
              writeMetadataFile(checksums, checksumTmpOpt.get, checksumFileOpt.get, false)
            }
          }
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.

          val offsets = lengths.scanLeft(0L)(_ + _)
          writeMetadataFile(offsets, indexTmp, indexFile, true)

          if (dataFile.exists()) {
            dataFile.delete()
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw SparkCoreErrors.failedRenameTempFileError(dataTmp, dataFile)
          }

          // write the checksum file
          checksumTmpOpt.zip(checksumFileOpt).foreach { case (checksumTmp, checksumFile) =>
            try {
              writeMetadataFile(checksums, checksumTmp, checksumFile, false)
            } catch {
              case e: Exception =>
                // It's not worthwhile to fail here after index file and data file are
                // already successfully stored since checksum is only a best-effort for
                // the corner error case.
                logError("Failed to write checksum file", e)
            }
          }
        }
      }
    } finally {
      logDebug(s"Shuffle index for mapId $mapId: ${lengths.mkString("[", ",", "]")}")
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
      checksumTmpOpt.foreach { checksumTmp =>
        if (checksumTmp.exists()) {
          try {
            if (!checksumTmp.delete()) {
              logError(s"Failed to delete temporary checksum file " +
                s"at ${checksumTmp.getAbsolutePath}")
            }
          } catch {
            case e: Exception =>
              // Unlike index deletion, we won't propagate the error for the checksum file since
              // checksum is only a best-effort.
              logError(s"Failed to delete temporary checksum file " +
                s"at ${checksumTmp.getAbsolutePath}", e)
          }
        }
      }
    }
  }

  /**
   * Write the metadata file (index or checksum). Metadata values will be firstly write into
   * the tmp file and the tmp file will be renamed to the target file at the end to avoid dirty
   * writes.
   * @param metaValues The metadata values
   * @param tmpFile The temp file
   * @param targetFile The target file
   * @param propagateError Whether to propagate the error for file operation. Unlike index file,
   *                       checksum is only a best-effort so we won't fail the whole task due to
   *                       the error from checksum.
   */
    //将元数据写入临时文件，并重命名为目标文件。该方法用于处理索引文件和校验和文件的写入
  private def writeMetadataFile(
      metaValues: Array[Long],
      tmpFile: File,
      targetFile: File,
      propagateError: Boolean): Unit = {
    val out = new DataOutputStream(
      new BufferedOutputStream(
        new FileOutputStream(tmpFile)
      )
    )
    Utils.tryWithSafeFinally {
      metaValues.foreach(out.writeLong)
    } {
      out.close()
    }

    if (targetFile.exists()) {
      targetFile.delete()
    }

    if (!tmpFile.renameTo(targetFile)) {
      if (propagateError) {
        throw SparkCoreErrors.failedRenameTempFileError(tmpFile, targetFile)
      } else {
        logWarning(s"fail to rename file $tmpFile to $targetFile")
      }
    }
  }

  /**
   * This is only used for reading local merged block data. In such cases, all chunks in the
   * merged shuffle file need to be identified at once, so the ShuffleBlockFetcherIterator
   * knows how to consume local merged shuffle file as multiple chunks.
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    val indexFile =
      getMergedBlockIndexFile(conf.getAppId, blockId.shuffleId, blockId.shuffleMergeId,
        blockId.reduceId, dirs)
    val dataFile = getMergedBlockDataFile(conf.getAppId, blockId.shuffleId,
      blockId.shuffleMergeId, blockId.reduceId, dirs)
    // Load all the indexes in order to identify all chunks in the specified merged shuffle file.
    val size = indexFile.length.toInt
    val offsets = Utils.tryWithResource {
      new DataInputStream(Files.newInputStream(indexFile.toPath))
    } { dis =>
      val buffer = ByteBuffer.allocate(size)
      dis.readFully(buffer.array)
      buffer.asLongBuffer
    }
    // Number of chunks is number of indexes - 1
    val numChunks = size / 8 - 1
    for (index <- 0 until numChunks) yield {
      new FileSegmentManagedBuffer(transportConf, dataFile,
        offsets.get(index),
        offsets.get(index + 1) - offsets.get(index))
    }
  }

  /**
   * This is only used for reading local merged block meta data.
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    val indexFile =
      getMergedBlockIndexFile(conf.getAppId, blockId.shuffleId,
        blockId.shuffleMergeId, blockId.reduceId, dirs)
    val size = indexFile.length.toInt
    val numChunks = (size / 8) - 1
    val metaFile = getMergedBlockMetaFile(conf.getAppId, blockId.shuffleId,
      blockId.shuffleMergeId, blockId.reduceId, dirs)
    val chunkBitMaps = new FileSegmentManagedBuffer(transportConf, metaFile, 0L, metaFile.length)
    new MergedBlockMeta(numChunks, chunkBitMaps)
  }
  //从给定的校验和文件中读取每个 shuffle 块的校验和
  private[shuffle] def getChecksums(checksumFile: File, blockNum: Int): Array[Long] = {
    if (!checksumFile.exists()) return null
    val checksums = new ArrayBuffer[Long]
    // Read the checksums of blocks
    var in: DataInputStream = null
    try {
      in = new DataInputStream(new NioBufferedFileInputStream(checksumFile))
      while (checksums.size < blockNum) {
        checksums += in.readLong()
      }
    } catch {
      case _: IOException | _: EOFException =>
        return null
    } finally {
      in.close()
    }

    checksums.toArray
  }

  /**
   * Get the shuffle checksum file.
   * 获取指定 shuffleId、mapId 和算法的校验和文件
   * When the dirs parameter is None then use the disk manager's local directories. Otherwise,
   * read from the specified directories.
   */
  def getChecksumFile(
      shuffleId: Int,
      mapId: Long,
      algorithm: String,
      dirs: Option[Array[String]] = None): File = {
    val blockId = ShuffleChecksumBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    val fileName = ShuffleChecksumHelper.getChecksumFileName(blockId.name, algorithm)
    dirs
      .map(d =>
        new File(ExecutorDiskUtils.getFilePath(d, blockManager.subDirsPerLocalDir, fileName)))
      .getOrElse(blockManager.diskBlockManager.getFile(fileName))
  }

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw SparkException.internalError(
          s"unexpected shuffle block id format: $blockId", category = "SHUFFLE")
    }
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(shuffleId, mapId, dirs)

    // SPARK-22982: if this FileInputStream's position is seeked forward by another piece of code
    // which is incorrectly using our file descriptor then this code will fetch the wrong offsets
    // (which may cause a reducer to be sent a different reducer's data). The explicit position
    // checks added here were a useful debugging aid during SPARK-22982 and may help prevent this
    // class of issue from re-occurring in the future which is why they are left here even though
    // SPARK-22982 is fixed.
    val channel = Files.newByteChannel(indexFile.toPath)
    channel.position(startReduceId * 8L)
    val in = new DataInputStream(Channels.newInputStream(channel))
    try {
      val startOffset = in.readLong()
      channel.position(endReduceId * 8L)
      val endOffset = in.readLong()
      val actualPosition = channel.position()
      val expectedPosition = endReduceId * 8L + 8
      if (actualPosition != expectedPosition) {
        throw SparkException.internalError(s"SPARK-22982: Incorrect channel position after index" +
          s" file reads: expected $expectedPosition but actual position was $actualPosition.",
          category = "SHUFFLE")
      }
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(shuffleId, mapId, dirs),
        startOffset,
        endOffset - startOffset)
    } finally {
      in.close()
    }
  }

  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    Seq(
      ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID),
      ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    )
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}
