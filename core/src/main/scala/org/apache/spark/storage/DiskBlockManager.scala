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

import java.io.{File, IOException}
import java.nio.file.Files
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.util.UUID

import scala.collection.mutable.HashMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.shuffle.ExecutorDiskUtils
import org.apache.spark.storage.DiskBlockManager.ATTEMPT_ID_KEY
import org.apache.spark.storage.DiskBlockManager.MERGE_DIR_KEY
import org.apache.spark.storage.DiskBlockManager.MERGE_DIRECTORY
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 * 提供了与块存储位置的逻辑映射，并负责管理和创建与块相关的文件
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 *
 * ShuffleDataIO also can change the behavior of deleteFilesOnStop.
 */
private[spark] class DiskBlockManager(
    conf: SparkConf,
    var deleteFilesOnStop: Boolean,
    isDriver: Boolean)
  extends Logging {

  private[spark] val subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)  //从配置中获取每个本地目录下的子目录数量

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)  //一个包含本地存储路径的数组，这些路径用于存储块数据
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }

  private[spark] val localDirsString: Array[String] = localDirs.map(_.toString) //将 localDirs 中的路径转换为字符串形式的数组

  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir)) //一个二维数组，每个本地目录对应一个子目录数组，用于存储各个文件。每个 subDirs(i) 代表一个本地目录的子目录数组，通过同步机制确保并发安全

  // Get merge directory name, append attemptId if there is any
  private val mergeDirName =
    s"$MERGE_DIRECTORY${conf.get(config.APP_ATTEMPT_ID).map(id => s"_$id").getOrElse("")}"  //合并目录的名称，包含了应用的尝试 ID

  // Create merge directories
  createLocalDirsForMergedShuffleBlocks()

  private val shutdownHook = addShutdownHook()  //在 JVM 关闭时执行的钩子，用于清理工作

  // If either of these features are enabled, we must change permissions on block manager
  // directories and files to accomodate the shuffle service deleting files in a secure environment.
  // Parent directories are assumed to be restrictive to prevent unauthorized users from accessing
  // or modifying world readable files.
  private val permissionChangingRequired = conf.get(config.SHUFFLE_SERVICE_ENABLED) && (
    conf.get(config.SHUFFLE_SERVICE_REMOVE_SHUFFLE_ENABLED) ||
    conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED) //启用了 Shuffle 服务且需要删除文件或提取 RDD，Spark 会调整文件权限以满足安全要求
  )
  //根据 filename 计算哈希值，确定其存储在 localDirs 的哪个目录和子目录中。创建目录（如果不存在），并返回文件对象
  /** Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExecutorDiskUtils#getFilePath().
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % localDirs.length
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else {
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists()) {
          val path = newDir.toPath
          Files.createDirectory(path)
          if (permissionChangingRequired) {
            // SPARK-37618: Create dir as group writable so files within can be deleted by the
            // shuffle service in a secure setup. This will remove the setgid bit so files created
            // within won't be created with the parent folder group.
            val currentPerms = Files.getPosixFilePermissions(path)
            currentPerms.add(PosixFilePermission.GROUP_WRITE)
            Files.setPosixFilePermissions(path, currentPerms)
          }
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }

  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** 获取合并后的 Shuffle 文件。如果是合并的 BlockId（如 ShuffleMergedDataBlockId、ShuffleMergedIndexBlockId），返回其对应的合并文件
   * This should be in sync with
   * @see [[org.apache.spark.network.shuffle.RemoteBlockPushResolver#getFile(
   *     java.lang.String, java.lang.String)]]
   */
  def getMergedShuffleFile(blockId: BlockId, dirs: Option[Array[String]]): File = {
    blockId match {
      case mergedBlockId: ShuffleMergedDataBlockId =>
        getMergedShuffleFile(mergedBlockId.name, dirs)
      case mergedIndexBlockId: ShuffleMergedIndexBlockId =>
        getMergedShuffleFile(mergedIndexBlockId.name, dirs)
      case mergedMetaBlockId: ShuffleMergedMetaBlockId =>
        getMergedShuffleFile(mergedMetaBlockId.name, dirs)
      case _ =>
        throw SparkException.internalError(
          s"Only merged block ID is supported, but got $blockId", category = "STORAGE")
    }
  }

  private def getMergedShuffleFile(filename: String, dirs: Option[Array[String]]): File = {
    if (!dirs.exists(_.nonEmpty)) {
      throw SparkException.internalError(
        s"Cannot read $filename because merged shuffle dirs is empty", category = "STORAGE")
    }
    new File(ExecutorDiskUtils.getFilePath(dirs.get, subDirsPerLocalDir, filename))
  }
  //检查指定的 blockId 是否存在。
  /** Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }
 //获取所有由 DiskBlockManager 管理的文件，包括所有子目录中的文件
  /** List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files.toSeq else Seq.empty
    }
  }
 //获取所有的块 ID（通过文件名转换）。若文件名无法转换为有效的 BlockId，则跳过。
  /** List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().flatMap { f =>
      try {
        Some(BlockId(f.getName))
      } catch {
        case _: UnrecognizedBlockId =>
          // Skip files which do not correspond to blocks, for example temporary
          // files created by [[SortShuffleWriter]].
          None
      }
    }
  }

  /** 创建一个可供其他用户读取的文件，并修改文件权限，使文件对所有人可读。
   * SPARK-37618: Makes sure that the file is created as world readable. This is to get
   * around the fact that making the block manager sub dirs group writable removes
   * the setgid bit in secure Yarn environments, which prevents the shuffle service
   * from being able to read shuffle files. The outer directories will still not be
   * world executable, so this doesn't allow access to these files except for the
   * running user and shuffle service.
   */
  def createWorldReadableFile(file: File): Unit = {
    val path = file.toPath
    Files.createFile(path)
    val currentPerms = Files.getPosixFilePermissions(path)
    currentPerms.add(PosixFilePermission.OTHERS_READ)
    Files.setPosixFilePermissions(path, currentPerms)
  }

  /** 创建一个临时文件，并设置为可供其他用户读取。返回临时文件对象
   * Creates a temporary version of the given file with world readable permissions (if required).
   * Used to create block files that will be renamed to the final version of the file.
   */
  def createTempFileWith(file: File): File = {
    val tmpFile = Utils.tempFileWith(file)
    if (permissionChangingRequired) {
      // SPARK-37618: we need to make the file world readable because the parent will
      // lose the setgid bit when making it group writable. Without this the shuffle
      // service can't read the shuffle files in a secure setup.
      createWorldReadableFile(tmpFile)
    }
    tmpFile
  }
  //创建一个临时的本地块文件，并返回一个唯一的 TempLocalBlockId 和文件对象。文件名是唯一的，确保不会与现有文件冲突
  /** Produces a unique block id and File suitable for storing local intermediate results. */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }
  //创建一个临时的 Shuffle 块文件，并设置为可供其他用户读取。返回一个唯一的 TempShuffleBlockId 和文件对象
  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = TempShuffleBlockId(UUID.randomUUID())
    }
    val tmpFile = getFile(blockId)
    if (permissionChangingRequired) {
      // SPARK-37618: we need to make the file world readable because the parent will
      // lose the setgid bit when making it group writable. Without this the shuffle
      // service can't read the shuffle files in a secure setup.
      createWorldReadableFile(tmpFile)
    }
    (blockId, tmpFile)
  }

  /** 根据配置中的 spark.local.dir 路径创建本地存储目录。如果创建失败，记录错误日志并返回空数组。
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  /** 如果启用了 Push-based Shuffle，创建用于合并 Shuffle 块的目录。该方法确保 merge_manager 目录存在并且其中有适当的子目录
   * Get the list of configured local dirs storing merged shuffle blocks created by executors
   * if push based shuffle is enabled. Note that the files in this directory will be created
   * by the external shuffle services. We only create the merge_manager directories and
   * subdirectories here because currently the external shuffle service doesn't have
   * permission to create directories under application local directories.
   */
  private def createLocalDirsForMergedShuffleBlocks(): Unit = {
    if (Utils.isPushBasedShuffleEnabled(conf, isDriver = isDriver, checkSerializer = false)) {
      // Will create the merge_manager directory only if it doesn't exist under the local dir.
      Utils.getConfiguredLocalDirs(conf).foreach { rootDir =>
        try {
          val mergeDir = new File(rootDir, mergeDirName)
          if (!mergeDir.exists() || mergeDir.listFiles().length < subDirsPerLocalDir) {
            // This executor does not find merge_manager directory, it will try to create
            // the merge_manager directory and the sub directories.
            logDebug(s"Try to create $mergeDir and its sub dirs since the " +
              s"$mergeDirName dir does not exist")
            for (dirNum <- 0 until subDirsPerLocalDir) {
              val subDir = new File(mergeDir, "%02x".format(dirNum))
              if (!subDir.exists()) {
                // Only one container will create this directory. The filesystem will handle
                // any race conditions.
                createDirWithPermission770(subDir)
              }
            }
          }
          logInfo(s"Merge directory and its sub dirs get created at $mergeDir")
        } catch {
          case e: IOException =>
            logError(
              s"Failed to create $mergeDirName dir in $rootDir. Ignoring this directory.", e)
        }
      }
    }
  }

  /** 创建一个具有770权限（rwxrwx---）的目录，用于存储合并的 Shuffle 数据，确保只有特定用户和 Shuffle 服务可以访问
   * Create a directory that is writable by the group.
   * Grant the permission 770 "rwxrwx---" to the directory so the shuffle server can
   * create subdirs/files within the merge folder.
   */
  def createDirWithPermission770(dirToCreate: File): Unit = {
    var attempts = 0
    val maxAttempts = Utils.MAX_DIR_CREATION_ATTEMPTS
    var created: File = null
    while (created == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw SparkCoreErrors.failToCreateDirectoryError(dirToCreate.getAbsolutePath, maxAttempts)
      }
      try {
        dirToCreate.mkdirs()
        Files.setPosixFilePermissions(
          dirToCreate.toPath, PosixFilePermissions.fromString("rwxrwx---"))
        if (dirToCreate.exists()) {
          created = dirToCreate
        }
        logDebug(s"Created directory at ${dirToCreate.getAbsolutePath} with permission 770")
      } catch {
        case e: SecurityException =>
          logWarning(s"Failed to create directory ${dirToCreate.getAbsolutePath} " +
            s"with permission 770", e)
          created = null;
      }
    }
  }
  //获取合并目录路径和应用尝试 ID 的 JSON 字符串，通常用于日志记录和调试。
  def getMergeDirectoryAndAttemptIDJsonString(): String = {
    val mergedMetaMap: HashMap[String, String] = new HashMap[String, String]()
    mergedMetaMap.put(MERGE_DIR_KEY, mergeDirName)
    conf.get(config.APP_ATTEMPT_ID).foreach(
      attemptId => mergedMetaMap.put(ATTEMPT_ID_KEY, attemptId))
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val jsonString = mapper.writeValueAsString(mergedMetaMap)
    jsonString
  }
  //添加 JVM 关闭时执行的钩子，在 Spark 关闭时调用 doStop() 进行清理。
  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop(): Unit = {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }
  //执行停止操作，删除本地目录（如果 deleteFilesOnStop 为 true）。这会删除临时文件，释放存储资源
  private def doStop(): Unit = {
    if (deleteFilesOnStop) {
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}

private[spark] object DiskBlockManager {
  val MERGE_DIRECTORY = "merge_manager"
  val MERGE_DIR_KEY = "mergeDir"
  val ATTEMPT_ID_KEY = "attemptId"
}
