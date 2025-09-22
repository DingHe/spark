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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.HadoopFSUtils


/**
 * A [[FileIndex]] that generates the list of files to process by recursively listing all the
 * files present in `paths`.
 *
 * @param rootPathsSpecified the list of root table paths to scan (some of which might be
 *                           filtered out later)
 * @param parameters as set of options to control discovery
 * @param userSpecifiedSchema an optional user specified schema that will be use to provide
 *                            types for the discovered partitions
 */
//主要用于高效地遍历和读取存储在 Hadoop 兼容文件系统（如 HDFS、S3）中的文件。
// 它支持对文件和分区进行缓存，以减少元数据开销，并提供并行文件扫描功能
class InMemoryFileIndex(
    sparkSession: SparkSession,
    rootPathsSpecified: Seq[Path], //需要扫描的根路径列表，可能包含多个目录
    parameters: Map[String, String],  //用户传递的参数，用于控制发现过程
    userSpecifiedSchema: Option[StructType], //用户提供的模式（schema），如果未提供，则从数据中推断
    fileStatusCache: FileStatusCache = NoopCache,  //缓存文件状态信息，默认使用 NoopCache（不缓存）
    userSpecifiedPartitionSpec: Option[PartitionSpec] = None, //用户提供的分区规范（可选）
    override val metadataOpsTimeNs: Option[Long] = None) //存储元数据操作的执行时间（可选）
  extends PartitioningAwareFileIndex(
    sparkSession, parameters, userSpecifiedSchema, fileStatusCache) {

  // Filter out streaming metadata dirs or files such as "/.../_spark_metadata" (the metadata dir)
  // or "/.../_spark_metadata/0" (a file in the metadata dir). `rootPathsSpecified` might contain
  // such streaming metadata dir or files, e.g. when after globbing "basePath/*" where "basePath"
  // is the output of a streaming query.
  override val rootPaths =   //如果包含元数据路径，则过滤调
    rootPathsSpecified.filterNot(FileStreamSink.ancestorIsMetadataDirectory(_, hadoopConf))
   //缓存信息
  @volatile private var cachedLeafFiles: mutable.LinkedHashMap[Path, FileStatus] = _
  @volatile private var cachedLeafDirToChildrenFiles: Map[Path, Array[FileStatus]] = _
  @volatile private var cachedPartitionSpec: PartitionSpec = _

  refresh0()

  override def partitionSpec(): PartitionSpec = {
    if (cachedPartitionSpec == null) {
      if (userSpecifiedPartitionSpec.isDefined) {
        cachedPartitionSpec = userSpecifiedPartitionSpec.get
      } else {
        cachedPartitionSpec = inferPartitioning()
      }
    }
    logTrace(s"Partition spec: $cachedPartitionSpec")
    cachedPartitionSpec
  }

  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
    cachedLeafFiles
  }

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
    cachedLeafDirToChildrenFiles
  }
  //刷新之前，使缓存数据失效
  override def refresh(): Unit = {
    fileStatusCache.invalidateAll()
    refresh0()
  }
  //刷新元数据
  private def refresh0(): Unit = {
    val files = listLeafFiles(rootPaths)
    cachedLeafFiles =
      new mutable.LinkedHashMap[Path, FileStatus]() ++= files.map(f => f.getPath -> f)
    cachedLeafDirToChildrenFiles = files.toArray.groupBy(_.getPath.getParent)
    cachedPartitionSpec = null
  }

  override def equals(other: Any): Boolean = other match {
    case hdfs: InMemoryFileIndex => rootPaths.toSet == hdfs.rootPaths.toSet
    case _ => false
  }

  override def hashCode(): Int = rootPaths.toSet.hashCode()

  /**
   * List leaf files of given paths. This method will submit a Spark job to do parallel
   * listing whenever there is a path having more files than the parallel partition discovery
   * discovery threshold.
   *
   * This is publicly visible for testing.
   */
  //列出叶子文件
  def listLeafFiles(paths: Seq[Path]): mutable.LinkedHashSet[FileStatus] = {
    val startTime = System.nanoTime()
    val output = mutable.LinkedHashSet[FileStatus]()
    val pathsToFetch = mutable.ArrayBuffer[Path]()
    for (path <- paths) {
      fileStatusCache.getLeafFiles(path) match {
        case Some(files) =>
          HiveCatalogMetrics.incrementFileCacheHits(files.length)
          output ++= files
        case None =>
          pathsToFetch += path
      }
      () // for some reasons scalac 2.12 needs this; return type doesn't matter
    }
    val filter = FileInputFormat.getInputPathFilter(new JobConf(hadoopConf, this.getClass))
    val discovered = InMemoryFileIndex.bulkListLeafFiles(
      pathsToFetch.toSeq, hadoopConf, filter, sparkSession, parameters)
    discovered.foreach { case (path, leafFiles) =>
      HiveCatalogMetrics.incrementFilesDiscovered(leafFiles.size)
      fileStatusCache.putLeafFiles(path, leafFiles.toArray)
      output ++= leafFiles
    }
    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to list leaf files" +
      s" for ${paths.length} paths.")
    output
  }
}

object InMemoryFileIndex extends Logging {
  //主要作用是 并行列出 paths 目录下的所有叶子文件，并返回一个包含 路径及其对应文件列表 的 Seq[(Path, Seq[FileStatus])] 结构
  private[sql] def bulkListLeafFiles(
      paths: Seq[Path], //需要扫描的根路径列表
      hadoopConf: Configuration,  //Hadoop 配置，用于文件系统访问
      filter: PathFilter,  //文件路径过滤器
      sparkSession: SparkSession,
      parameters: Map[String, String] = Map.empty): Seq[(Path, Seq[FileStatus])] = { //额外的参数（如是否忽略丢失的文件）
    HadoopFSUtils.parallelListLeafFiles(
      sc = sparkSession.sparkContext,
      paths = paths,
      hadoopConf = hadoopConf,
      filter = new PathFilterWrapper(filter),
      ignoreMissingFiles =
        new FileSourceOptions(CaseInsensitiveMap(parameters)).ignoreMissingFiles,
      ignoreLocality = sparkSession.sessionState.conf.ignoreDataLocality,
      parallelismThreshold = sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold,
      parallelismMax = sparkSession.sessionState.conf.parallelPartitionDiscoveryParallelism)
  }

}

private class PathFilterWrapper(val filter: PathFilter) extends PathFilter with Serializable {
  override def accept(path: Path): Boolean = {
    (filter == null || filter.accept(path)) && !HadoopFSUtils.shouldFilterOutPathName(path.getName)
  }
}
