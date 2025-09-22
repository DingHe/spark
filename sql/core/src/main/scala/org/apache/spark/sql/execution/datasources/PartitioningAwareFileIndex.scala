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

import org.apache.spark.internal.Logging
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.FileFormat.createMetadataInternalRow
import org.apache.spark.sql.types.StructType

/**
 * An abstract class that represents [[FileIndex]]s that are aware of partitioned tables.
 * It provides the necessary methods to parse partition data based on a set of files.
 *
 * @param parameters as set of options to control partition discovery
 * @param userSpecifiedSchema an optional user specified schema that will be use to provide
 *                            types for the discovered partitions
 */
abstract class PartitioningAwareFileIndex(
    sparkSession: SparkSession,
    parameters: Map[String, String], //参数列表
    userSpecifiedSchema: Option[StructType],  //用户指定的schema
    fileStatusCache: FileStatusCache = NoopCache) extends FileIndex with Logging {

  /** Returns the specification of the partitions inferred from the data. */
  def partitionSpec(): PartitionSpec  //返回从数据推断出的分区规范，包括分区列信息和具体的分区路径。
  //返回分区列的模式（Schema）
  override def partitionSchema: StructType = partitionSpec().partitionColumns
  //Hadoop 文件系统配置，用于与 HDFS 或本地文件系统交互
  protected val hadoopConf: Configuration =
    sparkSession.sessionState.newHadoopConfWithOptions(parameters)
  //存储所有叶子文件（即可直接读取的数据文件）
  protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus]
  //存储目录路径到其子文件的映射,用于文件列出时快速查找某个目录下的所有文件
  protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]]
  //提供大小写不敏感的访问方式
  private val caseInsensitiveMap = CaseInsensitiveMap(parameters)
  //存储所有 PathFilter，用于筛选符合规则的文件路径
  private val pathFilters = PathFilterFactory.create(caseInsensitiveMap)
  //检查某个文件是否符合路径过滤条件
  protected def matchPathPattern(file: FileStatus): Boolean =
    pathFilters.forall(_.accept(file))
  //指示是否需要递归查找文件
  protected lazy val recursiveFileLookup: Boolean = {
    caseInsensitiveMap.getOrElse(FileIndexOptions.RECURSIVE_FILE_LOOKUP, "false").toBoolean
  }
  //列出所有符合条件的文件，并按分区组织
  override def listFiles(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    def isNonEmptyFile(f: FileStatus): Boolean = {
      isDataPath(f.getPath) && f.getLen > 0
    }

    // retrieve the file constant metadata filters and reduce to a final filter expression that can
    // be applied to files.
    val fileMetadataFilterOpt = dataFilters.filter { f =>
      f.references.nonEmpty && f.references.forall {
        case FileSourceConstantMetadataAttribute(metadataAttr) =>
          // we only know block start and length after splitting files, so skip it here
          metadataAttr.name != FileFormat.FILE_BLOCK_START &&
            metadataAttr.name != FileFormat.FILE_BLOCK_LENGTH
        case _ => false
      }
    }.reduceOption(expressions.And)

    // - Retrieve all required metadata attributes and put them into a sequence
    // - Bind all file constant metadata attribute references to their respective index
    val requiredMetadataColumnNames: mutable.Buffer[String] = mutable.Buffer.empty
    val boundedFilterMetadataStructOpt = fileMetadataFilterOpt.map { fileMetadataFilter =>
      Predicate.createInterpreted(fileMetadataFilter.transform {
        case attr: AttributeReference =>
          val existingMetadataColumnIndex = requiredMetadataColumnNames.indexOf(attr.name)
          val metadataColumnIndex = if (existingMetadataColumnIndex >= 0) {
            existingMetadataColumnIndex
          } else {
            requiredMetadataColumnNames += attr.name
            requiredMetadataColumnNames.length - 1
          }
          BoundReference(metadataColumnIndex, attr.dataType, nullable = true)
      })
    }

    def matchFileMetadataPredicate(partitionValues: InternalRow, f: FileStatus): Boolean = {
      // use option.forall, so if there is no filter no metadata struct, return true
      boundedFilterMetadataStructOpt.forall { boundedFilter =>
        val row =
          createMetadataInternalRow(partitionValues, requiredMetadataColumnNames.toSeq,
            SparkPath.fromFileStatus(f), f.getLen, f.getModificationTime)
        boundedFilter.eval(row)
      }
    }

    val selectedPartitions = if (partitionSpec().partitionColumns.isEmpty) {
      PartitionDirectory(InternalRow.empty, allFiles().toArray
        .filter(f => isNonEmptyFile(f) && matchFileMetadataPredicate(InternalRow.empty, f))) :: Nil
    } else {
      if (recursiveFileLookup) {
        throw new IllegalArgumentException(
          "Datasource with partition do not allow recursive file loading.")
      }
      prunePartitions(partitionFilters, partitionSpec()).map {
        case PartitionPath(values, path) =>
          val files: Seq[FileStatus] = leafDirToChildrenFiles.get(path) match {
            case Some(existingDir) =>
              // Directory has children files in it, return them
              existingDir.filter(f => matchPathPattern(f) && isNonEmptyFile(f) &&
                matchFileMetadataPredicate(values, f))

            case None =>
              // Directory does not exist, or has no children files
              Nil
          }
          PartitionDirectory(values, files.toArray)
      }
    }
    logTrace("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))
    selectedPartitions
  }

  /** Returns the list of files that will be read when scanning this relation. */
    //返回所有需要读取的文件路径（URL 编码）
  override def inputFiles: Array[String] =
    allFiles().map(fs => SparkPath.fromFileStatus(fs).urlEncoded).toArray
  //返回所有文件的总大小（字节）
  override def sizeInBytes: Long = allFiles().map(_.getLen).sum
  //返回所有需要读取的文件
  def allFiles(): Seq[FileStatus] = {
    val files = if (partitionSpec().partitionColumns.isEmpty && !recursiveFileLookup) {
      // For each of the root input paths, get the list of files inside them
      rootPaths.flatMap { path =>
        // Make the path qualified (consistent with listLeafFiles and bulkListLeafFiles).
        val fs = path.getFileSystem(hadoopConf)
        val qualifiedPathPre = fs.makeQualified(path)
        val qualifiedPath: Path = if (qualifiedPathPre.isRoot && !qualifiedPathPre.isAbsolute) {
          // SPARK-17613: Always append `Path.SEPARATOR` to the end of parent directories,
          // because the `leafFile.getParent` would have returned an absolute path with the
          // separator at the end.
          new Path(qualifiedPathPre, Path.SEPARATOR)
        } else {
          qualifiedPathPre
        }

        // There are three cases possible with each path
        // 1. The path is a directory and has children files in it. Then it must be present in
        //    leafDirToChildrenFiles as those children files will have been found as leaf files.
        //    Find its children files from leafDirToChildrenFiles and include them.
        // 2. The path is a file, then it will be present in leafFiles. Include this path.
        // 3. The path is a directory, but has no children files. Do not include this path.

        leafDirToChildrenFiles.get(qualifiedPath)
          .orElse { leafFiles.get(qualifiedPath).map(Array(_)) }
          .getOrElse(Array.empty)
      }
    } else {
      leafFiles.values.toSeq
    }
    files.filter(matchPathPattern)
  }
  //推断数据集的分区结构
  protected def inferPartitioning(): PartitionSpec = {
    if (recursiveFileLookup) {
      PartitionSpec.emptySpec
    } else {
      // We use leaf dirs containing data files to discover the schema.
      val leafDirs = leafDirToChildrenFiles.filter { case (_, files) =>
        files.exists(f => isDataPath(f.getPath))
      }.keys.toSeq

      val caseInsensitiveOptions = CaseInsensitiveMap(parameters)
      val timeZoneId = caseInsensitiveOptions.get(FileIndexOptions.TIME_ZONE)
        .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)

      PartitioningUtils.parsePartitions(
        leafDirs,
        typeInference = sparkSession.sessionState.conf.partitionColumnTypeInferenceEnabled,
        basePaths = basePaths,
        userSpecifiedSchema = userSpecifiedSchema,
        caseSensitive = sparkSession.sqlContext.conf.caseSensitiveAnalysis,
        validatePartitionColumns = sparkSession.sqlContext.conf.validatePartitionColumns,
        timeZoneId = timeZoneId)
    }
  }
  //根据谓词条件裁剪分区
  private def prunePartitions(
      predicates: Seq[Expression],
      partitionSpec: PartitionSpec): Seq[PartitionPath] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = Predicate.createInterpreted(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionPath(values, _) => boundPredicate.eval(values)
      }
      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, " +
          s"pruned ${if (total == 0) "0" else s"$percentPruned%"} partitions."
      }

      selected
    } else {
      partitions
    }
  }

  /**
   * Contains a set of paths that are considered as the base dirs of the input datasets.
   * The partitioning discovery logic will make sure it will stop when it reaches any
   * base path.
   *
   * By default, the paths of the dataset provided by users will be base paths.
   * Below are three typical examples,
   * Case 1) `spark.read.parquet("/path/something=true/")`: the base path will be
   * `/path/something=true/`, and the returned DataFrame will not contain a column of `something`.
   * Case 2) `spark.read.parquet("/path/something=true/a.parquet")`: the base path will be
   * still `/path/something=true/`, and the returned DataFrame will also not contain a column of
   * `something`.
   * Case 3) `spark.read.parquet("/path/")`: the base path will be `/path/`, and the returned
   * DataFrame will have the column of `something`.
   *
   * Users also can override the basePath by setting `basePath` in the options to pass the new base
   * path to the data source.
   * For example, `spark.read.option("basePath", "/path/").parquet("/path/something=true/")`,
   * and the returned DataFrame will have the column of `something`.
   */
    //获取数据集的根目录（Base Path）
  private def basePaths: Set[Path] = {
    caseInsensitiveMap.get(FileIndexOptions.BASE_PATH_PARAM).map(new Path(_)) match {
      case Some(userDefinedBasePath) =>
        val fs = userDefinedBasePath.getFileSystem(hadoopConf)
        if (!fs.isDirectory(userDefinedBasePath)) {
          throw new IllegalArgumentException(s"Option '${FileIndexOptions.BASE_PATH_PARAM}' " +
            s"must be a directory")
        }
        val qualifiedBasePath = fs.makeQualified(userDefinedBasePath)
        val qualifiedBasePathStr = qualifiedBasePath.toString
        rootPaths
          .find(!fs.makeQualified(_).toString.startsWith(qualifiedBasePathStr))
          .foreach { rp =>
            throw new IllegalArgumentException(
              s"Wrong basePath $userDefinedBasePath for the root path: $rp")
          }
        Set(qualifiedBasePath)

      case None =>
        rootPaths.map { path =>
          // Make the path qualified (consistent with listLeafFiles and bulkListLeafFiles).
          val qualifiedPath = path.getFileSystem(hadoopConf).makeQualified(path)
          if (leafFiles.contains(qualifiedPath)) qualifiedPath.getParent else qualifiedPath }.toSet
    }
  }

  // SPARK-15895: Metadata files (e.g. Parquet summary files) and temporary files should not be
  // counted as data files, so that they shouldn't participate partition discovery.
  //判断一个路径是否是数据文件路径（排除临时文件）
  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
  }
}
