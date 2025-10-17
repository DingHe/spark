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

package org.apache.spark.sql.hive

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants._
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.{PartitionDesc, TableDesc}
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat => oldInputClass, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => newInputClass}

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, NewHadoopRDD, RDD, UnionRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A trait for subclasses that handle table scans.
 */
private[hive] sealed trait TableReader {
  def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow]

  def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[InternalRow]
}


/**
 * Helper class for scanning tables stored in Hadoop - e.g., to read Hive tables that reside in the
 * data warehouse directory.
 */
// HadoopTableReader 是 Spark SQL 读取基于 Hadoop 存储的 Hive 表数据的核心工具类。
// 它的主要作用是充当桥梁，连接 Spark 的 RDD 抽象层与 Hive/Hadoop 的底层文件读取机制（InputFormat 和 SerDe）
// 生成 RDD： 负责根据 Hive 表或分区信息，创建并配置正确的 HadoopRDD 或 NewHadoopRDD，作为 Spark 任务的起点。
// I/O 格式配置： 根据 Hive 表或分区的 TableDesc/PartitionDesc，配置 Hadoop JobConf，指定正确的 InputFormat、文件路径和 SerDe 属性。
//数据转换： 实现关键的数据格式转换逻辑，将 Hadoop Writable（原始数据块）通过 Hive 的 Deserializer 和 ObjectInspector 体系，高效地转换为 Spark 内部使用的 InternalRow 格式。
//分区值注入： 对于分区表，它负责将分区键的值（这些值存储在元数据中，而不是数据文件中）注入到最终的 InternalRow 中。
private[hive]
class HadoopTableReader(
                       // 输出属性（列），Spark 需要从表中读取的最终列列表。这些属性定义了输出 InternalRow 的 Schema。
    @transient private val attributes: Seq[Attribute],
                       // 分区键，表的分区列列表。用于在读取数据后，将元数据中的分区值填充到对应的列位置。
    @transient private val partitionKeys: Seq[Attribute],
                       // Hive 表描述，包含 Hive 表的 SerDe 类、InputFormat 类名、表属性等元数据信息。
    @transient private val tableDesc: TableDesc,
    @transient private val sparkSession: SparkSession,
    hadoopConf: Configuration) // 一个包含 Spark 和 Hive 配置信息的 Configuration 对象副本，用于配置 JobConf。
  extends TableReader with CastSupport with SQLConfHelper with Logging {

  // Hadoop honors "mapreduce.job.maps" as hint,
  // but will ignore when mapreduce.jobtracker.address is "local".
  // https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/
  // mapred-default.xml
  //
  // In order keep consistency with Hive, we will let it be 0 in local mode also.
  // 最小分区数
  // 确定生成的 RDD 应具有的最小分区数。在本地模式下为 0（基于文件块划分），否则为 mapreduce.job.maps 和 sparkContext.defaultMinPartitions 的较大值。
  private val _minSplitsPerRDD = if (sparkSession.sparkContext.isLocal) {
    0 // will splitted based on block by default.
  } else {
    math.max(hadoopConf.getInt("mapreduce.job.maps", 1),
      sparkSession.sparkContext.defaultMinPartitions)
  }

  SparkHadoopUtil.get.appendS3AndSparkHadoopHiveConfigurations(
    sparkSession.sparkContext.conf, hadoopConf)

  // 将配置 (hadoopConf) 包装成可序列化的对象并广播到集群中的所有执行器，以确保它们拥有正确的 Hadoop/Hive 配置来进行 I/O
  private val _broadcastedHadoopConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

  override def conf: SQLConf = sparkSession.sessionState.conf

  override def makeRDDForTable(hiveTable: HiveTable): RDD[InternalRow] =
    makeRDDForTable(
      hiveTable,
      Utils.classForName[Deserializer](tableDesc.getSerdeClassName),
      filterOpt = None)

  /**
   * Creates a Hadoop RDD to read data from the target table's data directory. Returns a transformed
   * RDD that contains deserialized rows.
   *
   * @param hiveTable Hive metadata for the table being scanned.
   * @param deserializerClass Class of the SerDe used to deserialize Writables read from Hadoop.
   * @param filterOpt If defined, then the filter is used to reject files contained in the data
   *                  directory being read. If None, then all files are accepted.
   */
  // 用于创建并返回一个 非分区 Hive 表 的 RDD[InternalRow]，它封装了 Hadoop 文件读取和 Hive SerDe 反序列化过程
  def makeRDDForTable(
      hiveTable: HiveTable, // Hive 表元数据
      deserializerClass: Class[_ <: Deserializer], // SerDe 类和可选的文件过滤器
      filterOpt: Option[PathFilter]): RDD[InternalRow] = {

    assert(!hiveTable.isPartitioned,
      "makeRDDForTable() cannot be called on a partitioned table, since input formats may " +
      "differ across partitions. Use makeRDDForPartitionedTable() instead.")

    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val localTableDesc = tableDesc
    val broadcastedHadoopConf = _broadcastedHadoopConf
    //应用文件过滤器
    val tablePath = hiveTable.getPath
    val inputPathStr = applyFilterIfNeeded(tablePath, filterOpt)

    // logDebug("Table input: %s".format(tablePath))
    // 根据 tableDesc 中的 InputFormat 类型（新或旧 API）创建并配置底层的 HadoopRDD 或 NewHadoopRDD。
    // 这个 RDD 的元素类型是 Writable（Hadoop 原始数据块）
    val hadoopRDD = createHadoopRDD(localTableDesc, inputPathStr)

    val attrsWithIndex = attributes.zipWithIndex
    // 将 HadoopTableReader 的输出列 (attributes) 与其在最终 InternalRow 中的索引位置进行配对，如 (Attribute, Int)，用于后续的数据填充
    val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))
    // 应用反序列化逻辑
    val deserializedHadoopRDD = hadoopRDD.mapPartitions { iter =>
      // 在执行器端，从广播变量中取出 Hadoop 配置 (Configuration 对象)
      val hconf = broadcastedHadoopConf.value.value
      val deserializer = deserializerClass.getConstructor().newInstance()
      DeserializerLock.synchronized {
        deserializer.initialize(hconf, localTableDesc.getProperties)
      }
      //转换数据行
      HadoopTableReader.fillObject(iter, deserializer, attrsWithIndex, mutableRow, deserializer)
    }

    deserializedHadoopRDD
  }

  override def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[InternalRow] = {
    val partitionToDeserializer = partitions.map(part =>
      (part, part.getDeserializer.getClass.asInstanceOf[Class[Deserializer]])).toMap
    makeRDDForPartitionedTable(partitionToDeserializer, filterOpt = None)
  }

  /**
   * Create a HadoopRDD for every partition key specified in the query. Note that for on-disk Hive
   * tables, a data directory is created for each partition corresponding to keys specified using
   * 'PARTITION BY'.
   *
   * @param partitionToDeserializer Mapping from a Hive Partition metadata object to the SerDe
   *     class to use to deserialize input Writables from the corresponding partition.
   * @param filterOpt If defined, then the filter is used to reject files contained in the data
   *     subdirectory of each partition being read. If None, then all files are accepted.
   */
  // 针对一个分区 Hive 表，为查询涉及的每个分区创建一个独立的 Spark RDD，然后将这些 RDD 联合起来，形成最终可供 Spark 处理的、包含所有数据的 RDD。
  // Spark 读取分区 Hive 表的起点，它负责配置每个分区的读取参数，并处理分区键的填充逻辑。
  // partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]]: 分区到反序列化器类的映射。 键是 Hive 分区元数据对象（包含路径和分区值），值是该分区应该使用的 SerDe/反序列化器（Deserializer）类
  // filterOpt: Option[PathFilter]: 可选的文件路径过滤器。如果存在，它将用于在每个分区的数据子目录中过滤文件。
  def makeRDDForPartitionedTable(
      partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]],
      filterOpt: Option[PathFilter]): RDD[InternalRow] = {

    // SPARK-5068:get FileStatus zand do the filtering locally when the path is not exists
    // 这段内部函数用于解决 Spark 早期版本中 不存在的分区路径 导致的性能或错误问题（如 SPARK-5068 提及）
    def verifyPartitionPath(
        partitionToDeserializer: Map[HivePartition, Class[_ <: Deserializer]]):
        Map[HivePartition, Class[_ <: Deserializer]] = {
      // 如果配置项 (spark.sql.hive.verifyPartitionPath) 关闭，则直接返回输入的分区映射，跳过验证
      if (!conf.verifyPartitionPath) {
        partitionToDeserializer
      } else {
        val existPathSet = collection.mutable.Set[String]()
        val pathPatternSet = collection.mutable.Set[String]()
        // 通过将分区路径（如 /data/year=2024/month=01）转换为路径模式（如 /data/*/*）来批量检查 HDFS 中实际存在的分区
        partitionToDeserializer.filter {
          case (partition, partDeserializer) =>
            def updateExistPathSetByPathPattern(pathPatternStr: String): Unit = {
              val pathPattern = new Path(pathPatternStr)
              val fs = pathPattern.getFileSystem(hadoopConf)
              val matches = fs.globStatus(pathPattern)
              matches.foreach(fileStatus => existPathSet += fileStatus.getPath.toString)
            }
            // convert  /demo/data/year/month/day  to  /demo/data/*/*/*/
            def getPathPatternByPath(parNum: Int, tempPath: Path): String = {
              var path = tempPath
              for (i <- (1 to parNum)) path = path.getParent
              val tails = (1 to parNum).map(_ => "*").mkString("/", "/", "/")
              path.toString + tails
            }

            val partPath = partition.getDataLocation
            val partNum = Utilities.getPartitionDesc(partition).getPartSpec.size()
            val pathPatternStr = getPathPatternByPath(partNum, partPath)
            if (!pathPatternSet.contains(pathPatternStr)) {
              pathPatternSet += pathPatternStr
              updateExistPathSetByPathPattern(pathPatternStr)
            }
            existPathSet.contains(partPath.toString)
        }
      }
    }
    // 创建 RDD 并处理每个分区
    val hivePartitionRDDs = verifyPartitionPath(partitionToDeserializer)
      .map { case (partition, partDeserializer) =>

      // 对经过验证的分区集合进行迭代映射，目标是为每个分区生成一个 RDD[InternalRow]

      val partDesc = Utilities.getPartitionDescFromTableDesc(tableDesc, partition, true)
      val partPath = partition.getDataLocation
      val inputPathStr = applyFilterIfNeeded(partPath, filterOpt)
      // Get partition field info
      val partSpec = partDesc.getPartSpec
      val partProps = partDesc.getProperties
      // 从分区属性中获取分区列的名称列表（以 / 分隔）
      val partColsDelimited: String = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
      // Partitioning columns are delimited by "/"
      // 分区列的名称序列。
      val partCols = partColsDelimited.trim().split("/").toSeq
      // 'partValues[i]' contains the value for the partitioning column at 'partCols[i]'.
      // 分区键值数组。 遍历分区列名称，从 partSpec 中提取对应的分区值（例如 2024，01），生成一个包含分区键字符串值的数组。如果 partSpec 为 null，则填充空字符串
      val partValues = if (partSpec == null) {
        Array.fill(partCols.size)(new String)
      } else {
        partCols.map(col => new String(partSpec.get(col))).toArray
      }

      val broadcastedHiveConf = _broadcastedHadoopConf
      val localDeserializer = partDeserializer
      // 创建一个可变 (Mutable) 的 SpecificInternalRow 实例，用于在 Executor 端重用，以减少 GC 开销。
      val mutableRow = new SpecificInternalRow(attributes.map(_.dataType))

      // Splits all attributes into two groups, partition key attributes and those that are not.
      // Attached indices indicate the position of each attribute in the output schema.
      // 分离分区键和非分区键属性

      val (partitionKeyAttrs, nonPartitionKeyAttrs) =
        attributes.zipWithIndex.partition { case (attr, _) =>
          partitionKeys.contains(attr)
        }
      // 定义一个函数，用于将分区值（字符串）填充到 mutableRow 中对应的位置
      def fillPartitionKeys(rawPartValues: Array[String], row: InternalRow): Unit = {
        partitionKeyAttrs.foreach { case (attr, ordinal) =>
          val partOrdinal = partitionKeys.indexOf(attr)
          row(ordinal) = cast(Literal(rawPartValues(partOrdinal)), attr.dataType).eval(null)
        }
      }

      // Fill all partition keys to the given MutableRow object
      fillPartitionKeys(partValues, mutableRow)

      val tableProperties = tableDesc.getProperties
      val avroSchemaProperties = Seq(AvroTableProperties.SCHEMA_LITERAL,
        AvroTableProperties.SCHEMA_URL).map(_.getPropName())

      // Create local references so that the outer object isn't serialized.
      val localTableDesc = tableDesc
        // RDD 创建和 mapPartitions 逻辑
        // 为当前分区创建一个标准的 HadoopRDD
      createHadoopRDD(partDesc, inputPathStr).mapPartitions { iter =>
        val hconf = broadcastedHiveConf.value.value
        val deserializer = localDeserializer.getConstructor().newInstance()
        // SPARK-13709: For SerDes like AvroSerDe, some essential information (e.g. Avro schema
        // information) may be defined in table properties. Here we should merge table properties
        // and partition properties before initializing the deserializer. Note that partition
        // properties take a higher priority here except for the Avro table properties
        // to support schema evolution: in that case the properties given at table level will
        // be used (for details please check SPARK-26836).
        // For example, a partition may have a different SerDe as the one defined in table
        // properties.
        val props = new Properties(tableProperties)
        partProps.asScala.filterNot { case (k, _) =>
          avroSchemaProperties.contains(k) && tableProperties.containsKey(k)
        }.foreach {
          case (key, value) => props.setProperty(key, value)
        }
        DeserializerLock.synchronized {
          deserializer.initialize(hconf, props)
        }
        // get the table deserializer
        val tableSerDe = localTableDesc.getDeserializerClass.getConstructor().newInstance()
        DeserializerLock.synchronized {
          tableSerDe.initialize(hconf, tableProperties)
        }

        // fill the non partition key attributes
        HadoopTableReader.fillObject(iter, deserializer, nonPartitionKeyAttrs,
          mutableRow, tableSerDe)
      }
    }.toSeq

    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.size == 0) {
      new EmptyRDD[InternalRow](sparkSession.sparkContext)
    } else {
      //如果有 RDD，则使用 UnionRDD 将所有单独的分区 RDD 联合起来，形成一个逻辑上包含所有数据的单个 RDD，这是最终返回的结果
      new UnionRDD(hivePartitionRDDs(0).context, hivePartitionRDDs)
    }
  }

  /**
   * If `filterOpt` is defined, then it will be used to filter files from `path`. These files are
   * returned in a single, comma-separated string.
   */
  // 根据一个可选的路径过滤器 (PathFilter) 来确定要读取的文件路径字符串。
  // 如果提供了过滤器，它将列出目录下所有符合条件的文件，并以逗号分隔的字符串形式返回；否则返回目录本身
  // path Hadoop Path 对象，表示数据目录
  // filterOpt  一个包含可选 PathFilter 的 Option 类型），
  // 返回一个字符串（表示输入路径或逗号分隔的文件列表
  private def applyFilterIfNeeded(path: Path, filterOpt: Option[PathFilter]): String = {
    filterOpt match {
      case Some(filter) =>
        val fs = path.getFileSystem(hadoopConf)
        val filteredFiles = fs.listStatus(path, filter).map(_.getPath.toString)
        filteredFiles.mkString(",")
      case None => path.toString
    }
  }

  /**
   * True if the new org.apache.hadoop.mapreduce.InputFormat is implemented (except
   * HiveHBaseTableInputFormat where although the new interface is implemented by base HBase class
   * the table inicialization in the Hive layer only happens via the old interface methods -
   * for more details see SPARK-32380).
   */
  // 判断hive的InputClass是新的api还是旧的api
  private def compatibleWithNewHadoopRDD(inputClass: Class[_ <: oldInputClass[_, _]]): Boolean =
    classOf[newInputClass[_, _]].isAssignableFrom(inputClass) &&
      !inputClass.getName.equalsIgnoreCase("org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat")

  /**
   * The entry of creating a RDD.
   * [SPARK-26630] Using which HadoopRDD will be decided by the input format of tables.
   * The input format of NewHadoopRDD is from `org.apache.hadoop.mapreduce` package while
   * the input format of HadoopRDD is from `org.apache.hadoop.mapred` package.
   */
  // 当了一个工厂或决策点。
  // 它的核心作用是判断 Hive 表使用的底层 Hadoop InputFormat 属于旧 API (org.apache.hadoop.mapred) 还是新 API (org.apache.hadoop.mapreduce)，
  // 并据此调用相应的私有方法来创建正确类型的 RDD (HadoopRDD 或 NewHadoopRDD)
  private def createHadoopRDD(localTableDesc: TableDesc, inputPathStr: String): RDD[Writable] = {
    val inputFormatClazz = localTableDesc.getInputFileFormatClass
    if (compatibleWithNewHadoopRDD(inputFormatClazz)) {
      createNewHadoopRDD(localTableDesc, inputPathStr)
    } else {
      createOldHadoopRDD(localTableDesc, inputPathStr)
    }
  }
  // 根据给定的分区信息和输入路径，选择并创建兼容 Hadoop 的 RDD（HadoopRDD 或 NewHadoopRDD）
  // Spark 兼容 Hive 读取数据时，从配置到实际创建数据输入 RDD 的一个关键分派（Dispatch）逻辑层
  private def createHadoopRDD(partitionDesc: PartitionDesc, inputPathStr: String): RDD[Writable] = {
    val inputFormatClazz = partitionDesc.getInputFileFormatClass
    if (compatibleWithNewHadoopRDD(inputFormatClazz)) {
      createNewHadoopRDD(partitionDesc, inputPathStr)
    } else {
      createOldHadoopRDD(partitionDesc, inputPathStr)
    }
  }

  /**
   * Creates a HadoopRDD based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each executor.
   */
  private def createOldHadoopRDD(tableDesc: TableDesc, path: String): RDD[Writable] = {
    val initializeJobConfFunc = HadoopTableReader.initializeLocalJobConfFunc(path, tableDesc) _
    val inputFormatClass = tableDesc.getInputFileFormatClass
      .asInstanceOf[Class[oldInputClass[Writable, Writable]]]
    createOldHadoopRDD(inputFormatClass, initializeJobConfFunc)
  }

  /**
   * Creates a HadoopRDD based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each executor.
   */
  private def createOldHadoopRDD(partitionDesc: PartitionDesc, path: String): RDD[Writable] = {
    val initializeJobConfFunc =
      HadoopTableReader.initializeLocalJobConfFunc(path, partitionDesc.getTableDesc) _
    val inputFormatClass = partitionDesc.getInputFileFormatClass
      .asInstanceOf[Class[oldInputClass[Writable, Writable]]]
    createOldHadoopRDD(inputFormatClass, initializeJobConfFunc)
  }

  private def createOldHadoopRDD(
      inputFormatClass: Class[oldInputClass[Writable, Writable]],
      initializeJobConfFunc: JobConf => Unit): RDD[Writable] = {
    val rdd = new HadoopRDD(
      sparkSession.sparkContext,
      _broadcastedHadoopConf,
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }

  /**
   * Creates a NewHadoopRDD based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each executor.
   */
  // 专门负责创建基于新版 Hadoop API（org.apache.hadoop.mapreduce）的 NewHadoopRDD，用于读取非分区表或以 TableDesc 为主的分区
  private def createNewHadoopRDD(tableDesc: TableDesc, path: String): RDD[Writable] = {
    val newJobConf = new JobConf(hadoopConf)
    HadoopTableReader.initializeLocalJobConfFunc(path, tableDesc)(newJobConf)
    //将获取到的 InputFormat 类引用强制转换为新版 Hadoop API (newInputClass 即 org.apache.hadoop.mapreduce.InputFormat) 所需的泛型类型
    val inputFormatClass = tableDesc.getInputFileFormatClass
      .asInstanceOf[Class[newInputClass[Writable, Writable]]]
    createNewHadoopRDD(inputFormatClass, newJobConf)
  }

  private def createNewHadoopRDD(partDesc: PartitionDesc, path: String): RDD[Writable] = {
    val newJobConf = new JobConf(hadoopConf)
    HadoopTableReader.initializeLocalJobConfFunc(path, partDesc.getTableDesc)(newJobConf)
    val inputFormatClass = partDesc.getInputFileFormatClass
      .asInstanceOf[Class[newInputClass[Writable, Writable]]]
    createNewHadoopRDD(inputFormatClass, newJobConf)
  }
  // 实际创建 NewHadoopRDD 的核心逻辑。它利用了 Spark 对新版 Hadoop I/O API（org.apache.hadoop.mapreduce）的封装，将读取数据的工作委托给 Hadoop/Hive
  private def createNewHadoopRDD(
      inputFormatClass: Class[newInputClass[Writable, Writable]],
      jobConf: JobConf): RDD[Writable] = {
    val rdd = new NewHadoopRDD(
      sparkSession.sparkContext,
      inputFormatClass, // 传入 Hive 表或分区对应的 InputFormat 类（例如 org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat），NewHadoopRDD 将使用它来读取数据。
      classOf[Writable],
      classOf[Writable],
      jobConf
    )

    // Only take the value (skip the key) because Hive works only with values.
    // 转换 RDD（取 Value）
    rdd.map(_._2)
  }

}

private[hive] object HiveTableUtil {

  // copied from PlanUtils.configureJobPropertiesForStorageHandler(tableDesc)
  // that calls Hive.get() which tries to access metastore, but it's not valid in runtime
  // it would be fixed in next version of hive but till then, we should use this instead
  def configureJobPropertiesForStorageHandler(
      tableDesc: TableDesc, conf: Configuration, input: Boolean): Unit = {
    val property = tableDesc.getProperties.getProperty(META_TABLE_STORAGE)
    val storageHandler =
      org.apache.hadoop.hive.ql.metadata.HiveUtils.getStorageHandler(conf, property)
    if (storageHandler != null) {
      val jobProperties = new java.util.LinkedHashMap[String, String]
      if (input) {
        storageHandler.configureInputJobProperties(tableDesc, jobProperties)
      } else {
        storageHandler.configureOutputJobProperties(tableDesc, jobProperties)
      }
      if (!jobProperties.isEmpty) {
        tableDesc.setJobProperties(jobProperties)
      }
    }
  }
}

/**
 * Object to synchronize on when calling org.apache.hadoop.hive.serde2.Deserializer#initialize.
 *
 * [SPARK-17398] org.apache.hive.hcatalog.data.JsonSerDe#initialize calls the non-thread-safe
 * HCatRecordObjectInspectorFactory.getHCatRecordObjectInspector, the results of which are
 * returned by JsonSerDe#getObjectInspector.
 * To protect against this bug in Hive (HIVE-15773/HIVE-21752), we synchronize on this object
 * when calling initialize on Deserializer instances that could be JsonSerDe instances.
 */
private[hive] object DeserializerLock

private[hive] object HadoopTableReader extends HiveInspectors with Logging {
  /**
   * Curried. After given an argument for 'path', the resulting JobConf => Unit closure is used to
   * instantiate a HadoopRDD.
   */
  // 一个**柯里化（Curried）函数，它在驱动程序（Driver）中被用来生成一个闭包（closure）。
  // 这个闭包最终会在执行器（Executor）**的每个任务启动前执行，用于配置该任务读取数据所需的 Hadoop JobConf 对象。
  // 第一组参数 (path, tableDesc) 在 Driver 端传入并被捕获到闭包中
  // 第二组参数 (jobConf) 在 Executor 端，作为闭包的输入，用于本地配置
  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc)(jobConf: JobConf): Unit = {
    // 将数据文件的路径 (path) 设置到当前的 jobConf 中
    // 告诉了底层的 Hadoop InputFormat（如 TextInputFormat 或 OrcInputFormat）应该从哪个文件或目录读取数据
    FileInputFormat.setInputPaths(jobConf, Seq[Path](new Path(path)): _*)
    if (tableDesc != null) {
      // 如果 Hive 表使用了存储处理器（Storage Handler）（如 HBaseStorageHandler），此行代码会配置与该存储处理器相关的 Hadoop Job 属性，确保正确的输入行为 (true 表示输入，即读取)
      HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, jobConf, true)
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
  }

  /**
   * Transform all given raw `Writable`s into `Row`s.
   *
   * @param iterator Iterator of all `Writable`s to be transformed
   * @param rawDeser The `Deserializer` associated with the input `Writable`
   * @param nonPartitionKeyAttrs Attributes that should be filled together with their corresponding
   *                             positions in the output schema
   * @param mutableRow A reusable `MutableRow` that should be filled
   * @param tableDeser Table Deserializer
   * @return An `Iterator[Row]` transformed from `iterator`
   */
  //  Spark SQL 内部用于从 Hive/Hadoop Writable 数据转换为 Spark InternalRow 格式的关键方法
  //  将 HDFS 中读取到的原始 Writable 字节流，通过 Hive 的反序列化器和 ObjectInspector 机制，高效地转化为 Spark Catalyst 引擎能够处理的内部行对象 (InternalRow)
  def fillObject(
      iterator: Iterator[Writable], // 从 Hadoop RecordReader 读取到的原始数据迭代器。每个元素是一个包含原始字节数据的 Writable 对象（如 Text 或 BytesWritable
      rawDeser: Deserializer, // 原始反序列化器。 负责将 Writable 转换为 Hive 内部的 Java 对象
      nonPartitionKeyAttrs: Seq[(Attribute, Int)], // 需要从数据文件中读取的非分区列的列表。每个元素是一个元组：(Spark 属性/列信息, 该列在最终输出 Row 中的位置)
      mutableRow: InternalRow, // 一个可重用的 InternalRow（通常是 SpecificInternalRow 或 GenericInternalRow）。Spark 会重复填充这个对象，以减少 GC 开销
      tableDeser: Deserializer): Iterator[InternalRow] = { // 表的最终反序列化器。 对应表的 Schema 定义。
    // 确定目标 ObjectInspector
    // soi（Struct Object Inspector）最终代表了我们期望从反序列化后的 Hive 对象中提取数据的结构
    val soi = if (rawDeser.getObjectInspector.equals(tableDeser.getObjectInspector)) {
      rawDeser.getObjectInspector.asInstanceOf[StructObjectInspector]
    } else {
      ObjectInspectorConverters.getConvertedOI(
        rawDeser.getObjectInspector,
        tableDeser.getObjectInspector).asInstanceOf[StructObjectInspector]
    }

    logDebug(soi.toString)
    // 准备字段引用和目标位置
    val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.map { case (attr, ordinal) =>
      soi.getStructFieldRef(attr.name) -> ordinal
    }.toArray.unzip

    /**
     * Builds specific unwrappers ahead of time according to object inspector
     * types to avoid pattern matching and branching costs per row.
     */
      // 预构建类型特化解包器
    val unwrappers: Seq[(Any, InternalRow, Int) => Unit] = fieldRefs.map {
      _.getFieldObjectInspector match {
        case oi: BooleanObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setBoolean(ordinal, oi.get(value))
        case oi: ByteObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setByte(ordinal, oi.get(value))
        case oi: ShortObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setShort(ordinal, oi.get(value))
        case oi: IntObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setInt(ordinal, oi.get(value))
        case oi: LongObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setLong(ordinal, oi.get(value))
        case oi: FloatObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setFloat(ordinal, oi.get(value))
        case oi: DoubleObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) => row.setDouble(ordinal, oi.get(value))
        case oi: HiveVarcharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveCharObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, UTF8String.fromString(oi.getPrimitiveJavaObject(value).getValue))
        case oi: HiveDecimalObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, HiveShim.toCatalystDecimal(oi, value))
        case oi: TimestampObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setLong(ordinal, DateTimeUtils.fromJavaTimestamp(oi.getPrimitiveJavaObject(value)))
        case oi: DateObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.setInt(ordinal, DateTimeUtils.fromJavaDate(oi.getPrimitiveJavaObject(value)))
        case oi: BinaryObjectInspector =>
          (value: Any, row: InternalRow, ordinal: Int) =>
            row.update(ordinal, oi.getPrimitiveJavaObject(value))
        case oi =>
          val unwrapper = unwrapperFor(oi)
          (value: Any, row: InternalRow, ordinal: Int) => row(ordinal) = unwrapper(value)
      }
    }
    // 这个转换器负责将 原始 SerDe 反序列化后生成的 Hive 对象（例如 LazyStruct）转换为 soi 所需的对象模型
    val converter = ObjectInspectorConverters.getConverter(rawDeser.getObjectInspector, soi)

    // Map each tuple to a row object
    iterator.map { value =>
      // 使用原始 SerDe 将 Writable（字节数据）反序列化为 Hive 内部的 Java 对象模型（通常是一个 LazyObject 或 LazyStruct）
      val raw = converter.convert(rawDeser.deserialize(value))
      var i = 0
      val length = fieldRefs.length
      while (i < length) {
        try {
          val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
          if (fieldValue == null) {
            mutableRow.setNullAt(fieldOrdinals(i))
          } else {
            unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
          }
          i += 1
        } catch {
          case ex: Throwable =>
            logError(s"Exception thrown in field <${fieldRefs(i).getFieldName}>")
            throw ex
        }
      }

      mutableRow: InternalRow
    }
  }
}
