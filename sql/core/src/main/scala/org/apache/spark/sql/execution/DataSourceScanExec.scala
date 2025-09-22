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

package org.apache.spark.sql.execution

import java.util.concurrent.TimeUnit._

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.util.{truncatedString, CaseInsensitiveMap}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat => ParquetSource}
import org.apache.spark.sql.execution.datasources.v2.PushedDownOperators
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet
//表示在物理计划中进行数据源扫描的节点
trait DataSourceScanExec extends LeafExecNode {
  def relation: BaseRelation  //表示数据源的基本关系，通常是数据源的元数据表示
  def tableIdentifier: Option[TableIdentifier] //表示表的标识符，用来指定扫描的表格

  protected val nodeNamePrefix: String = ""  //用于构建节点名称时的前缀

  override val nodeName: String = {
    s"Scan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  // Metadata that describes more details of this scan.
  protected def metadata: Map[String, String]  //存储有关扫描操作的元数据信息

  protected val maxMetadataValueLength = conf.maxMetadataStringLength  //从配置中获取最大元数据字符串的长度
  //返回一个简化的字符串描述，包括节点名称、输出字段、元数据（按照最大字段数限制）等
  override def simpleString(maxFields: Int): String = {
    val metadataEntries = metadata.toSeq.sorted.map {
      case (key, value) =>
        key + ": " + StringUtils.abbreviate(redact(value), maxMetadataValueLength)
    }
    val metadataStr = truncatedString(metadataEntries, " ", ", ", "", maxFields)
    redact(
      s"$nodeNamePrefix$nodeName${truncatedString(output, "[", ",", "]", maxFields)}$metadataStr")
  }
  //返回一个详细的字符串描述，包含节点名称、输出字段和元数据，适用于更详细的日志或调试信息
  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  /**
   * Shorthand for calling redactString() without specifying redacting rules
   */
  //用于掩码字符串中的敏感信息
  protected def redact(text: String): String = {
    Utils.redact(conf.stringRedactionPattern, text)
  }

  /**
   * The data being read in.  This is to provide input to the tests in a way compatible with
   * [[InputRDDCodegen]] which all implementations used to extend.
   */
  //输入的数据
  def inputRDDs(): Seq[RDD[InternalRow]]
}

/** Physical plan node for scanning data from a relation. */
case class RowDataSourceScanExec(
    output: Seq[Attribute],
    requiredSchema: StructType,
    filters: Set[Filter],
    handledFilters: Set[Filter],
    pushedDownOperators: PushedDownOperators,
    rdd: RDD[InternalRow],
    @transient relation: BaseRelation,
    tableIdentifier: Option[TableIdentifier])
  extends DataSourceScanExec with InputRDDCodegen {

  override lazy val metrics =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map( r => {
        numOutputRows += 1
        proj(r)
      })
    }
  }

  // Input can be InternalRow, has to be turned into UnsafeRows.
  override protected val createUnsafeProjection: Boolean = true

  override def inputRDD: RDD[InternalRow] = rdd

  override val metadata: Map[String, String] = {

    def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")

    val markedFilters = if (filters.nonEmpty) {
      for (filter <- filters) yield {
        if (handledFilters.contains(filter)) s"*$filter" else s"$filter"
      }
    } else {
      handledFilters
    }

    val topNOrLimitInfo =
      if (pushedDownOperators.limit.isDefined && pushedDownOperators.sortValues.nonEmpty) {
        val pushedTopN =
          s"ORDER BY ${seqToString(pushedDownOperators.sortValues.map(_.describe()))}" +
          s" LIMIT ${pushedDownOperators.limit.get}"
        Some("PushedTopN" -> pushedTopN)
      } else {
        pushedDownOperators.limit.map(value => "PushedLimit" -> s"LIMIT $value")
      }

    val offsetInfo = pushedDownOperators.offset.map(value => "PushedOffset" -> s"OFFSET $value")

    val pushedFilters = if (pushedDownOperators.pushedPredicates.nonEmpty) {
      seqToString(pushedDownOperators.pushedPredicates.map(_.describe()))
    } else {
      seqToString(markedFilters.toSeq)
    }

    Map("ReadSchema" -> requiredSchema.catalogString,
      "PushedFilters" -> pushedFilters) ++
      pushedDownOperators.aggregation.fold(Map[String, String]()) { v =>
        Map("PushedAggregates" -> seqToString(v.aggregateExpressions.map(_.describe())),
          "PushedGroupByExpressions" -> seqToString(v.groupByExpressions.map(_.describe())))} ++
      topNOrLimitInfo ++
      offsetInfo ++
      pushedDownOperators.sample.map(v => "PushedSample" ->
        s"SAMPLE (${(v.upperBound - v.lowerBound) * 100}) ${v.withReplacement} SEED(${v.seed})"
      )
  }

  // Don't care about `rdd` and `tableIdentifier` when canonicalizing.
  override def doCanonicalize(): SparkPlan =
    copy(
      output.map(QueryPlan.normalizeExpressions(_, output)),
      rdd = null,
      tableIdentifier = None)
}

/** 通常用于处理文件数据源扫描的底层逻辑。它结合了文件扫描、文件元数据处理、动态分区裁剪和查询优化等功能。这个 trait 包含许多方法和属性，用于描述和执行文件扫描操作，特别是对文件进行筛选、统计和优化
 * A base trait for file scans containing file listing and metrics code.
 */
trait FileSourceScanLike extends DataSourceScanExec {

  // Filters on non-partition columns.
  def dataFilters: Seq[Expression]  //表示应用在数据列上的过滤条件（非分区列）
  // Disable bucketed scan based on physical query plan, see rule
  // [[DisableUnnecessaryBucketedScan]] for details.
  def disableBucketedScan: Boolean  //指示是否禁用基于物理查询计划的分桶扫描
  // Bucket ids for bucket pruning.
  def optionalBucketSet: Option[BitSet] //表示用于分桶裁剪的桶 ID 集合
  // Number of coalesced buckets.
  def optionalNumCoalescedBuckets: Option[Int] //表示合并桶的数量。桶合并是指在读取时将多个桶合并为一个桶，这通常用于减少任务数
  // Output attributes of the scan, including data attributes and partition attributes.
  def output: Seq[Attribute] //表示扫描结果的输出列，包括数据列和分区列
  // Predicates to use for partition pruning.
  def partitionFilters: Seq[Expression]  //表示应用在分区列上的过滤条件
  // The file-based relation to scan.
  def relation: HadoopFsRelation  //表示要扫描的文件源关系（Hadoop 文件系统关系）。HadoopFsRelation 包含文件格式、路径、分区模式等信息
  // Required schema of the underlying relation, excluding partition columns.
  def requiredSchema: StructType  //表示底层数据关系所需的模式（不包括分区列）
  // Identifier for the table in the metastore.
  def tableIdentifier: Option[TableIdentifier]  //表示元数据中表的标识符。用于标识扫描的表

  //收集所有的常量元数据列，通常这些列在扫描过程中需要被附加到每个数据行中，用于表示额外的元数据（例如文件路径、文件名等）
  lazy val fileConstantMetadataColumns: Seq[AttributeReference] = output.collect {
    // Collect metadata columns to be handled outside of the scan by appending constant columns.
    case FileSourceConstantMetadataAttribute(attr) => attr
  }
  //返回支持的向量化读取类型。如果文件格式支持向量化读取（例如，Parquet 或 ORC），则返回相应的向量类型，此外，还会考虑常量元数据列
  override def vectorTypes: Option[Seq[String]] =
    relation.fileFormat.vectorTypes(
      requiredSchema = requiredSchema,
      partitionSchema = relation.partitionSchema,
      relation.sparkSession.sessionState.conf).map { vectorTypes =>
        vectorTypes ++
          // for column-based file format, append metadata column's vector type classes if any
          fileConstantMetadataColumns.map { _ => classOf[ConstantColumnVector].getName }
      }
  //定义一组驱动端的 SQL 性能指标（metrics），包括文件数量、元数据时间、文件大小等。还包括分区读取的指标，如分区数和动态分区裁剪时间
  lazy val driverMetrics = Map(
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files read"),
    "metadataTime" -> SQLMetrics.createTimingMetric(sparkContext, "metadata time"),
    "filesSize" -> SQLMetrics.createSizeMetric(sparkContext, "size of files read")
  ) ++ {
    if (relation.partitionSchema.nonEmpty) {
      Map(
        "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions read"),
        "pruningTime" ->
          SQLMetrics.createTimingMetric(sparkContext, "dynamic partition pruning time"))
    } else {
      Map.empty[String, SQLMetric]
    }
  } ++ staticMetrics

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has
   * been initialized. See SPARK-26327 for more details.
   *///发送驱动端的性能指标数据
  protected def sendDriverMetrics(): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, driverMetrics.values.toSeq)
  }
  //检查给定的过滤表达式是否为动态分区裁剪过滤器
  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])
  //根据过滤条件选择需要扫描的分区。selectedPartitions 存储所有符合条件的分区目录
  @transient lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret =
      relation.location.listFiles(
        partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
    setFilesNumAndSizeMetric(ret, true)
    val timeTakenMs = NANOSECONDS.toMillis(
      (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime").set(timeTakenMs)
    ret
  }.toArray

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient protected lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)
    //对动态分区裁剪的支持。此方法会检查是否存在动态分区裁剪的过滤条件，并根据这些条件进一步筛选出需要的分区
    if (dynamicPartitionFilters.nonEmpty) {
      val startTime = System.nanoTime()
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      }, Nil)
      val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
      setFilesNumAndSizeMetric(ret, false)
      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      driverMetrics("pruningTime").set(timeTakenMs)
      ret
    } else {
      selectedPartitions
    }
  }

  /**
   * [[partitionFilters]] can contain subqueries whose results are available only at runtime so
   * accessing [[selectedPartitions]] should be guarded by this method during planning
   */ //检查查询计划中是否存在运行时子查询（这些子查询的结果只在运行时可用），如果有，则需要在运行时决定分区
  private def hasPartitionsAvailableAtRunTime: Boolean = {
    partitionFilters.exists(ExecSubqueryExpression.hasSubquery)
  }

  private def toAttribute(colName: String): Option[Attribute] =
    output.find(_.name == colName)

  // exposed for testing  检查当前数据源是否支持分桶扫描
  lazy val bucketedScan: Boolean = {
    if (relation.sparkSession.sessionState.conf.bucketingEnabled && relation.bucketSpec.isDefined
      && !disableBucketedScan) {
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      bucketColumns.size == spec.bucketColumnNames.size
    } else {
      false
    }
  }

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    if (bucketedScan) {
      // For bucketed columns:
      // -----------------------
      // `HashPartitioning` would be used only when:
      // 1. ALL the bucketing columns are being read from the table
      //
      // For sorted columns:
      // ---------------------
      // Sort ordering should be used when ALL these criteria's match:
      // 1. `HashPartitioning` is being used
      // 2. A prefix (or all) of the sort columns are being read from the table.
      //
      // Sort ordering would be over the prefix subset of `sort columns` being read
      // from the table.
      // e.g.
      // Assume (col0, col2, col3) are the columns read from the table
      // If sort columns are (col0, col1), then sort ordering would be considered as (col0)
      // If sort columns are (col1, col0), then sort ordering would be empty as per rule #2
      // above
      val spec = relation.bucketSpec.get
      val bucketColumns = spec.bucketColumnNames.flatMap(n => toAttribute(n))
      val numPartitions = optionalNumCoalescedBuckets.getOrElse(spec.numBuckets)
      val partitioning = HashPartitioning(bucketColumns, numPartitions)
      val sortColumns =
        spec.sortColumnNames.map(x => toAttribute(x)).takeWhile(x => x.isDefined).map(_.get)
      val shouldCalculateSortOrder =
        conf.getConf(SQLConf.LEGACY_BUCKETED_TABLE_SCAN_OUTPUT_ORDERING) &&
          sortColumns.nonEmpty &&
          !hasPartitionsAvailableAtRunTime

      val sortOrder = if (shouldCalculateSortOrder) {
        // In case of bucketing, its possible to have multiple files belonging to the
        // same bucket in a given relation. Each of these files are locally sorted
        // but those files combined together are not globally sorted. Given that,
        // the RDD partition will not be sorted even if the relation has sort columns set
        // Current solution is to check if all the buckets have a single file in it

        val files = selectedPartitions.flatMap(partition => partition.files)
        val bucketToFilesGrouping =
          files.map(_.getPath.getName).groupBy(file => BucketingUtils.getBucketId(file))
        val singleFilePartitions = bucketToFilesGrouping.forall(p => p._2.length <= 1)

        // TODO SPARK-24528 Sort order is currently ignored if buckets are coalesced.
        if (singleFilePartitions && optionalNumCoalescedBuckets.isEmpty) {
          // TODO Currently Spark does not support writing columns sorting in descending order
          // so using Ascending order. This can be fixed in future
          sortColumns.map(attribute => SortOrder(attribute, Ascending))
        } else {
          Nil
        }
      } else {
        Nil
      }
      (partitioning, sortOrder)
    } else {
      (UnknownPartitioning(0), Nil)
    }
  }

  @transient
  protected lazy val pushedDownFilters = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    // `dataFilters` should not include any constant metadata col filters
    // because the metadata struct has been flatted in FileSourceStrategy
    // and thus metadata col filters are invalid to be pushed down. Metadata that is generated
    // during the scan can be used for filters.
    dataFilters.filterNot(_.references.exists {
      case FileSourceConstantMetadataAttribute(_) => true
      case _ => false
    }).flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

  override lazy val metadata: Map[String, String] = {
    def seqToString(seq: Seq[Any]) = seq.mkString("[", ", ", "]")
    val location = relation.location
    val locationDesc =
      location.getClass.getSimpleName +
        Utils.buildLocationMetadata(location.rootPaths, maxMetadataValueLength)
    val metadata =
      Map(
        "Format" -> relation.fileFormat.toString,
        "ReadSchema" -> requiredSchema.catalogString,
        "Batched" -> supportsColumnar.toString,
        "PartitionFilters" -> seqToString(partitionFilters),
        "PushedFilters" -> seqToString(pushedDownFilters),
        "DataFilters" -> seqToString(dataFilters),
        "Location" -> locationDesc)

    relation.bucketSpec.map { spec =>
      val bucketedKey = "Bucketed"
      if (bucketedScan) {
        val numSelectedBuckets = optionalBucketSet.map { b =>
          b.cardinality()
        } getOrElse {
          spec.numBuckets
        }
        metadata ++ Map(
          bucketedKey -> "true",
          "SelectedBucketsCount" -> (s"$numSelectedBuckets out of ${spec.numBuckets}" +
            optionalNumCoalescedBuckets.map { b => s" (Coalesced to $b)"}.getOrElse("")))
      } else if (!relation.sparkSession.sessionState.conf.bucketingEnabled) {
        metadata + (bucketedKey -> "false (disabled by configuration)")
      } else if (disableBucketedScan) {
        metadata + (bucketedKey -> "false (disabled by query planner)")
      } else {
        metadata + (bucketedKey -> "false (bucket column(s) not read)")
      }
    } getOrElse {
      metadata
    }
  }

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted.filterNot {
      case (_, value) if (value.isEmpty || value.equals("[]")) => true
      case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
      case (_, _) => false
    }.map {
      case (key, _) if (key.equals("Location")) =>
        val location = relation.location
        val numPaths = location.rootPaths.length
        val abbreviatedLocation = if (numPaths <= 1) {
          location.rootPaths.mkString("[", ", ", "]")
        } else {
          "[" + location.rootPaths.head + s", ... ${numPaths - 1} entries]"
        }
        s"$key: ${location.getClass.getSimpleName} ${redact(abbreviatedLocation)}"
      case (key, value) => s"$key: ${redact(value)}"
    }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  override def metrics: Map[String, SQLMetric] = scanMetrics

  /** SQL metrics generated only for scans using dynamic partition pruning. */
  protected lazy val staticMetrics = if (partitionFilters.exists(isDynamicPruningFilter)) {
    Map("staticFilesNum" -> SQLMetrics.createMetric(sparkContext, "static number of files read"),
      "staticFilesSize" -> SQLMetrics.createSizeMetric(sparkContext, "static size of files read"))
  } else {
    Map.empty[String, SQLMetric]
  }

  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles").set(filesNum)
      driverMetrics("filesSize").set(filesSize)
    } else {
      driverMetrics("staticFilesNum").set(filesNum)
      driverMetrics("staticFilesSize").set(filesSize)
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions").set(partitions.length)
    }
  }

  private lazy val scanMetrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
  ) ++ {
    // Tracking scan time has overhead, we can't afford to do it for each row, and can only do
    // it for each batch.
    if (supportsColumnar) {
      Some("scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))
    } else {
      None
    }
  } ++ driverMetrics
}

/**  Apache Spark 中用于从 Hadoop 文件系统（如 HDFS、S3 等）扫描数据的物理计划节点（Physical Plan Node）
 * Physical plan node for scanning data from HadoopFsRelations.
 *
 * @param relation The file-based relation to scan.
 * @param output Output attributes of the scan, including data attributes and partition attributes.
 * @param requiredSchema Required schema of the underlying relation, excluding partition columns.
 * @param partitionFilters Predicates to use for partition pruning.
 * @param optionalBucketSet Bucket ids for bucket pruning.
 * @param optionalNumCoalescedBuckets Number of coalesced buckets.
 * @param dataFilters Filters on non-partition columns.
 * @param tableIdentifier Identifier for the table in the metastore.
 * @param disableBucketedScan Disable bucketed scan based on physical query plan, see rule
 *                            [[DisableUnnecessaryBucketedScan]] for details.
 */
case class FileSourceScanExec(
    @transient override val relation: HadoopFsRelation, //扫描的文件数据源关系，它包含文件格式、文件路径、分区信息等
    override val output: Seq[Attribute],  //表示扫描操作的输出列，包括数据列和分区列
    override val requiredSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val optionalBucketSet: Option[BitSet],
    override val optionalNumCoalescedBuckets: Option[Int], //要合并成多少个buckets
    override val dataFilters: Seq[Expression],
    override val tableIdentifier: Option[TableIdentifier],
    override val disableBucketedScan: Boolean = false)
  extends FileSourceScanLike {

  // Note that some vals referring the file-based relation are lazy intentionally
  // so that this plan can be canonicalized on executor side too. See SPARK-23731.
  override lazy val supportsColumnar: Boolean = {  //判断是否支持列式存储优化
    val conf = relation.sparkSession.sessionState.conf
    // Only output columnar if there is WSCG to read it.
    val requiredWholeStageCodegenSettings =
      conf.wholeStageEnabled && !WholeStageCodegenExec.isTooManyFields(conf, schema)
    requiredWholeStageCodegenSettings &&
      relation.fileFormat.supportBatch(relation.sparkSession, schema)
  }
  //判断是否需要进行 UnsafeRow 转换
  private lazy val needsUnsafeRowConversion: Boolean = {
    if (relation.fileFormat.isInstanceOf[ParquetSource]) {
      conf.parquetVectorizedReaderEnabled
    } else {
      false
    }
  }

  lazy val inputRDD: RDD[InternalRow] = {
    val options = relation.options +
      (FileFormat.OPTION_RETURNING_BATCH -> supportsColumnar.toString)
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val readRDD = if (bucketedScan) {
      createBucketedReadRDD(relation.bucketSpec.get, readFile, dynamicallySelectedPartitions)
    } else {
      createReadRDD(readFile, dynamicallySelectedPartitions)
    }
    sendDriverMetrics()
    readRDD
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }
  //执行文件扫描操作，并将数据转换为 InternalRow
  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    if (needsUnsafeRowConversion) {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        val toUnsafe = UnsafeProjection.create(schema)
        toUnsafe.initialize(index)
        iter.map { row =>
          numOutputRows += 1
          toUnsafe(row)
        }
      }
    } else {
      inputRDD.mapPartitionsInternal { iter =>
        iter.map { row =>
          numOutputRows += 1
          row
        }
      }
    }
  }
  //执行列式扫描操作，返回 ColumnarBatch 数据格式。列式扫描可以提高查询性能，特别是在使用列式存储格式（如 Parquet 或 ORC）时
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = batches.hasNext
          scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }

  override val nodeNamePrefix: String = "File"

  /**
   * Create an RDD for bucketed reads.
   * The non-bucketed variant of this function is [[createReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same bucket id from all the given Hive partitions.
   *
   * @param bucketSpec the bucketing spec.
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   */ //目的是为基于分桶（bucketed）的读取创建一个 RDD。在 Apache Spark 中，分桶是对数据的物理划分，它将数据划分成多个桶（类似于哈希分区），通常用于优化大数据集的查询，尤其是当查询需要对特定桶的数据进行操作时
  private def createBucketedReadRDD(
      bucketSpec: BucketSpec, //表示分桶规范，定义了数据集的桶数和分桶方式
      readFile: (PartitionedFile) => Iterator[InternalRow], //用于读取文件中的数据
      selectedPartitions: Array[PartitionDirectory]): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      selectedPartitions.flatMap { p =>
        p.files.map(f => PartitionedFileUtil.getPartitionedFile(f, p.values))
      }.groupBy { f =>
        BucketingUtils
          .getBucketId(f.toPath.getName) //根据文件的路径和文件名获取分桶 ID（getBucketId）
          .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.urlEncodedPath))
      }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get //如果提供了一个桶 ID 集合（optionalBucketSet），则根据该集合进行桶裁剪
      filesGroupedToBuckets.filter {
        f => bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }
    //合并桶（Coalescing Buckets）
    val filePartitions = optionalNumCoalescedBuckets.map { numCoalescedBuckets =>
      logInfo(s"Coalescing to ${numCoalescedBuckets} buckets")
      val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
      Seq.tabulate(numCoalescedBuckets) { bucketId =>
        val partitionedFiles = coalescedBuckets.get(bucketId).map {
          _.values.flatten.toArray
        }.getOrElse(Array.empty)
        FilePartition(bucketId, partitionedFiles)
      }
    }.getOrElse {
      Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
        FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
      }
    }
    //创建 FileScanRDD
    new FileScanRDD(relation.sparkSession, readFile, filePartitions,
      new StructType(requiredSchema.fields ++ relation.partitionSchema.fields),
      fileConstantMetadataColumns, relation.fileFormat.fileConstantMetadataExtractors,
      new FileSourceOptions(CaseInsensitiveMap(relation.options)))
  }

  /**
   * Create an RDD for non-bucketed reads.
   * The bucketed variant of this function is [[createBucketedReadRDD]].
   * 目的是为非分桶（non-bucketed）读取创建一个 RDD
   * @param readFile a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   */
  private def createReadRDD(
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Array[PartitionDirectory]): RDD[InternalRow] = { //包含所有被选中的 Hive 分区的数组。每个分区包含了一些文件
    val openCostInBytes = relation.sparkSession.sessionState.conf.filesOpenCostInBytes  //通常是为了决定是否应该将多个文件合并成一个分片。如果文件打开的成本过高，可能会影响性能
    val maxSplitBytes =
      FilePartition.maxSplitBytes(relation.sparkSession, selectedPartitions) //每个分区的处理数据的大小，默认的128M和所有文件大小除以最小分区数量，取最小值
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    // Filter files with bucket pruning if possible  过滤文件以进行桶裁剪，检查当前 Spark 会话是否启用了分桶（bucketing）。如果启用了分桶，则根据文件的名称和桶 ID 判断是否处理该文件
    val bucketingEnabled = relation.sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: Path => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>  //如果启用了分桶并且提供了一个桶集合（optionalBucketSet），则只有桶 ID 在集合中的文件才会被处理
        // Do not prune the file if bucket file name is invalid
        filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
      case _ =>
        _ => true  //如果没有启用分桶或没有提供桶集合，则所有文件都将被处理
    }
    //根据分片规则分割文件
    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        if (shouldProcess(file.getPath)) {  //分桶检查是否要处理
          val isSplitable = relation.fileFormat.isSplitable(
              relation.sparkSession, relation.options, file.getPath)  //检查文件是否可拆分
          PartitionedFileUtil.splitFiles(
            sparkSession = relation.sparkSession,
            file = file,
            isSplitable = isSplitable,
            maxSplitBytes = maxSplitBytes,
            partitionValues = partition.values
          )  //对于每个文件，尝试将其拆分为多个分片。拆分的依据是文件是否可拆分（isSplitable），以及最大分片大小（maxSplitBytes）
        } else {
          Seq.empty
        }
      }
    }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    //上面的splitFiles已经按照设置的并行度计算分区大小，这里主要是合并一些小的分区文件
    val partitions =
      FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)

    new FileScanRDD(relation.sparkSession, readFile, partitions,
      new StructType(requiredSchema.fields ++ relation.partitionSchema.fields),
      fileConstantMetadataColumns, relation.fileFormat.fileConstantMetadataExtractors,
      new FileSourceOptions(CaseInsensitiveMap(relation.options)))
  }

  // Filters unused DynamicPruningExpression expressions - one which has been replaced
  // with DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
  private def filterUnusedDynamicPruningExpressions(
      predicates: Seq[Expression]): Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }

  override def doCanonicalize(): FileSourceScanExec = {
    FileSourceScanExec(
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionFilters), output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan)
  }
}
