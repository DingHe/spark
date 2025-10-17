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

package org.apache.spark.sql.hive.execution

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.{DelegateSymlinkTextInputFormat, SymlinkTextInputFormat}
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.mapred.InputFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.util.Utils

/**
 * The Hive table scan operator.  Column and partition pruning are both handled.
 *
 * @param requestedAttributes Attributes to be fetched from the Hive table.
 * @param relation The Hive table be scanned.
 * @param partitionPruningPred An optional partition pruning predicate for partitioned table.
 */
// HiveTableScanExec 是 Spark SQL 物理执行计划中的一个叶子节点（LeafExecNode），它专门用于从 Hive 表中读取数据。
// 核心职责：
// 数据读取起点： 它标志着 Hive 表数据流在 Spark 物理执行计划中的起点。它通过 HadoopTableReader 封装了 Hive SerDe（序列化/反序列化）和 Hadoop InputFormat 的复杂逻辑。
// 分区裁剪（Partition Pruning）： 负责根据查询的过滤条件 (partitionPruningPred)，在读取数据之前，确定并只读取所需的 Hive 分区，从而极大地减少 I/O。
// 列裁剪（Column Pruning）： 确保只读取查询中实际需要的列 (requestedAttributes)，并配置底层的 Hive SerDe 机制以进行优化。
// 数据格式转换： 负责将 Hive/Hadoop 读取的原始数据转换为 Spark 内部高效的 InternalRow 和最终的 UnsafeRow 格式。
private[hive]
case class HiveTableScanExec(
    requestedAttributes: Seq[Attribute], //请求的列，表示上层操作符（查询）实际需要从表中读取的列的列表。用于实现列裁剪。
    relation: HiveTableRelation, // Hive 表关系，包含了 Hive 表的所有元数据信息（Schema、分区列、存储信息、TableDesc 等），是逻辑计划中的表引用。
    partitionPruningPred: Seq[Expression])( // 分区裁剪谓词，包含用于过滤分区的表达式列表。这些表达式通常是 WHERE 子句中针对分区列的条件。
    @transient private val sparkSession: SparkSession)
  extends LeafExecNode with CastSupport {

  require(partitionPruningPred.isEmpty || relation.isPartitioned,
    "Partition pruning predicates only supported for partitioned tables.")
  // SQL 配置
  override def conf: SQLConf = sparkSession.sessionState.conf

  override def nodeName: String = s"Scan hive ${relation.tableMeta.qualifiedName}"

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))
  // 返回该节点产生的最终输出列 (outputSet)，以及分区裁剪谓词中引用的所有属性（用于确保分区列在执行前可用）
  override def producedAttributes: AttributeSet = outputSet ++
    AttributeSet(partitionPruningPred.flatMap(_.references))

  private val originalAttributes = AttributeMap(relation.output.map(a => a -> a))
  //输出 Schema
  override val output: Seq[Attribute] = {
    // Retrieve the original attributes based on expression ID so that capitalization matches.
    //定义该节点向其父节点输出的数据 Schema。它将 requestedAttributes 映射回 relation.output，以确保列名和大小写匹配（基于 ID）
    requestedAttributes.map(originalAttributes)
  }

  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  // 绑定后的裁剪谓词
  // 将 partitionPruningPred 合并（使用 And）并绑定到 relation.partitionCols 的属性引用上。这使得谓词可以在运行时针对分区值进行求值。
  private lazy val boundPruningPred = partitionPruningPred.reduceLeftOption(And).map { pred =>
    require(pred.dataType == BooleanType,
      s"Data type of predicate $pred must be ${BooleanType.catalogString} rather than " +
        s"${pred.dataType.catalogString}.")

    BindReferences.bindReference(pred, relation.partitionCols)
  }
  // Hive Table 对象
  // 将 Spark Catalog 中的 tableMeta 转换为 Hive 客户端库使用的 org.apache.hadoop.hive.ql.metadata.Table 对象。
  @transient private lazy val hiveQlTable = HiveClientImpl.toHiveTable(relation.tableMeta)
  //Hive Table 描述
  //创建 Hive/Hadoop TableDesc 对象，包含了输入格式、输出格式和表属性，用于配置底层 Hadoop I/O。
  @transient private lazy val tableDesc = new TableDesc(
    getInputFormat(hiveQlTable.getInputFormatClass, conf),
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata)

  // Create a local copy of hadoopConf,so that scan specific modifications should not impact
  // other queries
  //Hadoop 配置
  @transient private lazy val hadoopConf = {
    val c = sparkSession.sessionState.newHadoopConf()
    // append columns ids and names before broadcast
    addColumnMetadataToConf(c)
    c
  }
  // Hadoop 表读取器
  // 核心读取机制。实例化 HadoopTableReader，这是 Spark 封装 Hive I/O 逻辑的关键类，负责生成用于读取文件的 RDD。
  @transient private lazy val hadoopReader = new HadoopTableReader(
    output,
    relation.partitionCols,
    tableDesc,
    sparkSession,
    hadoopConf)
  //用于将从 Hive Metastore 读出的分区值（总是字符串）转换为其对应的实际数据类型（如 IntegerType、DateType 等），以便于谓词求值。
  private def castFromString(value: String, dataType: DataType) = {
    cast(Literal(value), dataType).eval(null)
  }
  //置 Hadoop Configuration，以支持列裁剪。它设置了 Hive 的 LIST_COLUMNS 和 LIST_COLUMN_TYPES 属性，告诉底层的 SerDe 只读取需要的列，实现列裁剪。
  private def addColumnMetadataToConf(hiveConf: Configuration): Unit = {
    // Specifies needed column IDs for those non-partitioning columns.
    val columnOrdinals = AttributeMap(relation.dataCols.zipWithIndex)
    val neededColumnIDs = output.flatMap(columnOrdinals.get).map(o => o: Integer)
    val neededColumnNames = output.filter(columnOrdinals.contains).map(_.name)

    HiveShim.appendReadColumns(hiveConf, neededColumnIDs, neededColumnNames)

    val deserializer = tableDesc.getDeserializerClass.getConstructor().newInstance()
    deserializer.initialize(hiveConf, tableDesc.getProperties)

    // Specifies types and object inspectors of columns to be scanned.
    val structOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        deserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val columnTypeNames = structOI
      .getAllStructFieldRefs.asScala
      .map(_.getFieldObjectInspector)
      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
      .mkString(",")

    hiveConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeNames)
    hiveConf.set(serdeConstants.LIST_COLUMNS, relation.dataCols.map(_.name).mkString(","))
  }

  /**
   * Prunes partitions not involve the query plan.
   *
   * @param partitions All partitions of the relation.
   * @return Partitions that are involved in the query plan.
   */
  // 裁剪后的分区列表
  private[hive] def prunePartitions(partitions: Seq[HivePartition]): Seq[HivePartition] = {
    boundPruningPred match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = relation.partitionCols.map(_.dataType)
        val castedValues = part.getValues.asScala.zip(dataTypes)
          .map { case (value, dataType) => castFromString(value, dataType) }

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = InternalRow.fromSeq(castedValues.toSeq)
        shouldKeep.eval(row).asInstanceOf[Boolean]
      }
    }
  }

  @transient lazy val prunedPartitions: Seq[HivePartition] = {
    if (relation.prunedPartitions.nonEmpty) {
      val hivePartitions =
        relation.prunedPartitions.get.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
      if (partitionPruningPred.forall(!ExecSubqueryExpression.hasSubquery(_))) {
        hivePartitions
      } else {
        prunePartitions(hivePartitions)
      }
    } else {
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionPruningPred.nonEmpty) {
        rawPartitions
      } else {
        prunePartitions(rawPartitions)
      }
    }
  }

  // exposed for tests
  //原始分区列表
  //（暴露给测试） 如果启用了 Metastore 裁剪，则返回 Metastore 裁剪后的分区；否则，返回所有分区。
  @transient lazy val rawPartitions: Seq[HivePartition] = {
    val prunedPartitions =
      if (sparkSession.sessionState.conf.metastorePartitionPruning &&
        partitionPruningPred.nonEmpty) {
        // Retrieve the original attributes based on expression ID so that capitalization matches.
        val normalizedFilters = partitionPruningPred.map(_.transform {
          case a: AttributeReference => originalAttributes(a)
        })
        sparkSession.sessionState.catalog
          .listPartitionsByFilter(relation.tableMeta.identifier, normalizedFilters)
      } else {
        sparkSession.sessionState.catalog.listPartitions(relation.tableMeta.identifier)
      }
    prunedPartitions.map(HiveClientImpl.toHivePartition(_, hiveQlTable))
  }
  //执行并生成 RDD
  // 物理执行的核心。
  // 1. 调用 hadoopReader.makeRDDForTable（非分区表）或 hadoopReader.makeRDDForPartitionedTable（分区表）生成底层 RDD[InternalRow]。
  // 2. 对 RDD 应用 mapPartitionsWithIndexInternal，在每个分区上： - 创建 UnsafeProjection（将 InternalRow 转换为高效的 UnsafeRow）。
  // - 遍历数据并递增 numOutputRows 指标。
  // 3. 返回最终的 RDD。
  protected override def doExecute(): RDD[InternalRow] = {
    // Using dummyCallSite, as getCallSite can turn out to be expensive with
    // multiple partitions.
    val rdd = if (!relation.isPartitioned) {
      Utils.withDummyCallSite(sparkContext) {
        //读取非分区表
        hadoopReader.makeRDDForTable(hiveQlTable)
      }
    } else {
      Utils.withDummyCallSite(sparkContext) {
        //读取分区表
        hadoopReader.makeRDDForPartitionedTable(prunedPartitions)
      }
    }
    val numOutputRows = longMetric("numOutputRows")
    // Avoid to serialize MetastoreRelation because schema is lazy. (see SPARK-15649)
    val outputSchema = schema
    rdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(outputSchema)
      proj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  // Filters unused DynamicPruningExpression expressions - one which has been replaced
  // with DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
  private def filterUnusedDynamicPruningExpressions(
      predicates: Seq[Expression]): Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }

  // Optionally returns a delegate input format based on the provided input format class.
  // This is currently used to replace SymlinkTextInputFormat with DelegateSymlinkTextInputFormat
  // in order to fix SPARK-40815.
  private def getInputFormat(
      inputFormatClass: Class[_ <: InputFormat[_, _]],
      conf: SQLConf): Class[_ <: InputFormat[_, _]] = {
    if (inputFormatClass == classOf[SymlinkTextInputFormat] &&
        conf != null && conf.getConf(HiveUtils.USE_DELEGATE_FOR_SYMLINK_TEXT_INPUT_FORMAT)) {
      classOf[DelegateSymlinkTextInputFormat]
    } else {
      inputFormatClass
    }
  }

  override def doCanonicalize(): HiveTableScanExec = {
    val input: AttributeSeq = relation.output
    HiveTableScanExec(
      requestedAttributes.map(QueryPlan.normalizeExpressions(_, input)),
      relation.canonicalized.asInstanceOf[HiveTableRelation],
      QueryPlan.normalizePredicates(
        filterUnusedDynamicPruningExpressions(partitionPruningPred), input))(sparkSession)
  }

  override def otherCopyArgs: Seq[AnyRef] = Seq(sparkSession)
}
