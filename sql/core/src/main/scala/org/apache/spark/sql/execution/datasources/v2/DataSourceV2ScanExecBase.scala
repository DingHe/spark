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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, RowOrdering, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.{ExplainUtils, LeafExecNode, SQLExecution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

// Spark SQL 物理执行计划中专门用于 DataSource V2 读取 的基类
// 主要的职责是：
// 封装 V2 数据源 Scan 执行节点的通用逻辑
// 管理 V2 数据源读取的 InputPartition → RDD 的转换
// 支持 V2 分区信息与排序信息 的下推与传播（用于避免 Shuffle）
// 处理 Columnar 读取模式（批处理，矢量化读取）
trait DataSourceV2ScanExecBase extends LeafExecNode {
  // 注册当前 Scan 支持的自定义指标
  lazy val customMetrics = scan.supportedCustomMetrics().map { customMetric =>
    customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
  }.toMap

  override lazy val metrics = {
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")) ++
      customMetrics
  }
  // 表示对应的 V2 数据源扫描对象
  // Scan 是 DataSource V2 读取的核心接口，封装了 partition 列表、列剪裁、谓词下推等信息
  def scan: Scan
  // 表示如何为每个 InputPartition 创建一个 PartitionReader
  def readerFactory: PartitionReaderFactory

  /** Optional partitioning expressions provided by the V2 data sources, through
   * `SupportsReportPartitioning` */
  // 描述 V2 数据源提供的 分区键表达式
  def keyGroupedPartitioning: Option[Seq[Expression]]

  /** Optional ordering expressions provided by the V2 data sources, through
   * `SupportsReportOrdering` */
  // 描述 V2 数据源提供的 输出排序信息
  def ordering: Option[Seq[SortOrder]]
  // 当前 Scan 返回的所有输入分区（InputPartition）
  // 每个分区对应 Spark 任务执行的一个输入 split
  protected def inputPartitions: Seq[InputPartition]

  override def simpleString(maxFields: Int): String = {
    val result =
      s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)} ${scan.description()}"
    redact(result)
  }
  // 用于将 InputPartition 按照 key 分组
  // 如果 groupedPartitions 存在，则返回其内部的分区列表
  // 否则，将每个分区单独包装成单元素列表
  def partitions: Seq[Seq[InputPartition]] =
    groupedPartitions.map(_.map(_._2)).getOrElse(inputPartitions.map(Seq(_)))

  /**
   * Shorthand for calling redact() without specifying redacting rules
   */
  protected def redact(text: String): String = {
    Utils.redact(session.sessionState.conf.stringRedactionPattern, text)
  }

  override def verboseStringWithOperatorId(): String = {
    val metaDataStr = scan match {
      case s: SupportsMetadata =>
        s.getMetaData().toSeq.sorted.flatMap {
          case (_, value) if value.isEmpty || value.equals("[]") => None
          case (key, value) => Some(s"$key: ${redact(value)}")
          case _ => None
        }
      case _ =>
        Seq(scan.description())
    }
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metaDataStr.mkString("\n")}
       |""".stripMargin
  }
  // 根据 keyGroupedPartitioning 来推导 Spark 的 Partitioning
  override def outputPartitioning: physical.Partitioning = {
    keyGroupedPartitioning match {
      case Some(exprs) if KeyGroupedPartitioning.supportsExpressions(exprs) =>
        groupedPartitions
          .map { partitionValues =>
            KeyGroupedPartitioning(exprs, partitionValues.size, partitionValues.map(_._1))
          }
          .getOrElse(super.outputPartitioning)
      case _ =>
        super.outputPartitioning
    }
  }
  // 当 keyGroupedPartitioning 存在时，尝试对 inputPartitions 进行分组
  @transient lazy val groupedPartitions: Option[Seq[(InternalRow, Seq[InputPartition])]] = {
    // Early check if we actually need to materialize the input partitions.
    keyGroupedPartitioning match {
      case Some(_) => groupPartitions(inputPartitions)
      case _ => None
    }
  }

  /**
   * Group partition values for all the input partitions. This returns `Some` iff:
   *   - [[SQLConf.V2_BUCKETING_ENABLED]] is turned on
   *   - all input partitions implement [[HasPartitionKey]]
   *   - `keyGroupedPartitioning` is set
   *
   * The result, if defined, is a list of tuples where the first element is a partition value,
   * and the second element is a list of input partitions that share the same partition value.
   *
   * A non-empty result means each partition is clustered on a single key and therefore eligible
   * for further optimizations to eliminate shuffling in some operations such as join and aggregate.
   */
  def groupPartitions(
      inputPartitions: Seq[InputPartition],
      groupSplits: Boolean = !conf.v2BucketingPushPartValuesEnabled ||
          !conf.v2BucketingPartiallyClusteredDistributionEnabled):
    Option[Seq[(InternalRow, Seq[InputPartition])]] = {

    if (!SQLConf.get.v2BucketingEnabled) return None
    keyGroupedPartitioning.flatMap { expressions =>
      val results = inputPartitions.takeWhile {
        case _: HasPartitionKey => true
        case _ => false
      }.map(p => (p.asInstanceOf[HasPartitionKey].partitionKey(), p))

      if (results.length != inputPartitions.length || inputPartitions.isEmpty) {
        // Not all of the `InputPartitions` implements `HasPartitionKey`, therefore skip here.
        None
      } else {
        // also sort the input partitions according to their partition key order. This ensures
        // a canonical order from both sides of a bucketed join, for example.
        val partitionDataTypes = expressions.map(_.dataType)
        val partitionOrdering: Ordering[(InternalRow, Seq[InputPartition])] = {
          RowOrdering.createNaturalAscendingOrdering(partitionDataTypes).on(_._1)
        }

        val partitions = if (groupSplits) {
          // Group the splits by their partition value
          results
            .map(t => (InternalRowComparableWrapper(t._1, expressions), t._2))
            .groupBy(_._1)
            .toSeq
            .map {
              case (key, s) => (key.row, s.map(_._2))
            }
        } else {
          // No splits grouping, each split will become a separate Spark partition
          results.map(t => (t._1, Seq(t._2)))
        }

        Some(partitions.sorted(partitionOrdering))
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = {
    // when multiple partitions are grouped together, ordering inside partitions is not preserved
    val partitioningPreservesOrdering = groupedPartitions.forall(_.forall(_._2.length <= 1))
    ordering.filter(_ => partitioningPreservesOrdering).getOrElse(super.outputOrdering)
  }
  // 决定是否支持列式读取
  override def supportsColumnar: Boolean = {
    scan.columnarSupportMode() match {
      case Scan.ColumnarSupportMode.PARTITION_DEFINED =>
        require(
          inputPartitions.forall(readerFactory.supportColumnarReads) ||
            !inputPartitions.exists(readerFactory.supportColumnarReads),
          "Cannot mix row-based and columnar input partitions.")
        inputPartitions.exists(readerFactory.supportColumnarReads)
      case Scan.ColumnarSupportMode.SUPPORTED => true
      case Scan.ColumnarSupportMode.UNSUPPORTED => false
    }
  }
  // 负责将 InputPartition 转换成对应的 Spark RDD
  def inputRDD: RDD[InternalRow]

  def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)
  // DataSource V2 Scan 的实际执行入口
  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.map { r =>
      numOutputRows += 1
      r
    }
  }

  protected def postDriverMetrics(): Unit = {
    val driveSQLMetrics = scan.reportDriverMetrics().map(customTaskMetric => {
      val metric = metrics(customTaskMetric.name())
      metric.set(customTaskMetric.value())
      metric
    })

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      driveSQLMetrics)
  }
  // 当 supportsColumnar = true 时，使用列式执行路径
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { b =>
      numOutputRows += b.numRows()
      b
    }
  }
}
