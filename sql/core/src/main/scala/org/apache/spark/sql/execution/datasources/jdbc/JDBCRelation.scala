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

package org.apache.spark.sql.execution.datasources.jdbc

import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, stringToDate, stringToTimestamp}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, DateType, NumericType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Instructions on how to partition the table among workers.
 */
private[sql] case class JDBCPartitioningInfo(
    column: String,
    columnType: DataType,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int)

private[sql] object JDBCRelation extends Logging {
  /**
   * Given a partitioning schematic (a column of integral type, a number of
   * partitions, and upper and lower bounds on the column's value), generate
   * WHERE clauses for each partition so that each row in the table appears
   * exactly once.  The parameters minValue and maxValue are advisory in that
   * incorrect values may cause the partitioning to be poor, but no data
   * will fail to be represented.
   *
   * Null value predicate is added to the first partition where clause to include
   * the rows with null value for the partitions column.
   *
   * @param schema resolved schema of a JDBC table
   * @param resolver function used to determine if two identifiers are equal
   * @param timeZoneId timezone ID to be used if a partition column type is date or timestamp
   * @param jdbcOptions JDBC options that contains url
   * @return an array of partitions with where clause for each partition
   */
  def columnPartition(
      schema: StructType,
      resolver: Resolver,
      timeZoneId: String,
      jdbcOptions: JDBCOptions): Array[Partition] = {
    val partitioning = {
      import JDBCOptions._
      // 从配置中获取分区列名、下界、上界和分区数
      val partitionColumn = jdbcOptions.partitionColumn
      val lowerBound = jdbcOptions.lowerBound
      val upperBound = jdbcOptions.upperBound
      val numPartitions = jdbcOptions.numPartitions
      // 如果没有指定分区列，那么下界和上界也必须为空
      if (partitionColumn.isEmpty) {
        assert(lowerBound.isEmpty && upperBound.isEmpty, "When 'partitionColumn' is not " +
          s"specified, '$JDBC_LOWER_BOUND' and '$JDBC_UPPER_BOUND' are expected to be empty")
        null
      } else {
        // 如果指定了分区列，则必须同时提供下界、上界和分区数
        assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions.nonEmpty,
          s"When 'partitionColumn' is specified, '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', and " +
            s"'$JDBC_NUM_PARTITIONS' are also required")
        // 验证分区列是否存在于 Schema 中，并获取其规范化的名称和数据类型
        val (column, columnType) = verifyAndGetNormalizedPartitionColumn(
          schema, partitionColumn.get, resolver, jdbcOptions)
        // 将字符串类型的上下界转换为 Spark 内部使用的 Long 型值（支持 Date/Timestamp）
        val lowerBoundValue = toInternalBoundValue(lowerBound.get, columnType, timeZoneId)
        val upperBoundValue = toInternalBoundValue(upperBound.get, columnType, timeZoneId)
        // 封装成分区信息对象
        JDBCPartitioningInfo(
          column, columnType, lowerBoundValue, upperBoundValue, numPartitions.get)
      }
    }
    // 如果不分区、分区数为1，或者上下界相等，则只生成一个全量扫描的分区（WHERE 为空）
    if (partitioning == null || partitioning.numPartitions <= 1 ||
      partitioning.lowerBound == partitioning.upperBound) {
      return Array[Partition](JDBCPartition(null, 0))
    }

    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    // 强制要求下界不能大于上界
    require (lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
      s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")
    // 定义一个辅助函数，将数值转回 SQL WHERE 子句中使用的字符串格式
    val boundValueToString: Long => String =
      toBoundValueInWhereClause(_, partitioning.columnType, timeZoneId)
    // 确定最终的分区数
    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions || /* check for overflow */
          (upperBound - lowerBound) < 0) {
        // 如果范围足够大，或者发生溢出（Long 范围极大），使用用户指定的分区数
        partitioning.numPartitions
      } else {
        logWarning("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${partitioning.numPartitions}; " +
          s"Lower bound: ${boundValueToString(lowerBound)}; " +
          s"Upper bound: ${boundValueToString(upperBound)}.")
        // 如果范围比分区数还小（比如范围是1-5，但要求分10个区），则缩小分区数至范围大小
        upperBound - lowerBound
      }

    // Overflow can happen if you subtract then divide. For example:
    // (Long.MaxValue - Long.MinValue) / (numPartitions - 2).
    // Also, using fixed-point decimals here to avoid possible inaccuracy from floating point.
    // 步长（Stride）的精确计算：
    val upperStride = (upperBound / BigDecimal(numPartitions))
      .setScale(18, RoundingMode.HALF_EVEN)
    val lowerStride = (lowerBound / BigDecimal(numPartitions))
      .setScale(18, RoundingMode.HALF_EVEN)
    // 取整作为每个分区的步长
    val preciseStride = upperStride - lowerStride
    val stride = preciseStride.toLong

    // Determine the number of strides the last partition will fall short of compared to the
    // supplied upper bound. Take half of those strides, and then add them to the lower bound
    // for better distribution of the first and last partitions.
    // 计算由于取整（preciseStride -> stride）累积损失的步长数量
    val lostNumOfStrides = (preciseStride - stride) * numPartitions / stride
    // 将损失的一部分偏移量加到起始值上，使得第一和最后一个分区的范围更加均衡
    val lowerBoundWithStrideAlignment = lowerBound +
      ((lostNumOfStrides / 2) * stride).setScale(0, RoundingMode.HALF_UP).toLong

    // 生成 WHERE 子句循环
    var i: Int = 0
    val column = partitioning.column
    var currentValue = lowerBoundWithStrideAlignment
    val ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lBoundValue = boundValueToString(currentValue)
      // 如果不是第一个分区，生成 >= 下限
      val lBound = if (i != 0) s"$column >= $lBoundValue" else null
      currentValue += stride
      val uBoundValue = boundValueToString(currentValue)
      // 如果不是最后一个分区，生成 < 上限
      val uBound = if (i != numPartitions - 1) s"$column < $uBoundValue" else null
      val whereClause =
        if (uBound == null) {
          lBound // 最后一个分区：column >= lBoundValue
        } else if (lBound == null) {
          s"$uBound or $column is null" // 第一个分区：column < uBoundValue OR NULL
        } else {
          s"$lBound AND $uBound" // 中间分区：column >= lBound AND column < uBound
        }
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }
    val partitions = ans.toArray
    logInfo(s"Number of partitions: $numPartitions, WHERE clauses of these partitions: " +
      partitions.map(_.asInstanceOf[JDBCPartition].whereClause).mkString(", "))
    partitions
  }

  // Verify column name and type based on the JDBC resolved schema
  // 验证用户指定的分区列（Partition Column）是否合法。
  // 它确保该列存在于表的 Schema 中，并且其数据类型支持分区操作（数值、日期或时间戳），最后返回处理过的规范化列名和类型。
  private def verifyAndGetNormalizedPartitionColumn(
      schema: StructType,
      columnName: String,
      resolver: Resolver,
      jdbcOptions: JDBCOptions): (String, DataType) = {
    // 根据 JDBC URL 获取对应的数据库方言（JdbcDialect）
    // 不同的数据库（MySQL, Oracle, PostgreSQL）有不同的转义规则
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val column = schema.find { f =>
      // 使用 resolver 比较列名，resolver 通常处理大小写敏感性
      // 逻辑：原始字段名匹配 OR 带方言引号的字段名匹配
      resolver(f.name, columnName) || resolver(dialect.quoteIdentifier(f.name), columnName)
    }.getOrElse {
      // 如果没找到对应的列，获取配置中允许打印的最大字段数
      val maxNumToStringFields = SQLConf.get.maxToStringFields
      // 抛出编译时错误：告知用户定义的分区列在 Schema 中找不到
      throw QueryCompilationErrors.userDefinedPartitionNotFoundInJDBCRelationError(
        columnName, schema.simpleString(maxNumToStringFields))
    }
    column.dataType match {
      // 检查该列的数据类型
      // 只允许：数值类型（如 Int, Long, Decimal）、日期类型（Date）或时间戳类型（Timestamp）
      case _: NumericType | DateType | TimestampType =>
      case _ =>
        // 如果是 String, Boolean 等其他类型，抛出错误
        // 因为 JDBC 分区依赖于范围（Range）计算，非连续或非序类型无法分区
        throw QueryCompilationErrors.invalidPartitionColumnTypeError(column)
    }
    // 返回一个二元组：(经过方言转义后的列名, 列的数据类型)
    (dialect.quoteIdentifier(column.name), column.dataType)
  }
  // 将用户在配置中提供的字符串格式的上下界（lowerBound 和 upperBound）转换成 Spark 内部统一使用的 Long 类型数值。
  // 由于 Spark 在计算分区步长（Stride）时需要进行算术运算，因此无论原始类型是整数、日期还是时间戳，都需要先“归一化”为数字。
  private def toInternalBoundValue(
      value: String, // 用户输入的边界值字符串（例如 "2023-01-01" 或 "100"）
      columnType: DataType, // 分区列的 Spark 数据类型
      timeZoneId: String): Long = { // 时区 ID（处理时间类型时必不可少）
    def parse[T](f: UTF8String => Option[T]): T = {
      // 将 Scala String 转换为 Spark 内部的 UTF8String
      // 然后调用传入的解析函数 f（如 stringToDate）
      f(UTF8String.fromString(value)).getOrElse {
        throw new IllegalArgumentException(
          s"Cannot parse the bound value $value as ${columnType.catalogString}")
      }
    }
    columnType match {
      case _: NumericType => value.toLong
      case DateType => parse(stringToDate).toLong
      case TimestampType => parse(stringToTimestamp(_, getZoneId(timeZoneId)))
    }
  }
  // 作用与之前解析的 toInternalBoundValue 正好相反
  // 将 Spark 内部计算好的 Long 类型分区边界值，还原回适合放在 SQL WHERE 子句中的字符串格式。例如，将 Long 型的微秒数还原为 '2023-10-27 10:00:00' 这种数据库能读懂的字符串。
  private def toBoundValueInWhereClause(
      value: Long, // Spark 内部计算出来的 Long 型边界值
      columnType: DataType, // 分区列的原始数据类型
      timeZoneId: String): String = { // 用于格式化时间戳的时区 ID
    def dateTimeToString(): String = {
      val dateTimeStr = columnType match {
        // 情况 A：如果是日期类型
        case DateType =>
          // 调用 Spark 内部的 DateFormatter，将天数（Int）转换为 "yyyy-MM-dd" 字符串
          DateFormatter().format(value.toInt)
        // 情况 B：如果是时间戳类型
        case TimestampType =>
          // 1. 根据时区获取一个支持分数（微秒）的格式化器
          val timestampFormatter = TimestampFormatter.getFractionFormatter(
            DateTimeUtils.getZoneId(timeZoneId))
          // 2. 将微秒数（Long）格式化为时间戳字符串（如 "yyyy-MM-dd HH:mm:ss.SSSSSS"）
          timestampFormatter.format(value)
      }
      // 在日期时间字符串外面加上单引号，以符合 SQL 标准
      s"'$dateTimeStr'"
    }
    columnType match {
      case _: NumericType => value.toString
      case DateType | TimestampType => dateTimeToString()
    }
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst schema.
   * If `customSchema` defined in the JDBC options, replaces the schema's dataType with the
   * custom schema's type.
   *
   * @param resolver function used to determine if two identifiers are equal
   * @param jdbcOptions JDBC options that contains url, table and other information.
   * @return resolved Catalyst schema of a JDBC table
   */
  def getSchema(resolver: Resolver, jdbcOptions: JDBCOptions): StructType = {
    val tableSchema = JDBCRDD.resolveTable(jdbcOptions)
    jdbcOptions.customSchema match {
      case Some(customSchema) => JdbcUtils.getCustomSchema(
        tableSchema, customSchema, resolver)
      case None => tableSchema
    }
  }

  /**
   * Resolves a Catalyst schema of a JDBC table and returns [[JDBCRelation]] with the schema.
   */
  def apply(
      parts: Array[Partition],
      jdbcOptions: JDBCOptions)(
      sparkSession: SparkSession): JDBCRelation = {
    val schema = JDBCRelation.getSchema(sparkSession.sessionState.conf.resolver, jdbcOptions)
    JDBCRelation(schema, parts, jdbcOptions)(sparkSession)
  }
}

private[sql] case class JDBCRelation(
    override val schema: StructType,
    parts: Array[Partition],
    jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  // Check if JdbcDialect can compile input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      val dialect = JdbcDialects.get(jdbcOptions.url)
      filters.filter(f => dialect.compileExpression(f.toV2).isEmpty)
    } else {
      filters
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // When pushDownPredicate is false, all Filters that need to be pushed down should be ignored
    val pushedPredicates = if (jdbcOptions.pushDownPredicate) {
      filters.map(_.toV2)
    } else {
      Array.empty[Predicate]
    }
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      pushedPredicates,
      parts,
      jdbcOptions).asInstanceOf[RDD[Row]]
  }

  def buildScan(
      requiredColumns: Array[String],
      finalSchema: StructType,
      predicates: Array[Predicate],
      groupByColumns: Option[Array[String]],
      tableSample: Option[TableSampleInfo],
      limit: Int,
      sortOrders: Array[String],
      offset: Int): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      predicates,
      parts,
      jdbcOptions,
      Some(finalSchema),
      groupByColumns,
      tableSample,
      limit,
      sortOrders,
      offset).asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .jdbc(jdbcOptions.url, jdbcOptions.tableOrQuery, jdbcOptions.asProperties)
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"JDBCRelation(${jdbcOptions.prepareQuery}${jdbcOptions.tableOrQuery})$partitioningInfo"
  }
}
