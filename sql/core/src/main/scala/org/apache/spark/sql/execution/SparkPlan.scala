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

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.spark.{broadcast, SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, TreeNodeTag, UnaryLike}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.WriteFilesSpec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.NextIterator
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}
//主要用于表示执行物理操作的计划
object SparkPlan {
  /** The original [[LogicalPlan]] from which this [[SparkPlan]] is converted. */
  val LOGICAL_PLAN_TAG = TreeNodeTag[LogicalPlan]("logical_plan")

  /** The [[LogicalPlan]] inherited from its ancestor. */
  val LOGICAL_PLAN_INHERITED_TAG = TreeNodeTag[LogicalPlan]("logical_plan_inherited")

  private val nextPlanId = new AtomicInteger(0)

  /** Register a new SparkPlan, returning its SparkPlan ID */
  private[execution] def newPlanId(): Int = nextPlanId.getAndIncrement()
}

/**
 * The base class for physical operators.
 *
 * The naming convention is that physical operators end with "Exec" suffix, e.g. [[ProjectExec]].
 */
//物理执行节点的基类
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {
  @transient final val session = SparkSession.getActiveSession.orNull

  protected def sparkContext = session.sparkContext

  override def conf: SQLConf = {
    if (session != null) {
      session.sessionState.conf
    } else {
      super.conf
    }
  }
   //SparkPlan 的唯一 ID，通过调用 SparkPlan.newPlanId() 来生成
  val id: Int = SparkPlan.newPlanId()

  /**
   * Return true if this stage of the plan supports row-based execution. A plan
   * can also support columnar execution (see `supportsColumnar`). Spark will decide
   * which execution to be called during query planning.
   */
    //判断当前计划是否支持基于行的执行。如果支持列存储（通过 supportsColumnar），则返回 false，否则返回 true
  def supportsRowBased: Boolean = !supportsColumnar

  /**
   * Return true if this stage of the plan supports columnar execution. A plan
   * can also support row-based execution (see `supportsRowBased`). Spark will decide
   * which execution to be called during query planning.
   */
    //判断当前计划是否支持基于列的执行。默认为 false
  def supportsColumnar: Boolean = false

  /**
   * The exact java types of the columns that are output in columnar processing mode. This
   * is a performance optimization for code generation and is optional.
   */
    //定义列存储模式下每列的 Java 类型，用于代码生成优化
  def vectorTypes: Option[Seq[String]] = None

  /** Overridden make copy also propagates sqlContext to copied plan. */
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    if (session != null) {
      session.withActive(super.makeCopy(newArgs))
    } else {
      super.makeCopy(newArgs)
    }
  }

  /**
   * @return The logical plan this plan is linked to.
   */
    //返回与当前计划关联的逻辑计划（LogicalPlan）。如果没有找到，则返回 None
  def logicalLink: Option[LogicalPlan] =
    getTagValue(SparkPlan.LOGICAL_PLAN_TAG)
      .orElse(getTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG))

  /**
   * Set logical plan link recursively if unset.
   */
  def setLogicalLink(logicalPlan: LogicalPlan): Unit = {
    setLogicalLink(logicalPlan, false)
  }
  //设置逻辑记录连接，inherited表示是否继承父节点的信息
  private def setLogicalLink(logicalPlan: LogicalPlan, inherited: Boolean = false): Unit = {
    // Stop at a descendant which is the root of a sub-tree transformed from another logical node.
    if (inherited && getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isDefined) {   //如果已经定义，直接返回
      return
    }

    val tag = if (inherited) {  //如果需要继承，则使用继承的标签
      SparkPlan.LOGICAL_PLAN_INHERITED_TAG
    } else {
      SparkPlan.LOGICAL_PLAN_TAG
    }
    setTagValue(tag, logicalPlan)
    children.foreach(_.setLogicalLink(logicalPlan, true))  //递归为子节点设置
  }

  /**
   * @return All metrics containing metrics of this SparkPlan.
   */
  def metrics: Map[String, SQLMetric] = Map.empty

  /**
   * Resets all the metrics.
   */
  def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
    children.foreach(_.resetMetrics())
  }

  /**
   * @return [[SQLMetric]] for the `name`.
   */
  def longMetric(name: String): SQLMetric = metrics(name)

  // TODO: Move to `DistributedPlan`
  /**
   * Specifies how data is partitioned across different nodes in the cluster.
   * Note this method may fail if it is invoked before `EnsureRequirements` is applied
   * since `PartitioningCollection` requires all its partitionings to have
   * the same number of partitions.
   */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /**
   * Specifies the data distribution requirements of all the children for this operator. By default
   * it's [[UnspecifiedDistribution]] for each child, which means each child can have any
   * distribution.
   *
   * If an operator overwrites this method, and specifies distribution requirements(excluding
   * [[UnspecifiedDistribution]] and [[BroadcastDistribution]]) for more than one child, Spark
   * guarantees that the outputs of these children will have same number of partitions, so that the
   * operator can safely zip partitions of these children's result RDDs. Some operators can leverage
   * this guarantee to satisfy some interesting requirement, e.g., non-broadcast joins can specify
   * HashClusteredDistribution(a,b) for its left child, and specify HashClusteredDistribution(c,d)
   * for its right child, then it's guaranteed that left and right child are co-partitioned by
   * a,b/c,d, which means tuples of same value are in the partitions of same index, e.g.,
   * (a=1,b=2) and (c=1,d=2) are both in the second partition of left and right child.
   */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /** Specifies sort order for each partition requirements on the input data for this operator. */
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to `doExecute` after
   * preparations.
   *
   * Concrete implementations of SparkPlan should override `doExecute`.
   */
  //物理节点实际执行的地方
  final def execute(): RDD[InternalRow] = executeQuery {
    if (isCanonicalizedPlan) {
      throw SparkException.internalError("A canonicalized plan is not supposed to be executed.")
    }
    doExecute()
  }

  /**
   * Returns the result of this query as a broadcast variable by delegating to `doExecuteBroadcast`
   * after preparations.
   *
   * Concrete implementations of SparkPlan should override `doExecuteBroadcast`.
   */
  final def executeBroadcast[T](): broadcast.Broadcast[T] = executeQuery {
    if (isCanonicalizedPlan) {
      throw SparkException.internalError("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteBroadcast()
  }

  /**
   * Returns the result of this query as an RDD[ColumnarBatch] by delegating to `doColumnarExecute`
   * after preparations.
   *
   * Concrete implementations of SparkPlan should override `doColumnarExecute` if `supportsColumnar`
   * returns true.
   */
  final def executeColumnar(): RDD[ColumnarBatch] = executeQuery {
    if (isCanonicalizedPlan) {
      throw SparkException.internalError("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteColumnar()
  }

  /**
   * Returns the result of writes as an RDD[WriterCommitMessage] variable by delegating to
   * `doExecuteWrite` after preparations.
   *
   * Concrete implementations of SparkPlan should override `doExecuteWrite`.
   */
  def executeWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = executeQuery {
    if (isCanonicalizedPlan) {
      throw SparkException.internalError("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteWrite(writeFilesSpec)
  }

  /**
   * Executes a query after preparing the query and adding query plan information to created RDDs
   * for visualization.
   */
  protected final def executeQuery[T](query: => T): T = {
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      waitForSubqueries()
      query  //这里是实际传进来的代码
    }
  }

  /**
   * List of (uncorrelated scalar subquery, future holding the subquery result) for this plan node.
   * This list is populated by [[prepareSubqueries]], which is called in [[prepare]].
   */
  @transient  //修饰符 @transient：这意味着该属性不会被 Spark 序列化
  private val runningSubqueries = new ArrayBuffer[ExecSubqueryExpression]

  /**
   * Finds scalar subquery expressions in this plan node and starts evaluating them.
   */
  //查找当前执行计划中的标量子查询，并启动它们的执行（准备工作）
  protected def prepareSubqueries(): Unit = {
    expressions.foreach {
      _.collect {
        case e: ExecSubqueryExpression =>
          e.plan.prepare()
          runningSubqueries += e
      }
    }
  }

  /**
   * Blocks the thread until all subqueries finish evaluation and update the results.
   */
  //阻塞当前线程，直到所有子查询完成评估并更新其结果
  protected def waitForSubqueries(): Unit = synchronized {
    // fill in the result of subqueries
    runningSubqueries.foreach { sub =>
      sub.updateResult()
    }
    runningSubqueries.clear()
  }

  /**
   * Whether the "prepare" method is called.
   */
  private var prepared = false

  /**
   * Prepares this SparkPlan for execution. It's idempotent.
   */
    //作用是为执行做准备。它的设计是 幂等的，意味着无论调用多少次，结果都是相同的
  final def prepare(): Unit = {
    // doPrepare() may depend on it's children, we should call prepare() on all the children first.
    //准备当前物理计划之前，必须先准备它的子计划（递归准备子节点
    children.foreach(_.prepare())
    synchronized {
      //只有在当前物理计划还没有被准备过时，才会执行准备工作。prepared 是一个标志位，确保每个物理计划只会被准备一次
      if (!prepared) {
        prepareSubqueries()
        doPrepare()
        prepared = true
      }
    }
  }

  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
   *
   * @note `prepare` method has already walked down the tree, so the implementation doesn't have
   * to call children's `prepare` methods.
   *
   * This will only be called once, protected by `this`.
   */
    //由具体的子类实现，在Execute之前做初始化
  protected def doPrepare(): Unit = {}

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  //解释执行的实现逻辑
  protected def doExecute(): RDD[InternalRow]

  /**
   * Produces the result of the query as a broadcast variable.
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    throw QueryExecutionErrors.doExecuteBroadcastNotImplementedError(nodeName)
  }

  /**
   * Produces the result of the query as an `RDD[ColumnarBatch]` if [[supportsColumnar]] returns
   * true. By convention the executor that creates a ColumnarBatch is responsible for closing it
   * when it is no longer needed. This allows input formats to be able to reuse batches if needed.
   */
  protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw SparkException.internalError(s"Internal Error ${this.getClass} has column support" +
      s" mismatch:\n${this}")
  }

  /**
   * Produces the result of the writes as an `RDD[WriterCommitMessage]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  protected def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    throw SparkException.internalError(s"Internal Error ${this.getClass} has write support" +
      s" mismatch:\n${this}")
  }

  /**
   * Converts the output of this plan to row-based if it is columnar plan.
   */
  def toRowBased: SparkPlan = if (supportsColumnar) ColumnarToRowExec(this) else this

  /**
   * Packing the UnsafeRows into byte array for faster serialization.
   * The byte arrays are in the following format:
   * [size] [bytes of UnsafeRow] [size] [bytes of UnsafeRow] ... [-1]
   *
   * UnsafeRow is highly compressible (at least 8 bytes for any column), the byte array is also
   * compressed.
   */
  //作用是将查询计划中的 UnsafeRow 数据序列化为压缩的字节数组，并返回一个包含这些字节数组的 RDD
  private def getByteArrayRdd(
      n: Int = -1, takeFromEnd: Boolean = false): RDD[(Long, ChunkedByteBuffer)] = {
    //n：表示要返回的 UnsafeRow 数量。默认值是 -1，表示返回所有的行。如果 n 大于 0，则只返回最后 n 行
    //takeFromEnd：布尔值，表示是否从末尾开始收集行数据。如果为 true，则从数据的末尾开始收集并返回
    execute().mapPartitionsInternal { iter =>
      var count = 0
      val buffer = new Array[Byte](4 << 10)  // 一个 4KB 的字节缓冲区，用来暂存 UnsafeRow 数据的序列化内容
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf) //创建一个压缩编解码器（CompressionCodec）。Spark 会根据配置使用不同的压缩算法（如 Snappy 或 LZ4），以提高序列化效率
      val cbbos = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate) //一个输出流，它会将字节写入内存缓冲区，并支持分块存储。当字节流达到一定大小时，它会将数据分块存储，以提高内存管理效率
      val out = new DataOutputStream(codec.compressedOutputStream(cbbos))

      if (takeFromEnd && n > 0) {
        // To collect n from the last, we should anyway read everything with keeping the n.
        // Otherwise, we don't know where is the last from the iterator.
        var last: Seq[UnsafeRow] = Seq.empty[UnsafeRow]
        val slidingIter = iter.map(_.copy()).sliding(n) //sliding(n) 会将迭代器转换为一个滑动窗口的迭代器，每次返回 n 行数据
        while (slidingIter.hasNext) {
          last = slidingIter.next().asInstanceOf[Seq[UnsafeRow]] } //保存最后 n 行的 UnsafeRow 数据
        var i = 0
        count = last.length
        while (i < count) {
          val row = last(i)
          out.writeInt(row.getSizeInBytes)
          row.writeToStream(out, buffer)
          i += 1
        }
      } else {
        // `iter.hasNext` may produce one row and buffer it, we should only call it when the
        // limit is not hit.
        while ((n < 0 || count < n) && iter.hasNext) {
          val row = iter.next().asInstanceOf[UnsafeRow]
          out.writeInt(row.getSizeInBytes)
          row.writeToStream(out, buffer)
          count += 1
        }
      }
      out.writeInt(-1)  //写入一个特殊的标志值 -1，表示数据流的结束
      out.flush()
      out.close()
      Iterator((count, cbbos.toChunkedByteBuffer))
    }
  }

  /**
   * Decodes the byte arrays back to UnsafeRows and put them into buffer.
   */
  private def decodeUnsafeRows(bytes: ChunkedByteBuffer): Iterator[InternalRow] = {
    val nFields = schema.length

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val cbbis = bytes.toInputStream()
    val ins = new DataInputStream(codec.compressedInputStream(cbbis))

    new NextIterator[InternalRow] {
      private var sizeOfNextRow = ins.readInt()
      private def _next(): InternalRow = {
        val bs = new Array[Byte](sizeOfNextRow)
        ins.readFully(bs)
        val row = new UnsafeRow(nFields)
        row.pointTo(bs, sizeOfNextRow)
        sizeOfNextRow = ins.readInt()
        row
      }

      override def getNext(): InternalRow = {
        if (sizeOfNextRow >= 0) {
          try {
            _next()
          } catch {
            case t: Throwable if ins != null =>
              ins.close()
              throw t
          }
        } else {
          finished = true
          null
        }
      }
      override def close(): Unit = ins.close()
    }
  }

  /**
   * Runs this query returning the result as an array.
   */
  //作用是执行查询并返回查询结果作为一个数组（Array[InternalRow]）
  def executeCollect(): Array[InternalRow] = {  //返回一个 Array[InternalRow]，表示查询结果的所有行
    val byteArrayRdd = getByteArrayRdd()  //返回一个包含压缩字节数据的 RDD

    val results = ArrayBuffer[InternalRow]()
    byteArrayRdd.collect().foreach { countAndBytes =>  //collect() 会触发整个计算过程，获取包含序列化字节数据的所有元素
      decodeUnsafeRows(countAndBytes._2).foreach(results.+=)
    }
    results.toArray
  }

  private[spark] def executeCollectIterator(): (Long, Iterator[InternalRow]) = {
    val countsAndBytes = getByteArrayRdd().collect()
    val total = countsAndBytes.map(_._1).sum
    val rows = countsAndBytes.iterator.flatMap(countAndBytes => decodeUnsafeRows(countAndBytes._2))
    (total, rows)
  }

  /**
   * Runs this query returning the result as an iterator of InternalRow.
   *
   * @note Triggers multiple jobs (one for each partition).
   */
  def executeToIterator(): Iterator[InternalRow] = {
    getByteArrayRdd().map(_._2).toLocalIterator.flatMap(decodeUnsafeRows)
  }

  /**
   * Runs this query returning the result as an array, using external Row format.
   */
  def executeCollectPublic(): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    executeCollect().map(converter(_).asInstanceOf[Row])
  }

  /**
   * Runs this query returning the first `n` rows as an array.
   *  返回查询的前n条记录
   * This is modeled after `RDD.take` but never runs any job locally on the driver.
   */
  def executeTake(n: Int): Array[InternalRow] = executeTake(n, takeFromEnd = false)

  /**
   * Runs this query returning the last `n` rows as an array.
   *
   * This is modeled after `RDD.take` but never runs any job locally on the driver.
   */
  def executeTail(n: Int): Array[InternalRow] = executeTake(n, takeFromEnd = true)

  private def executeTake(n: Int, takeFromEnd: Boolean): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }
    val limitScaleUpFactor = Math.max(conf.limitScaleUpFactor, 2)
    // TODO: refactor and reuse the code from RDD's take()
    val childRDD = getByteArrayRdd(n, takeFromEnd) //生成处理分区的代码，通过mapPartition函数实现业务逻辑，返回新的MapPartitionsRDD

    val buf = if (takeFromEnd) new ListBuffer[InternalRow] else new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.length < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = conf.limitInitialNumPartitions
      if (partsScanned > 0) {
        // If we didn't find any rows after the previous iteration, multiply by
        // limitScaleUpFactor and retry. Otherwise, interpolate the number of partitions we need
        // to try, but overestimate it by 50%. We also cap the estimation in the end.
        if (buf.isEmpty) {
          numPartsToTry = partsScanned * limitScaleUpFactor
        } else {
          val left = n - buf.length
          // As left > 0, numPartsToTry is always >= 1
          numPartsToTry = Math.ceil(1.5 * left * partsScanned / buf.length).toInt
          numPartsToTry = Math.min(numPartsToTry, partsScanned * limitScaleUpFactor)
        }
      }

      val parts = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val partsToScan = if (takeFromEnd) {
        // Reverse partitions to scan. So, if parts was [1, 2, 3] in 200 partitions (0 to 199),
        // it becomes [198, 197, 196].
        parts.map(p => (totalParts - 1) - p)
      } else {
        parts
      }
      val sc = sparkContext
      val res = sc.runJob(childRDD, (it: Iterator[(Long, ChunkedByteBuffer)]) =>
        if (it.hasNext) it.next() else (0L, new ChunkedByteBuffer()), partsToScan)   //真正提交MapPartitionsRDD到集群处理

      var i = 0

      if (takeFromEnd) {
        while (buf.length < n && i < res.length) {
          val rows = decodeUnsafeRows(res(i)._2)
          if (n - buf.length >= res(i)._1) {
            buf.prepend(rows.toArray[InternalRow]: _*)
          } else {
            val dropUntil = res(i)._1 - (n - buf.length)
            // Same as Iterator.drop but this only takes a long.
            var j: Long = 0L
            while (j < dropUntil) { rows.next(); j += 1L}
            buf.prepend(rows.toArray[InternalRow]: _*)
          }
          i += 1
        }
      } else {
        while (buf.length < n && i < res.length) {
          val rows = decodeUnsafeRows(res(i)._2)
          if (n - buf.length >= res(i)._1) {
            buf ++= rows.toArray[InternalRow]
          } else {
            buf ++= rows.take(n - buf.length).toArray[InternalRow]
          }
          i += 1
        }
      }
      partsScanned += partsToScan.size
    }
    buf.toArray
  }

  /**
   * Cleans up the resources used by the physical operator (if any). In general, all the resources
   * should be cleaned up when the task finishes but operators like SortMergeJoinExec and LimitExec
   * may want eager cleanup to free up tight resources (e.g., memory).
   */
  protected[sql] def cleanupResources(): Unit = {
    children.foreach(_.cleanupResources())
  }
}
//物理叶子节点
trait LeafExecNode extends SparkPlan with LeafLike[SparkPlan] {

  override def producedAttributes: AttributeSet = outputSet
  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val outputStr = s"${ExplainUtils.generateFieldString("Output", output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$outputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$outputStr
         |""".stripMargin
    }
  }
}

object UnaryExecNode {
  def unapply(a: Any): Option[(SparkPlan, SparkPlan)] = a match {
    case s: SparkPlan if s.children.size == 1 => Some((s, s.children.head))
    case _ => None
  }
}
//表示所有具有 单子节点 结构的执行节点。执行节点是 Spark SQL 查询计划的实际执行步骤
trait UnaryExecNode extends SparkPlan with UnaryLike[SparkPlan] {
  //生成该节点的详细字符串表示
  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val inputStr = s"${ExplainUtils.generateFieldString("Input", child.output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$inputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$inputStr
         |""".stripMargin
    }
  }
}
//两个子节点的物理执行节点
trait BinaryExecNode extends SparkPlan with BinaryLike[SparkPlan] {

  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val leftOutputStr = s"${ExplainUtils.generateFieldString("Left output", left.output)}"
    val rightOutputStr = s"${ExplainUtils.generateFieldString("Right output", right.output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$leftOutputStr
         |$rightOutputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$leftOutputStr
         |$rightOutputStr
         |""".stripMargin
    }
  }
}
