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

package org.apache.spark.rdd

import java.io.{FileNotFoundException, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.reflect.ClassTag

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.hdfs.BlockMissingException
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileInputFormat, FileSplit, InvalidInputException}
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.security.AccessControlException

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rdd.NewHadoopRDD.NewHadoopMapPartitionsWithSplitRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager, Utils}
// NewHadoopPartition 类是 Spark 中用于处理基于新版 Hadoop I/O API (org.apache.hadoop.mapreduce) 的数据源的具体分区实现。
// 核心作用是作为 Spark 分区 (Partition trait) 和底层 Hadoop 输入切片 (InputSplit) 之间的桥梁和容器。每个 NewHadoopPartition 实例都封装了一个 Hadoop InputSplit，代表了 RDD 中一个独立的、可并行处理的数据块。
// 封装 Hadoop Split： 持有底层的 Hadoop InputSplit 对象，该对象定义了数据的位置、长度和读取范围。
// 实现可序列化： 由于 InputSplit 本身可能不完全兼容 Spark 的序列化机制，NewHadoopPartition 使用 SerializableWritable 来封装 InputSplit，确保它可以在 Spark Driver 和 Executor 之间安全传输。
// 提供唯一标识： 结合其父 RDD 的 ID (rddId) 和自己的索引 (index)，提供了一个更强的唯一标识（通过重写的 hashCode），用于区分不同 RDD 中的同名分区。
private[spark] class NewHadoopPartition(
    rddId: Int, // 父 RDD ID，该分区所属的 NewHadoopRDD 的唯一标识符。用于在 hashCode 计算中提供更强的唯一性。
    val index: Int, // 分区索引，该分区在其父 RDD 中的顺序索引（从 0 开始）。这是继承自 Partition trait 的核心标识。
    rawSplit: InputSplit with Writable)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the new MapReduce API (`org.apache.hadoop.mapreduce`).
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param inputFormatClass Storage format of the data to be read.
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 *
 * @note Instantiating this class directly is not recommended, please use
 * `org.apache.spark.SparkContext.newAPIHadoopRDD()`
 */
// NewHadoopRDD 是 Spark 核心库中的一个 RDD 实现，它提供了一种核心功能，用于使用 新版 Hadoop MapReduce API (org.apache.hadoop.mapreduce) 来读取存储在 Hadoop 兼容系统（如 HDFS、S3、HBase）中的数据。
// 核心职责：
// 数据抽象与解耦： 将底层 Hadoop InputFormat（负责文件切分和记录读取）的复杂 I/O 逻辑抽象为标准的 Spark RDD 和 Partition 概念。
// 分区划分： 负责调用 Hadoop 的 InputFormat.getSplits() 方法，将数据源切分成逻辑上的 InputSplit，并将其转化为 Spark 的 Partition，每个分区对应一个读取任务。
// 任务执行： 在执行器（Executor）端，为每个分区创建 Hadoop 的 RecordReader，负责实际读取数据，并将键值对 (K, V) 流式地返回给 Spark。
@DeveloperApi
class NewHadoopRDD[K, V](
    sc : SparkContext,
    inputFormatClass: Class[_ <: InputFormat[K, V]], // 输入格式类 ，底层 Hadoop InputFormat 的类对象，它定义了如何读取和切分数据。
    keyClass: Class[K], // Key 的类，Hadoop InputFormat 读取的 Key 的数据类型。
    valueClass: Class[V], // Value 的类 ，Hadoop InputFormat 读取的 Value 的数据类型（通常是实际的数据记录）。
    @transient private val _conf: Configuration) // Hadoop 配置，原始的 Hadoop 配置对象（JobConf），包含了文件路径、SerDe 信息和所有作业所需的配置参数
  extends RDD[(K, V)](sc, Nil) with Logging {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  // 广播配置
  // 将原始的 _conf 包装成可序列化对象后，通过 Spark 的广播机制 (sc.broadcast) 发送到集群所有节点。这是为了高效地在执行器上访问配置。
  private val confBroadcast = sc.broadcast(new SerializableConfiguration(_conf))
  // private val serializableConf = new SerializableWritable(_conf)
  // 生成一个基于时间的唯一 ID，模拟 Hadoop JobTracker ID，用于创建任务的唯一标识符（JobID, TaskAttemptID）
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)
  // 从 Spark 配置 (spark.hadoop.cloneConf) 读取布尔值。如果为 true，则在每个任务中克隆 Configuration 对象，以解决 Hadoop 配置的线程不安全问题（避免并发读写冲突
  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)
  // 忽略损坏文件
  // 从 Spark 配置中读取标志，指示是否应忽略读取过程中遇到的损坏文件导致的 I/O 异常。
  private val ignoreCorruptFiles = sparkContext.conf.get(IGNORE_CORRUPT_FILES)
  // 忽略丢失文件
  // 从 Spark 配置中读取标志，指示如果文件路径不存在，是否应忽略并返回空分区。
  private val ignoreMissingFiles = sparkContext.conf.get(IGNORE_MISSING_FILES)
  // 忽略空切片
  // 从 Spark 配置中读取标志，指示是否应过滤掉长度为 0 的 InputSplit
  private val ignoreEmptySplits = sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)
  // 获取任务配置
  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546, SPARK-10611).  This
      // problem occurs somewhat rarely because most jobs treat the configuration as though it's
      // immutable.  One solution, implemented here, is to clone the Configuration object.
      // Unfortunately, this clone can be very expensive.  To avoid unexpected performance
      // regressions for workloads and Hadoop versions that do not suffer from these thread-safety
      // issues, this cloning is disabled by default.
      NewHadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        // The Configuration passed in is actually a JobConf and possibly contains credentials.
        // To keep those credentials properly we have to create a new JobConf not a Configuration.
        if (conf.isInstanceOf[JobConf]) {
          new JobConf(conf)
        } else {
          new Configuration(conf)
        }
      }
    } else {
      conf
    }
  }
  // 获取 RDD 分区
  // 负责调用底层 Hadoop API，将数据源切分为 Spark RDD 的分区
  override def getPartitions: Array[Partition] = {
    // 通过反射机制，实例化配置的 Hadoop InputFormat 类。这是负责文件切分和记录读取的关键对象。
    val inputFormat = inputFormatClass.getConstructor().newInstance()
    // setMinPartitions below will call FileInputFormat.listStatus(), which can be quite slow when
    // traversing a large number of directories and files. Parallelize it.
    // 并行化文件状态获取
    // 如果配置中未设置 FileInputFormat.LIST_STATUS_NUM_THREADS，则将其设置为当前 JVM 可用的处理器核心数。这能并行执行文件列表操作，提高大型数据集的启动速度。
    // 解释设置并行度的原因：FileInputFormat.listStatus() 可能会很慢，通过设置并行线程数来加速文件状态列表的获取。
    _conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString)
    inputFormat match {
      case configurable: Configurable =>
        // 如果 inputFormat 实现了 Configurable 接口
        // 调用 setConf 方法，将 RDD 持有的配置（_conf）传递给 InputFormat 实例，使其完成初始化或配置更新。
        configurable.setConf(_conf)
        //如果没有实现 Configurable，则不执行任何操作
      case _ =>
    }
    try {
      // 获取原始 Split
      //要求 InputFormat 根据上下文和配置将输入数据源切分成 Hadoop InputSplit 列表
      val allRowSplits = inputFormat.getSplits(new JobContextImpl(_conf, jobId)).asScala
      val rawSplits = if (ignoreEmptySplits) {
        allRowSplits.filter(_.getLength > 0)
      } else {
        allRowSplits
      }
      //大型文件警告检查开始
      if (rawSplits.length == 1 && rawSplits(0).isInstanceOf[FileSplit]) {
        val fileSplit = rawSplits(0).asInstanceOf[FileSplit]
        val path = fileSplit.getPath
        if (fileSplit.getLength > conf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
          val codecFactory = new CompressionCodecFactory(_conf)
          // 判断文件是否可切分
          if (Utils.isFileSplittable(path, codecFactory)) {
            logWarning(s"Loading one large file ${path.toString} with only one partition, " +
              s"we can increase partition numbers for improving performance.")
          } else {
            logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
              s"partition, because the file is compressed by unsplittable compression codec.")
          }
        }
      }
      // 根据 rawSplits 的数量，创建一个新的 Spark Partition 数组来存储最终结果
      val result = new Array[Partition](rawSplits.size)
      for (i <- rawSplits.indices) {
        result(i) =
            new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
      }
      result
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(s"${_conf.get(FileInputFormat.INPUT_DIR)} doesn't exist and no" +
            s" partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }
  // NewHadoopRDD 的核心方法 compute 的代码详细解读。该方法在 Executor 端 执行，负责为指定的 RDD 分区创建 Hadoop RecordReader 并返回一个可迭代的数据流。
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    // 实例化一个匿名的 Scala Iterator，所有的读取逻辑都封装在这个迭代器内部。
    val iter = new Iterator[(K, V)] {
      private val split = theSplit.asInstanceOf[NewHadoopPartition]
      logInfo("Input split: " + split.serializableHadoopSplit)
      private val conf = getConf

      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Sets InputFileBlockHolder for the file block's information
      // 如果它是 FileSplit，则设置 InputFileBlockHolder（一个线程局部变量），记录当前正在读取的文件路径、起始偏移量和长度。
      // 这主要用于 Spark SQL 等高级优化。
      // 非 FileSplit 类型则清除该持有者。
      split.serializableHadoopSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      // 尝试获取一个回调函数，该函数能返回当前线程在底层文件系统（如 HDFS）中已读取的字节数。
      // 这需要在创建 RecordReader 之前完成，因为 RecordReader 的构造函数可能已经开始读取字节
      private val getBytesReadCallback: Option[() => Long] =
        split.serializableHadoopSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
          case _ => None
        }

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      // 用于将当前线程读取的字节数累加到 inputMetrics 中。它使用 existingBytesRead 来确保在处理多个分区时，度量值是正确累加的。
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      private val format = inputFormatClass.getConstructor().newInstance()
      format match {
        case configurable: Configurable =>
          configurable.setConf(conf)
        case _ =>
      }
      private val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      private val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
      private var finished = false
      //创建 RecordReader
      private var reader =
        try {
          val _reader = format.createRecordReader(
            split.serializableHadoopSplit.value, hadoopAttemptContext)
          _reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
          _reader
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file: ${split.serializableHadoopSplit}", e)
            finished = true
            null
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e @ (_ : AccessControlException | _ : BlockMissingException) => throw e
          case e: IOException if ignoreCorruptFiles =>
            logWarning(
              s"Skipped the rest content in the corrupted file: ${split.serializableHadoopSplit}",
              e)
            finished = true
            null
        }

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener[Unit] { context =>
        // Update the bytesRead before closing is to make sure lingering bytesRead statistics in
        // this thread get correctly added.
        updateBytesRead()
        close()
      }

      private var havePair = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          try {
            finished = !reader.nextKeyValue
          } catch {
            case e: FileNotFoundException if ignoreMissingFiles =>
              logWarning(s"Skipped missing file: ${split.serializableHadoopSplit}", e)
              finished = true
            // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
            case e: FileNotFoundException if !ignoreMissingFiles => throw e
            case e @ (_ : AccessControlException | _ : BlockMissingException) => throw e
            case e: IOException if ignoreCorruptFiles =>
              logWarning(
                s"Skipped the rest content in the corrupted file: ${split.serializableHadoopSplit}",
                e)
              finished = true
          }
          if (finished) {
            // Close and release the reader here; close() will also be called when the task
            // completes, but for tasks that read from many files, it helps to release the
            // resources early.
            close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw SparkCoreErrors.endOfStreamError()
        }
        havePair = false
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        (reader.getCurrentKey, reader.getCurrentValue)
      }

      private def close(): Unit = {
        if (reader != null) {
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (split.serializableHadoopSplit.value.isInstanceOf[FileSplit] ||
                     split.serializableHadoopSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.serializableHadoopSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new NewHadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  override def getPreferredLocations(hsplit: Partition): Seq[String] = {
    val split = hsplit.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value
    val locs = HadoopRDD.convertSplitLocationInfo(split.getLocationInfo)
    locs.getOrElse(split.getLocations.filter(_ != "localhost"))
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

}

private[spark] object NewHadoopRDD {
  /**
   * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
   * Therefore, we synchronize on this lock before calling new Configuration().
   */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /**
   * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
   * the given function rather than the index of the partition.
   */
  private[spark] class NewHadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
      prev: RDD[T],
      f: (InputSplit, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[NewHadoopPartition]
      val inputSplit = partition.serializableHadoopSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }
}
