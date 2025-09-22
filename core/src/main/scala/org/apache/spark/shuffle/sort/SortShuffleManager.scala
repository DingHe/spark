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

package org.apache.spark.shuffle.sort

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.util.collection.OpenHashSet

/**
 * In sort-based shuffle, incoming records are sorted according to their target partition ids, then
 * written to a single map output file. Reducers fetch contiguous regions of this file in order to
 * read their portion of the map output. In cases where the map output data is too large to fit in
 * memory, sorted subsets of the output can be spilled to disk and those on-disk files are merged
 * to produce the final output file.
 *
 * Sort-based shuffle has two different write paths for producing its map output files:
 *
 *  - Serialized sorting: used when all three of the following conditions hold:
 *    1. The shuffle dependency specifies no map-side combine.
 *    2. The shuffle serializer supports relocation of serialized values (this is currently
 *       supported by KryoSerializer and Spark SQL's custom serializers).
 *    3. The shuffle produces fewer than or equal to 16777216 output partitions.
 *  - Deserialized sorting: used to handle all other cases.
 *
 * -----------------------
 * Serialized sorting mode
 * -----------------------
 *
 * In the serialized sorting mode, incoming records are serialized as soon as they are passed to the
 * shuffle writer and are buffered in a serialized form during sorting. This write path implements
 * several optimizations:
 *
 *  - Its sort operates on serialized binary data rather than Java objects, which reduces memory
 *    consumption and GC overheads. This optimization requires the record serializer to have certain
 *    properties to allow serialized records to be re-ordered without requiring deserialization.
 *    See SPARK-4550, where this optimization was first proposed and implemented, for more details.
 *
 *  - It uses a specialized cache-efficient sorter ([[ShuffleExternalSorter]]) that sorts
 *    arrays of compressed record pointers and partition ids. By using only 8 bytes of space per
 *    record in the sorting array, this fits more of the array into cache.
 *
 *  - The spill merging procedure operates on blocks of serialized records that belong to the same
 *    partition and does not need to deserialize records during the merge.
 *
 *  - When the spill compression codec supports concatenation of compressed data, the spill merge
 *    simply concatenates the serialized and compressed spill partitions to produce the final output
 *    partition.  This allows efficient data copying methods, like NIO's `transferTo`, to be used
 *    and avoids the need to allocate decompression or copying buffers during the merge.
 *
 * For more details on these optimizations, see SPARK-7081.
 */
// 理基于排序的 Shuffle 操作的具体实现。它继承了 ShuffleManager 接口，并在此基础上实现了更复杂的 Shuffle 过程，涉及数据的排序、序列化/反序列化等
private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  import SortShuffleManager._

  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
    //该属性存储每个 shuffle 操作对应的任务 ID 集合。键是 shuffleId，值是 OpenHashSet[Long]，即每个 shuffleId 对应的所有 map 任务 ID 集合
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()
   //它是执行器级别 Shuffle 数据 I/O 组件，负责 Shuffle 数据的输入输出操作
  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)

  override val shuffleBlockResolver =  //用于通过块坐标获取 Shuffle 数据。它负责处理 Shuffle 数据的存储和检索
    new IndexShuffleBlockResolver(conf, taskIdMapsForShuffle = taskIdMapsForShuffle)

  /** 该方法用于注册一个 Shuffle 操作，并返回一个 ShuffleHandle 供任务使用。它根据 ShuffleDependency 中的配置决定使用哪种 Shuffle 路径
   * Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      //1、map端combine，则可以跳过 2、dependency的分区数量少于SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD，则不可以跳过
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
        //是否可以使用序列化器Shuffle
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) { //1、序列化对象重定位 2、map-size聚合 3、超过阀值
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      new SerializedShuffleHandle[K, V](
        shuffleId, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, dependency)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from a range of map outputs(startMapIndex to endMapIndex-1, inclusive).
   * If endMapIndex=Int.MaxValue, the actual endMapIndex will be changed to the length of total map
   * outputs of the shuffle in `getMapSizesByExecutorId`.
   *
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,  //包含 Shuffle 的元信息，例如 Shuffle ID 和依赖关系
      startMapIndex: Int, //读取的 Map 输出范围 [startMapIndex, endMapIndex - 1]
      endMapIndex: Int,
      startPartition: Int,  //读取的 Reduce 分区范围 [startPartition, endPartition - 1]
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, _, C]] //将 ShuffleHandle 转换为 BaseShuffleHandle，以便访问具体的依赖信息和 Shuffle ID
    val (blocksByAddress, canEnableBatchFetch) =
      if (baseShuffleHandle.dependency.isShuffleMergeFinalizedMarked) { //如果 Shuffle 合并已完成（即启用了 Push-Based Shuffle），通过 getPushBasedShuffleMapSizesByExecutorId 获取 Map 输出的大小信息
        val res = SparkEnv.get.mapOutputTracker.getPushBasedShuffleMapSizesByExecutorId(
          handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
        (res.iter, res.enableBatchFetch)
      } else {  //使用传统方法 getMapSizesByExecutorId
        val address = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
        (address, true)
      }
    new BlockStoreShuffleReader(  //一个具体的 Shuffle 数据读取实现类，用于从存储系统（如本地磁盘或远程存储）读取数据
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context, metrics,
      shouldBatchFetch =
        canEnableBatchFetch && canUseBatchFetch(startPartition, endPartition, context))
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      handle.shuffleId, _ => new OpenHashSet[Long](16)) //将 mapId 注册到 taskIdMapsForShuffle 中，确保每个 mapId 都被记录，方便后续管理。
    mapTaskIds.synchronized { mapTaskIds.add(mapId) }
    val env = SparkEnv.get
    handle match {  //选择合适的 ShuffleWriter
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new BypassMergeSortShuffleWriter(
          env.blockManager,
          bypassMergeSortHandle,
          mapId,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(other, mapId, context, metrics, shuffleExecutorComponents)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
      mapTaskIds.iterator.foreach { mapTaskId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapTaskId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}


private[spark] object SortShuffleManager extends Logging {

  /**
   * The maximum number of shuffle output partitions that SortShuffleManager supports when
   * buffering map outputs in a serialized form. This is an extreme defensive programming measure,
   * since it's extremely unlikely that a single shuffle produces over 16 million output partitions.
   */
  //表示序列化模式下支持的最大 Shuffle 输出分区数。这个值非常大（超过 1600 万），是一个防御性编程措施，用于防止在极端情况下因分区数过多而导致的数据结构溢出
  val MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE =
    PackedRecordPointer.MAXIMUM_PARTITION_ID + 1

  /**
   * The local property key for continuous shuffle block fetching feature.
   */
  //用于在任务的本地属性中标记是否启用了连续 Shuffle 块批量拉取功能
  val FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY =
    "__fetch_continuous_blocks_in_batch_enabled"

  /**
   * Helper method for determining whether a shuffle reader should fetch the continuous blocks
   * in batch.
   */
  // 用于判断 Shuffle 读取器是否可以批量拉取连续的Shuffle块
  def canUseBatchFetch(startPartition: Int, endPartition: Int, context: TaskContext): Boolean = {
    val fetchMultiPartitions = endPartition - startPartition > 1
    fetchMultiPartitions &&
      context.getLocalProperty(FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY) == "true"
  }

  /**
   * Helper method for determining whether a shuffle should use an optimized serialized shuffle
   * path or whether it should fall back to the original path that operates on deserialized objects.
   */
  // 用于判断是否可以使用优化的序列化 Shuffle 路径。
  // 这种路径直接操作序列化字节，避免了不必要的反序列化和重新序列化，从而提高性能
  // 重排序 = 序列化后的二进制数据仍然保留了逻辑顺序，可以直接比较和排序
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shufId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) { //1.检查当前的序列化器是否支持对象的重定位
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
        s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.mapSideCombine) { //2.需要 Map 端聚合（如 MapReduce 的 Combiner 操作），Map 端聚合通常需要对数据进行反序列化处理
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because we need to do " +
        s"map-side aggregation")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) { //3.检查分区数是否超出限制
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      true
    }
  }
  //用于加载和初始化Shuffle 执行器组件。这个方法确保在执行器上可以加载正确的 Shuffle Data IO 实现（如外部 Shuffle 服务）
  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX)
        .toMap
    //初始化IO组件
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }
}

/** 继承此类，表示使用了基于序列化数据排序的shuffle处理器
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * serialized shuffle.
 */
private[spark] class SerializedShuffleHandle[K, V](
  shuffleId: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {
}

/** 继承此类，表示采用了旁路归并排序的shuffle器
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * bypass merge sort shuffle path.
 */
private[spark] class BypassMergeSortShuffleHandle[K, V](
  shuffleId: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {
}
