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

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.serializer._
import org.apache.spark.shuffle.{ShufflePartitionPairsWriter, ShuffleWriteMetricsReporter}
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.shuffle.checksum.ShuffleChecksumSupport
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter, ShuffleBlockId}
import org.apache.spark.util.{CompletionIterator, Utils => TryUtils}

/**
 * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
 *  ExternalSorter 可以对多个类型为 (K, V) 的键值对进行排序和潜在合并，以生成类型为 (K, C) 的键-聚合器（key-combiner）对
 * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
 *  它首先使用一个 分区器（Partitioner） 将键值对分组到不同的分区中，然后可选地使用自定义的 比较器（Comparator） 对每个分区内的键进行排序
 * optionally sorts keys within each partition using a custom Comparator. Can output a single
 *  ExternalSorter 可以输出一个单一的、已分区的文件，其中每个分区对应不同的字节范围，这非常适合于 Shuffle 拉取（fetch）
 * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
 * 如果合并（combining） 被禁用，类型 C 必须等于类型 V——我们会在最后对对象进行类型转换（cast）
 * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
 *
 * Note: Although ExternalSorter is a fairly generic sorter, some of its configuration is tied
 * to its use in sort-based shuffle (for example, its block compression is controlled by
 * `spark.shuffle.compress`).  We may need to revisit this if ExternalSorter is used in other
 * non-shuffle contexts where we might want to use different configuration settings.
 * 注意：尽管 ExternalSorter 是一个相当通用的排序器，但它的一些配置与其在基于排序的 Shuffle 中的使用紧密相关（例如，它的块压缩由 spark.shuffle.compress 参数控制）。
 * 如果 ExternalSorter 在其他非 Shuffle 场景中使用，并且我们希望使用不同的配置设置，我们可能需要重新审视这一点
 *
 * @param aggregator optional Aggregator with combine functions to use for merging data
 * @param partitioner optional Partitioner; if given, sort by partition ID and then key
 * @param ordering optional Ordering to sort keys within each partition; should be a total ordering
 * @param serializer serializer to use when spilling to disk
 *
 * Note that if an Ordering is given, we'll always sort using it, so only provide it if you really
 * want the output keys to be sorted. In a map task without map-side combine for example, you
 * probably want to pass None as the ordering to avoid extra sorting. On the other hand, if you do
 * want to do combining, having an Ordering is more efficient than not having it.
 * 如果你真的想让输出的键（keys）被排序，那么就需要提供 Ordering。这会确保结果数据是按照你指定的顺序排列的
 * 如果你不需要排序，那么应该传入 None。例如，在没有 Map端聚合（map-side combine） 的 Map 任务中，如果传入了 Ordering，
 * Spark 会执行不必要的排序操作，这会增加额外的性能开销
 * Users interact with this class in the following way:
 *
 * 1. Instantiate an ExternalSorter.  实例化类
 *
 * 2. Call insertAll() with a set of records. 把记录插入
 *
 * 3. Request an iterator() back to traverse sorted/aggregated records. 使用迭代器遍历记录
 *     - or -
 *    Invoke writePartitionedMapOutput() to create a file containing sorted/aggregated outputs
 *    that can be used in Spark's sort shuffle.  或者通过writePartitionedMapOutput写出分区文件
 *
 * At a high level, this class works internally as follows:
 *   ExternalSorter 在内部的工作原理可以概括为以下三个步骤
 *
 *  - We repeatedly fill up buffers of in-memory data, using either a PartitionedAppendOnlyMap if
 *    we want to combine by key, or a PartitionedPairBuffer if we don't.
 *    Inside these buffers, we sort elements by partition ID and then possibly also by key.
 *    To avoid calling the partitioner multiple times with each key, we store the partition ID
 *    alongside each record.
 *    首先，它会反复将数据填入内存缓冲区
 *    如果需要按键进行聚合（combine by key），它会使用一个PartitionedAppendOnlyMap 数据结构
 *    如果不需要聚合，则使用 PartitionedPairBuffer
 *    在这些缓冲区内部，元素会首先按分区 ID（partition ID） 排序，然后可选地按键（key）进行排序
 *  - When each buffer reaches our memory limit, we spill it to a file. This file is sorted first
 *    by partition ID and possibly second by key or by hash code of the key, if we want to do
 *    aggregation. For each file, we track how many objects were in each partition in memory, so we
 *    don't have to write out the partition ID for every element.
 *    当每个内存缓冲区达到预设的内存限制时，其中的数据就会被溢写（spill） 到一个临时文件中
 *    这个文件中的数据会先按分区 ID 排序，然后可选地按键或键的哈希码进行二次排序（如果需要聚合）
 *    为了避免为每个元素都写入分区 ID，ExternalSorter 会跟踪并记录每个文件在内存中属于哪个分区、有多少对象
 *  - When the user requests an iterator or file output, the spilled files are merged, along with
 *    any remaining in-memory data, using the same sort order defined above (unless both sorting
 *    and aggregation are disabled). If we need to aggregate by key, we either use a total ordering
 *    from the ordering parameter, or read the keys with the same hash code and compare them with
 *    each other for equality to merge values.
 *    当用户请求获取一个迭代器或将数据写入文件时，所有溢写到磁盘的临时文件会与内存中剩余的数据一起进行合并
 *    合并过程遵循与上述相同的排序顺序（除非排序和聚合都被禁用）
 *    如果需要按键进行聚合，它会使用 Ordering 参数来定义一个全序关系（total ordering），或者通过读取具有相同哈希码的键并比较它们是否相等来合并值
 *  - Users are expected to call stop() at the end to delete all the intermediate files.
 */
// 作用是在内存中对键值对进行排序和聚合，并在内存不足时将数据溢写（spill）到磁盘，
// 最后将内存和磁盘中的数据进行合并，并按分区和键进行排序，以准备好 Shuffle 的下一步操作
// 内存缓冲：它可以在内存中存储键值对，如果用户提供了聚合器（aggregator），它会立即进行 Map 端聚合，减少数据量；否则，它会直接缓冲原始数据
// 内存管理与溢写：当内存使用量达到设定的阈值时，ExternalSorter 会将内存中的数据块排序并溢写到一个或多个临时文件中
// 归并排序：当所有输入数据都处理完毕后，它会将内存中剩余的数据和磁盘上的所有溢写文件进行多路归并排序（merge-sort），最终生成一个全局有序（按分区和键）的输出
private[spark] class ExternalSorter[K, V, C](
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,  //用于在数据进入内存时进行Map端预聚合。None 表示不进行聚合
    partitioner: Option[Partitioner] = None,  //用于计算每个键值对应该属于哪个分区
    ordering: Option[Ordering[K]] = None, //用于决定每个分区内部的键的排序顺序
    serializer: Serializer = SparkEnv.get.serializer)  //用于序列化/反序列化数据
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
  with Logging with ShuffleChecksumSupport {

  private val conf = SparkEnv.get.conf

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1) //分区数，默认为1
  private val shouldPartition = numPartitions > 1  //是否需要分区
  //根据键计算其分区 ID。如果不需要分区（shouldPartition 为 false），则总是返回分区 0
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val serializerManager = SparkEnv.get.serializerManager
  // 用于写磁盘文件时序列化
  private val serInstance = serializer.newInstance()

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024  //文件缓冲区大小

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = conf.get(config.SHUFFLE_SPILL_BATCH_SIZE) //序列化批量大小，避免大对象序列化时占用过多内存

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  @volatile private var map = new PartitionedAppendOnlyMap[K, C] // 当 aggregator 存在时使用，它是一个哈希表，用于在内存中对相同键的值进行聚合
  @volatile private var buffer = new PartitionedPairBuffer[K, C] // 当 aggregator 不存在时使用，它是一个简单的可增长数组，用于高效地追加键值对

  // Total spilling statistics
  private var _diskBytesSpilled = 0L  //记录到目前为止溢写到磁盘的总字节数
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Peak size of the in-memory data structure observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L //记录任务执行期间内存使用的峰值
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes
  //检查当前 ExternalSorter 是否处于Shuffle 排序模式
  @volatile private var isShuffleSort: Boolean = true
  //强制溢写的磁盘文件
  private val forceSpillFiles = new ArrayBuffer[SpilledFile]
  //支持磁盘溢写的迭代器
  @volatile private var readingIterator: SpillableIterator = null
  //每个分区的CheckSum
  private val partitionChecksums = createPartitionChecksums(numPartitions, conf)

  def getChecksums: Array[Long] = getChecksumValues(partitionChecksums)

  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.

  //键的比较器。如果提供了 ordering，则使用它；否则，使用基于哈希码的比较器。这决定了分区内的排序方式
  private val keyComparator: Comparator[K] = ordering.getOrElse((a: K, b: K) => {
    val h1 = if (a == null) 0 else a.hashCode()
    val h2 = if (b == null) 0 else b.hashCode()
    if (h1 < h2) -1 else if (h1 == h2) 0 else 1
  })
  //排序的逻辑
  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  // Information about a spilled file. Includes sizes in bytes of "batches" written by the
  // serializer as we periodically reset its stream, as well as number of elements in each
  // partition, used to efficiently keep track of partitions when merging.
  //记录溢写文件的元信息
  private[this] case class SpilledFile(
    file: File,
    blockId: BlockId,
    serializerBatchSizes: Array[Long], //记录每个批次字节的长度
    elementsPerPartition: Array[Long]) //每个分区的元素个数

  //用于存储所有溢写到磁盘的临时文件的元数据（文件名、块ID、分区信息等）
  private val spills = new ArrayBuffer[SpilledFile]

  /**
   * Number of files this sorter has spilled so far.
   * Exposed for testing.
   */
  private[spark] def numSpills: Int = spills.size
  // 将输入迭代器中的所有记录插入到 ExternalSorter 中
  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) { //如果有 aggregator，会执行聚合操作
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead() //记录读取的元素
        kv = records.next()
        map.changeValue((getPartition(kv._1), kv._1), update)
        maybeSpillCollection(usingMap = true)
      }
    } else { //否则直接存储
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
      }
    }
  }

  /**
   * Spill the current in-memory collection to disk if needed.
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  // 检查当前内存使用量，如果超过阈值，则触发溢写
  // 根据 usingMap 判断当前使用的是 map 还是 buffer，然后调用 estimateSize() 估算内存大小，最后调用 maybeSpill 方法进行判断和溢写
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
      estimatedSize = map.estimateSize()
      if (maybeSpill(map, estimatedSize)) {
        //写磁盘后，重新分配一个数据结构继续
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) {
        //写磁盘后，重新分配一个buffer继续
        buffer = new PartitionedPairBuffer[K, C]
      }
    }
    //更新内存的峰值
    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   *
   * @param collection whichever collection we're using (map or buffer)
   */
  // 将指定的内存集合（map 或 buffer）溢写到磁盘
  // 在Spillable 接口中调用
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    //获取一个排序好的、可写入的迭代器
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    //将数据写入磁盘文件
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    spills += spillFile
  }

  /** 如果当前任务处于 shuffle 阶段（isShuffleSort = true），强制溢出会返回 false
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  override protected[this] def forceSpill(): Boolean = {
    if (isShuffleSort) {
      false
    } else {
      assert(readingIterator != null)
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        map = null
        buffer = null
      }
      isSpilled
    }
  }

  /**
   * Spill contents of in-memory iterator to a temporary file on disk.
   */
  // 将内存迭代器中的内容写入一个临时的磁盘文件
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator[K, C])
      : SpilledFile = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    //获取一个临时的 Shuffle 块 ID 和文件
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    // 创建一个 DiskBlockObjectWriter 来高效地将序列化数据写入文件
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // List of batch sizes (bytes) in the order they are written to disk
    // 记录每个批次的大小
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    // 记录每个分区有多少条记录
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is committed at the end of this process.
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      //循环写入每条数据
      while (inMemoryIterator.hasNext) {
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        //写入数据记录
        inMemoryIterator.writeNext(writer)
        //记录每个分区的条数
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1
        //达到批次大小，就刷入磁盘
        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      if (objectsWritten > 0) {
        flush()
        writer.close()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.closeAndDelete()
      }
    }

    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }

  /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   *
   * Returns an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   */
  // 将所有溢写文件和内存中的数据进行多路归并排序，并返回一个按分区排序的迭代器。
  //inMemory: Iterator[((Int, K), C)]：内存中剩余的数据的迭代器，这些数据已经按分区 ID 和键进行了排序
  // Iterator[(Int, Iterator[Product2[K, C]])]: 方法的返回类型。它返回一个迭代器，其中每个元素都是一个元组 (Int, Iterator[...])。
  // 元组的第一个元素是分区 ID，第二个元素是该分区内所有键值对的迭代器
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {
    //为每个溢写文件创建一个 SpillReader，这些 SpillReader 可以按分区顺序读取文件中的数据
    val readers = spills.map(new SpillReader(_))
    //缓冲迭代器允许在不消耗元素的情况下查看下一个元素（通过 head 方法）
    val inMemBuffered = inMemory.buffered
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      //这是一个关键的步骤，它将所有数据源的迭代器收集起来
      //调用 readNextPartition() 方法，该方法返回一个只包含当前分区 p 数据的迭代器
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      if (aggregator.isDefined) {
        //如果存在聚合器，意味着需要进行键的聚合操作
        // Perform partial aggregation across partitions
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        //如果没有聚合器，但存在排序器（ordering），则只需要对键进行排序，而不需要聚合
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {
        //既没有聚合器，也没有排序器
        //只需要将所有迭代器中的数据拉平（flatten）即可
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   */
  //多路归并排序。它接收一个已排序的迭代器序列，通过使用最小堆（min-heap） 的数据结构，将这些迭代器中的数据合并为一个全局有序的迭代器
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
      : Iterator[Product2[K, C]] = {
    //过滤掉输入序列中所有空的迭代器
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    // Use the reverse order (compare(y,x)) because PriorityQueue dequeues the max
    //创建了一个优先级队列（PriorityQueue），在 Scala 中，它默认是一个最大堆（max-heap），即 dequeue() 会返回最大的元素
    //为了将最大堆转换为最小堆，比较器中对 x 和 y 的比较顺序被反转了
    val heap = new mutable.PriorityQueue[Iter]()(
      (x: Iter, y: Iter) => comparator.compare(y.head._1, x.head._1))
    //将所有非空的缓冲迭代器加入到优先级队列中。此时，优先级队列的顶部是所有迭代器中键值最小的那个元素所在的迭代器
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      //检查优先级队列是否为空。只要队列中还有迭代器，就说明还有元素可以被归并
      override def hasNext: Boolean = heap.nonEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        //从优先级队列中取出键最小的那个迭代器
        val firstBuf = heap.dequeue()
        //从这个迭代器中取出下一个（即当前最小的）键值对
        val firstPair = firstBuf.next()
        //检查刚刚取过元素的迭代器是否还有后续元素
        //如果有，则再次将其放回优先级队列。由于它已经消费了一个元素，其下一个元素可能会改变其在堆中的位置
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  /**
   * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
   * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
   * (e.g. when we sort objects by hash code and different keys may compare as equal although
   * they're not), we still merge them by doing equality tests for all keys that compare as equal.
   */
  //关键的归并和聚合操作，它负责将多个已排序的迭代器中的数据合并，并对相同的键值进行聚合。该方法会根据键的排序类型（全序或偏序）来选择不同的聚合逻辑
  private def mergeWithAggregation(
      iterators: Seq[Iterator[Product2[K, C]]],//一个包含 (K, C) 键值对的迭代器序列，每个迭代器都已按键排序。
      mergeCombiners: (C, C) => C, //一个聚合函数，用于合并相同键的两个值
      comparator: Comparator[K],
      totalOrder: Boolean) //指示 comparator 是否为全序（true）或偏序（false）
      : Iterator[Product2[K, C]] = {
    if (!totalOrder) {
      //当键的比较器是偏序时（例如，基于哈希码的比较），不同的键可能被比较为相等。为了正确聚合，需要额外处理
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
      val it = new Iterator[Iterator[Product2[K, C]]] {
        //调用 mergeSort 方法对所有输入迭代器进行多路归并排序。这里的排序是基于传入的偏序 comparator，
        // 这意味着所有被认为是“相等”的键都会被分组在一起。然后，将结果转换为一个缓冲迭代器（buffered），以便于预读下一个元素
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        //用于临时存储一组被偏序比较器判定为相等的键和值
        val keys = new ArrayBuffer[K]
        val combiners = new ArrayBuffer[C]

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          //从排序后的流中读取第一个元素，这是当前组的起始
          val firstPair = sorted.next()
          keys += firstPair._1
          combiners += firstPair._2
          //记住当前组的第一个键
          val key = firstPair._1
          // 这个循环会读取所有被偏序比较器判定为与 key 相等的元素
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            while (i < keys.size && !foundKey) {
              //真正的相等比较（==）**来判断当前读取的键是否已经存在于缓冲区中
              if (keys(i) == pair._1) {
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                //已经找到就不用追加到数组，第509行就不用执行
                foundKey = true
              }
              i += 1
            }
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          //返回键值对
          keys.iterator.zip(combiners.iterator)
        }
      }
      it.flatten
    } else {
      // We have a total ordering, so the objects with the same key are sequential.
      //全序排序就简单了
      new Iterator[Product2[K, C]] {
        val sorted = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }

  /**
   * An internal class for reading a spilled file partition by partition. Expects all the
   * partitions to be requested in order.
   */
  // 核心作用是以高效、可控的方式，逐个分区地读取溢写到磁盘的临时文件。
  // 在 ExternalSorter 的归并排序阶段，它为每个溢写文件创建一个 SpillReader 实例，然后通过这些实例，按顺序、按分区拉取数据，
  // 以便与其他溢写文件以及内存中的数据进行归并
  // 设计巧妙地利用了溢写文件在磁盘上按分区排序的特性，实现了流式读取，避免了一次性加载整个文件到内存中，从而大大节省了内存。
  // 它还能处理溢写文件中的分批（batch） 序列化数据，确保读取的正确性和效率
  private[this] class SpillReader(spill: SpilledFile) {
    // Serializer batch offsets; size will be batchSize.length + 1
    //批次偏移量数组。它通过对 spill.serializerBatchSizes 进行累加得到，用于快速定位每个序列化批次在文件中的起始位置
    val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    // Track which partition and which batch stream we're in. These will be the indices of
    // the next element we will read. We'll also store the last partition read so that
    // readNextPartition() can figure out what partition that was from.
    //当前正在读取的分区 ID
    var partitionId = 0
    //当前分区内已读取的元素数量
    var indexInPartition = 0L
    // 当前正在读取的批次 ID
    var batchId = 0
    // 当前批次内已读取的元素数量
    var indexInBatch = 0
    // 记录上一个读取元素的所属分区 ID，用于 readNextPartition() 方法的逻辑校验
    var lastPartitionId = 0

    skipToNextPartition()

    // Intermediate file and deserializer streams that read from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    //用于从文件中读取字节流的输入流
    var fileStream: FileInputStream = null
    //用于反序列化字节流为对象的输入流
    var deserializeStream = nextBatchStream()  // Also sets fileStream
    //预读取的下一个元素，用于实现 hasNext 方法的预检查
    var nextItem: (K, C) = null
    //标志位，表示是否已读取完整个文件
    var finished = false

    /** Construct a stream that only reads from the next batch */
    // 创建并返回一个只读取下一个序列化批次的 DeserializationStream
    def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      //首先关闭旧的流，
      if (batchId < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }
        //使用 batchOffsets 找到下一个批次的起始位置 start 和结束位置 end
        val start = batchOffsets(batchId)
        fileStream = new FileInputStream(spill.file)
        fileStream.getChannel.position(start)
        batchId += 1

        val end = batchOffsets(batchId)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        //创建 DeserializationStream 进行反序列化
        val wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream)
        serInstance.deserializeStream(wrappedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Update partitionId if we have reached the end of our current partition, possibly skipping
     * empty partitions on the way.
     */
    //更新 partitionId，跳过可能为空的分区，直到找到下一个有数据的分区
    private def skipToNextPartition(): Unit = {
      while (partitionId < numPartitions &&
          indexInPartition == spill.elementsPerPartition(partitionId)) {
        partitionId += 1
        indexInPartition = 0L
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream and update partitionId,
     * indexInPartition, indexInBatch and such to match its location.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    //从反序列化流中读取下一个键值对
    private def readNextItem(): (K, C) = {
      //如果已读完或流已关闭，则返回 null
      if (finished || deserializeStream == null) {
        return null
      }
      //从 deserializeStream 中读取键和值
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      lastPartitionId = partitionId
      // Start reading the next batch if we're done with this one
      indexInBatch += 1
      //如果当前批次已读完，则调用 nextBatchStream() 打开新的批次流
      if (indexInBatch == serializerBatchSize) {
        indexInBatch = 0
        deserializeStream = nextBatchStream()
      }
      // Update the partition location of the element we're reading
      indexInPartition += 1
      skipToNextPartition()
      // If we've finished reading the last partition, remember that we're done
      if (partitionId == numPartitions) {
        finished = true
        if (deserializeStream != null) {
          deserializeStream.close()
        }
      }
      (k, c)
    }

    var nextPartitionToRead = 0
    //返回一个只包含单个分区数据的迭代器
    //这是一个非常重要的设计。每次调用 readNextPartition() 都会返回一个新的迭代器，该迭代器只负责读取下一个分区的数据
    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      //记录当前迭代器负责的分区 ID
      val myPartition = nextPartitionToRead
      nextPartitionToRead += 1
      //检查是否有下一个元素。它会预读取一个元素到 nextItem，然后检查 lastPartitionId 是否与 myPartition 相等，以确保只返回属于当前分区的数据
      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        assert(lastPartitionId >= myPartition)
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

    // Clean up our open streams and put us in a state where we can't read any more data
    //关闭数据流
    def cleanup(): Unit = {
      batchId = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      if (ds != null) {
        ds.close()
      }
      // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
      // This should also be fixed in ExternalAppendOnlyMap.
    }
  }

  /**
   * Returns a destructive iterator for iterating over the entries of this map.
   * If this iterator is forced spill to disk to release memory when there is not enough memory,
   * it returns pairs from an on-disk map.
   */
  // 方法签名定义了它接收一个内存迭代器，并返回一个可能具有不同行为的迭代器
  def destructiveIterator(memoryIterator: Iterator[((Int, K), C)]): Iterator[((Int, K), C)] = {
    if (isShuffleSort) {
      memoryIterator
    } else {
      readingIterator = new SpillableIterator(memoryIterator)
      readingIterator
    }
  }

  /**
   * Return an iterator over all the data written to this object, grouped by partition and
   * aggregated by the requested aggregator. For each partition we then have an iterator over its
   * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
   * partition without reading the previous one). Guaranteed to return a key-value pair for each
   * partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   * Exposed for testing.
   */
  // 用于遍历所有已经写入的数据
  // 这些数据首先按分区（partition）分组，然后根据请求的聚合器进行聚合
  // 方法签名定义了该方法返回一个迭代器
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    //spills 列表存储了所有因为内存不足而被溢写（spill）到磁盘的文件
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      // 如果为空，表示用户只要求按分区 ID 排序，不要求按键排序
      if (ordering.isEmpty) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(None)))
      } else {
        // We do need to sort by both partition ID and key
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      // Merge spilled and in-memory data
      merge(spills.toSeq, destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }

  /**
   * Return an iterator over all the data written to this object, aggregated by our aggregator.
   */
  def iterator: Iterator[Product2[K, C]] = {
    isShuffleSort = false
    partitionedIterator.flatMap(pair => pair._2)
  }

  /**
   * Insert all records, updates related task metrics, and return a completion iterator
   * over all the data written to this object, aggregated by our aggregator.
   * On task completion (success, failure, or cancellation), it releases resources by
   * calling `stop()`.
   */
  def insertAllAndUpdateMetrics(records: Iterator[Product2[K, V]]): Iterator[Product2[K, C]] = {
    insertAll(records)
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)
    // Use completion callback to stop sorter if task was finished/cancelled.
    context.addTaskCompletionListener[Unit](_ => stop())
    CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](iterator, stop())
  }

  /**
   * Write all the data added into this ExternalSorter into a map output writer that pushes bytes
   * to some arbitrary backing store. This is called by the SortShuffleWriter.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
  // 负责将排序和聚合后的所有数据写入到 Shuffle 的 Map 输出文件中。
  // 这个方法根据数据是否溢写到磁盘，采用了两种不同的处理逻辑
  def writePartitionedMapOutput(
      shuffleId: Int,//Shuffle 操作的 ID
      mapId: Long,//Map 任务的 ID
      mapOutputWriter: ShuffleMapOutputWriter, //一个用于写入 Shuffle 输出的抽象类，它可以将数据写入磁盘或内存
      writeMetrics: ShuffleWriteMetricsReporter): Unit = { //用于记录写入相关的指标，如写入的字节数
    if (spills.isEmpty) {
      // 无溢写文件的情况
      // Case where we only have in-memory data
      // 根据是否设置了聚合器，选择使用 map 或 buffer 作为数据源
      val collection = if (aggregator.isDefined) map else buffer
      // 从内存集合中获取一个可写入的、按分区和键排序的、破坏性的迭代器
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      //循环遍历迭代器中的所有数据
      while (it.hasNext) {
        val partitionId = it.nextPartition()
        var partitionWriter: ShufflePartitionWriter = null
        var partitionPairsWriter: ShufflePartitionPairsWriter = null
        TryUtils.tryWithSafeFinally {
          //获取一个分区写入器
          partitionWriter = mapOutputWriter.getPartitionWriter(partitionId)
          // 为当前分区创建一个唯一的 Shuffle 块 ID
          val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
          // 创建一个键值对写入器
          partitionPairsWriter = new ShufflePartitionPairsWriter(
            partitionWriter,
            serializerManager,
            serInstance,
            blockId,
            writeMetrics,
            if (partitionChecksums.nonEmpty) partitionChecksums(partitionId) else null)
          while (it.hasNext && it.nextPartition() == partitionId) {
            it.writeNext(partitionPairsWriter)
          }
        } {
          if (partitionPairsWriter != null) {
            partitionPairsWriter.close()
          }
        }
      }
    } else {
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      //这种情况表示数据量太大，一部分数据已溢写到磁盘
      for ((id, elements) <- this.partitionedIterator) {
        val blockId = ShuffleBlockId(shuffleId, mapId, id)
        var partitionWriter: ShufflePartitionWriter = null
        var partitionPairsWriter: ShufflePartitionPairsWriter = null
        TryUtils.tryWithSafeFinally {
          partitionWriter = mapOutputWriter.getPartitionWriter(id)
          partitionPairsWriter = new ShufflePartitionPairsWriter(
            partitionWriter,
            serializerManager,
            serInstance,
            blockId,
            writeMetrics,
            if (partitionChecksums.nonEmpty) partitionChecksums(id) else null)
          if (elements.hasNext) {
            for (elem <- elements) {
              partitionPairsWriter.write(elem._1, elem._2)
            }
          }
        } {
          if (partitionPairsWriter != null) {
            partitionPairsWriter.close()
          }
        }
      }
    }

    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)
  }

  def stop(): Unit = {
    spills.foreach(s => s.file.delete())
    spills.clear()
    forceSpillFiles.foreach(s => s.file.delete())
    forceSpillFiles.clear()
    if (map != null || buffer != null || readingIterator != null) {
      map = null // So that the memory can be garbage-collected
      buffer = null // So that the memory can be garbage-collected
      readingIterator = null // So that the memory can be garbage-collected
      releaseMemory()
    }
  }

  /**
   * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
   * group together the pairs for each partition into a sub-iterator.
   *
   * @param data an iterator of elements, assumed to already be sorted by partition ID
   */
  // 核心作用是将一个已经按分区 ID 排序的键值对流（((Int, K), C)）转换成一个按分区 ID 分组的迭代器
  private def groupByPartition(data: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
   * An iterator that reads only the elements for a given partition ID from an underlying buffered
   * stream, assuming this partition is the next one to be read. Used to make it easier to return
   * partitioned iterators from our in-memory collection.
   */
  //由于data是排序的，所以直接循环的到分区等于partitionId的元素就是下一个要读取的分区
  //从一个全局有序的迭代器（BufferedIterator[((Int, K), C)]）中，按分区 partitionId 提供数据的迭代器视图
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }
  // 主要作用是包装一个内存中的迭代器，并在需要时将它所指向的数据溢写到磁盘，然后无缝地切换到从磁盘读取数据
  // 就像一个可以"动态换源"的迭代器。它一开始从内存中读取数据，当 Spark 运行时检测到内存压力过大（例如，通过 ExternalSorter 的内存管理器），SpillableIterator 就会被通知进行溢写操作。
  // 在溢写完成后，它会停止从内存读取，转而从刚刚写入的磁盘文件中继续读取数据。这种设计是 Spark 处理大数据集而不会耗尽内存的关键所在
  // upstream: 这是一个引用，一开始指向内存中的数据迭代器。当溢写发生后，这个引用会被更新为指向从磁盘文件中读取的迭代器
  private[this] class SpillableIterator(var upstream: Iterator[((Int, K), C)])
    extends Iterator[((Int, K), C)] {
    //一个私有的同步锁对象
    private val SPILL_LOCK = new Object()
    //用于保存从磁盘读取数据的迭代器
    private var nextUpstream: Iterator[((Int, K), C)] = null
    //存储下一个要返回的元素
    private var cur: ((Int, K), C) = readNext()
    //指示是否已经发生过溢写
    private var hasSpilled: Boolean = false
    //执行内存溢写操作
    def spill(): Boolean = SPILL_LOCK.synchronized {
      //检查 hasSpilled。如果已经溢写过，直接返回 false
      if (hasSpilled) {
        false
      } else {
        val inMemoryIterator = new WritablePartitionedIterator[K, C](upstream)
        logInfo(s"Task ${TaskContext.get().taskAttemptId} force spilling in-memory map to disk " +
          s"and it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
        //调用 spillMemoryIteratorToDisk() 方法将数据写入磁盘
        val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
        forceSpillFiles += spillFile
        val spillReader = new SpillReader(spillFile)
        //磁盘到内存: 创建一个 SpillReader 来读取刚刚溢写的文件，并创建一个新的迭代器 nextUpstream，将数据从磁盘文件加载回来
        nextUpstream = (0 until numPartitions).iterator.flatMap { p =>
          val iterator = spillReader.readNextPartition()
          iterator.map(cur => ((p, cur._1), cur._2))
        }
        hasSpilled = true
        true
      }
    }
    //从当前迭代器（无论是内存还是磁盘）中读取下一个元素
    def readNext(): ((Int, K), C) = SPILL_LOCK.synchronized {
      if (nextUpstream != null) {
        upstream = nextUpstream
        nextUpstream = null
      }
      if (upstream.hasNext) {
        upstream.next()
      } else {
        null
      }
    }

    override def hasNext(): Boolean = cur != null

    override def next(): ((Int, K), C) = {
      val r = cur
      cur = readNext()
      r
    }
  }
}
