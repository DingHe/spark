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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}

/** 当内存中的集合数据大小超过设定阈值时，会自动将数据写入磁盘，释放内存，防止内存溢出
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 */
private[spark] abstract class Spillable[C](taskMemoryManager: TaskMemoryManager) //管理任务内存的 TaskMemoryManager 对象
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) with Logging { //是 Spillable 的父类，MemoryMode.ON_HEAP 表示内存使用是在堆内存中
  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   *
   * @param collection collection to spill to disk
   */
  protected def spill(collection: C): Unit  //要求子类实现该方法，用于将当前内存中的集合数据写入磁盘

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  protected def forceSpill(): Boolean  //用于强制将当前内存中的集合数据溢写到磁盘，通常在内存不足时调用

  // Number of elements read from input since last spill
  protected def elementsRead: Int = _elementsRead  //获取自上次溢写以来，已读取的元素数量

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // Initial threshold for the size of a collection before we start tracking its memory usage
  // For testing only
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.get(SHUFFLE_SPILL_INITIAL_MEM_THRESHOLD)  //初始化内存阈值

  // Force this collection to spill when there are this many elements in memory
  // For testing only
  private[this] val numElementsForceSpillThreshold: Int =
    SparkEnv.get.conf.get(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD)  //强制溢写的元素数量阈值

  // Threshold for this collection's size in bytes before we start tracking its memory usage
  // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
  @volatile private[this] var myMemoryThreshold = initialMemoryThreshold  //当前的内存阈值，标记集合数据在内存中允许的最大大小

  // Number of elements read from input since last spill
  private[this] var _elementsRead = 0  //自上次溢写以来，已读取的元素数量

  // Number of bytes spilled in total
  @volatile private[this] var _memoryBytesSpilled = 0L  //已经溢写的内存字节数

  // Number of spills
  private[this] var _spillCount = 0  //溢写的次数

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    //// 每32个元素检查一次内存是否超过阈值
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      //// 如果内存达到阈值，尝试从 Shuffle 内存池中申请更多的内存
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = acquireMemory(amountToRequest)
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      //如果申请到的内存太少（或者本来内存已经超过阈值），就决定溢写数据
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    //// 如果决定需要溢写，执行溢写操作
    if (shouldSpill) {
      _spillCount += 1
      logSpillage(currentMemory)
      spill(collection)   // 执行溢写操作
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory
      releaseMemory()
    }
    shouldSpill
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   */
  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this && taskMemoryManager.getTungstenMemoryMode == MemoryMode.ON_HEAP) {
      val isSpilled = forceSpill()
      if (!isSpilled) {
        0L
      } else {
        val freeMemory = myMemoryThreshold - initialMemoryThreshold
        _memoryBytesSpilled += freeMemory
        releaseMemory()
        freeMemory
      }
    } else {
      0L
    }
  }

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the execution pool so that other tasks can grab it.
   */
  def releaseMemory(): Unit = {
    freeMemory(myMemoryThreshold - initialMemoryThreshold)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * Prints a standard log message detailing spillage.
   * 打印溢写的信息
   * @param size number of bytes spilled
   */
  @inline private def logSpillage(size: Long): Unit = {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
