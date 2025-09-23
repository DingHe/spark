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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * Implements policies and bookkeeping for sharing an adjustable-sized pool of memory between tasks.
 *
 * Tries to ensure that each task gets a reasonable share of memory, instead of some task ramping up
 * to a large amount first and then causing others to spill to disk repeatedly.
 *
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever this
 * set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
// Spark 内存管理中专门用于执行（Execution） 目的的内存池。它的主要作用是为正在运行的任务（Task） 分配和管理内存
//内存主要用于以下场景：
//Shuffle 操作：如 ExternalSorter 和 UnsafeShuffleWriter，用于在 Shuffle 过程中对数据进行排序、聚合和溢写
//设计目标是实现一个公平的内存分配策略。它确保每个任务都能够获得“合理”的内存份额，避免某个任务占用过多内存而导致其他任务频繁溢写到磁盘，从而影响整体性能
//动态份额分配：根据当前活跃任务的数量（N）动态计算每个任务的最小和最大内存份额
//阻塞与唤醒：当某个任务无法获得其最小内存份额时，它会阻塞等待，直到其他任务释放内存
//内存借用：它能从 StorageMemoryPool 借用空闲内存，并在需要时归还
private[memory] class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode //主要堆内和堆外内存
  ) extends MemoryPool(lock) with Logging {
  //内存池的名称（例如 "on-heap execution" 或 "off-heap execution"），用于日志记录和调试
  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap execution"
    case MemoryMode.OFF_HEAP => "off-heap execution"
  }

  /** 用于记录每个任务已分配的内存量
   * Map from taskAttemptId -> memory consumption in bytes
   */
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()
  //返回当前池子中所有任务已使用的总内存量
  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum
  }

  /** 返回指定任务已使用的内存量
   * Returns the memory consumption, in bytes, for the given task.
   */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  /**
   * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
   * obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * @param numBytes number of bytes to acquire
   * @param taskAttemptId the task attempt acquiring memory
   * @param maybeGrowPool a callback that potentially grows the size of this pool. It takes in
   *                      one parameter (Long) that represents the desired amount of memory by
   *                      which this pool should be expanded.
   * @param computeMaxPoolSize a callback that returns the maximum allowable size of this pool
   *                           at this given moment. This is not a field because the max pool
   *                           size is variable in certain cases. For instance, in unified
   *                           memory management, the execution pool can be expanded by evicting
   *                           cached blocks, thereby shrinking the storage pool.
   *
   * @return the number of bytes granted to the task.
   */
  // 核心的内存分配方法，它尝试为指定任务申请内存，并可能阻塞等待
  //
  private[memory] def acquireMemory(
      numBytes: Long, //请求的内存量
      taskAttemptId: Long, //请求内存的任务ID
      //回调函数，用于在内存不足时尝试扩大内存池（例如从 StorageMemoryPool 借用内存）
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => (),
      //一个回调函数，用于计算当前内存池的最大可用容量
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // TODO: clean up this clunky method signature
    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    //如果任务ID尚未在 memoryForTask 中，将其添加进去并通知所有等待的线程
    if (!memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      lock.notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // 在每次循环中，根据当前活跃任务的数量 (numActiveTasks) 和最大池子大小 (maxPoolSize)，
    // 计算每个任务的最小 (minMemoryPerTask) 和最大 (maxMemoryPerTask) 内存份额
    // 对于当前 N 个 active task，必须保证在每个 task 溢出之前至少获得 1 / 2N，最多 1 / N 的内存。
    // 由于 N 是动态变化的，所以要持续跟踪 active task 的状态，随时重新分配内存，所以使用 while 循环。
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      val numActiveTasks = memoryForTask.keys.size
      val curMem = memoryForTask(taskAttemptId)

      // In every iteration of this loop, we should first try to reclaim any borrowed execution
      // space from storage. This is necessary because of the potential race condition where new
      // storage blocks may steal the free execution memory that this task was waiting for.
      maybeGrowPool(numBytes - memoryFree)

      // Maximum size the pool would have after potentially growing the pool.
      // This is used to compute the upper bound of how much memory each task can occupy. This
      // must take into account potential free memory as well as the amount this pool currently
      // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
      // we did not take into account space that could have been freed by evicting cached blocks.
      val maxPoolSize = computeMaxPoolSize()
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)

      // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
      //计算可授予的最大内存
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      //授予该任务的内存量 (toGrant)，它不能超过请求量，也不能超过最大份额，更不能超过当前空闲内存
      val toGrant = math.min(maxToGrant, memoryFree)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)
      //如果授予的内存量小于请求量，并且授予后该任务的总内存仍然小于其最小份额 (minMemoryPerTask)，则表示它没有获得应有的份额。
      // 此时，该任务会调用 lock.wait() 进入阻塞状态，等待其他任务释放内存
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        lock.wait()
      } else {
        memoryForTask(taskAttemptId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

  /**
   * Release `numBytes` of memory acquired by the given task.
   */
  //释放指定任务占用的内存
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    val memoryToFree = if (curMem < numBytes) {
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      curMem
    } else {
      numBytes
    }
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  //释放指定任务所有的内存
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId)
    numBytesToFree
  }

}
