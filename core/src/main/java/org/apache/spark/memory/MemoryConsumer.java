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

package org.apache.spark.memory;

import java.io.IOException;

import org.apache.spark.errors.SparkCoreErrors;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * A memory consumer of {@link TaskMemoryManager} that supports spilling.
 *
 * Note: this only supports allocation / spilling of Tungsten memory.
 */
//Spark Tungsten 内存管理中的一个抽象基类。它的主要作用是作为内存的使用者（Consumer），与 TaskMemoryManager 协同工作
//这个类的核心设计思想是可溢写（spillable）。任何需要大量内存且可以在内存不足时将数据溢写到磁盘的组件（例如 ExternalSorter、UnsafeFixedWidthAggregationMap 等）都必须继承 MemoryConsumer
public abstract class MemoryConsumer {
  //引用了负责当前任务内存管理的 TaskMemoryManager 实例
  protected final TaskMemoryManager taskMemoryManager;
  private final long pageSize; //定义了内存分配的页（page） 大小，Spark 会以这个粒度来分配内存，以减少内存碎片。它通常在构造函数中初始化
  private final MemoryMode mode; //堆内存和非堆内存
  protected long used; //记录当前已使用的内存大小

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    this.taskMemoryManager = taskMemoryManager;
    this.pageSize = pageSize;
    this.mode = mode;
  }

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, MemoryMode mode) {
    this(taskMemoryManager, taskMemoryManager.pageSizeBytes(), mode);
  }

  /**
   * Returns the memory mode, {@link MemoryMode#ON_HEAP} or {@link MemoryMode#OFF_HEAP}.
   */
  public MemoryMode getMode() {
    return mode;
  }

  /**
   * Returns the size of used memory in bytes.
   */
  public long getUsed() {
    return used;
  }

  /** 强制触发内存溢写操作（将内存数据写入磁盘），通常在内存不足时触发。此方法调用 spill(Long.MAX_VALUE, this)，意味着尽可能释放所有内存
   * Force spill during building.
   */
  public void spill() throws IOException {
    spill(Long.MAX_VALUE, this);
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   *
   * This should be implemented by subclass.
   *
   * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
   * 需要注意的是：在 spill() 中不应该调用 acquireMemory()，以避免死锁
   * Note: today, this only frees Tungsten-managed pages.
   *
   * @param size the amount of memory should be released
   * @param trigger the MemoryConsumer that trigger this spilling
   * @return the amount of released memory in bytes
   */
  public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

  /**
   * Allocates a LongArray of `size`. Note that this method may throw `SparkOutOfMemoryError`
   * if Spark doesn't have enough memory for this allocation, or throw `TooLargePageException`
   * if this `LongArray` is too large to fit in a single page. The caller side should take care of
   * these two exceptions, or make sure the `size` is small enough that won't trigger exceptions.
   * 分配一个 LongArray 数组，大小为 size。每个元素占用 8 字节，因此总内存要求为 size * 8 字节。该方法调用 taskMemoryManager.allocatePage 来分配内存，如果内存不足或页面太小，会抛出 SparkOutOfMemoryError 异常
   * @throws SparkOutOfMemoryError
   * @throws TooLargePageException
   */
  public LongArray allocateArray(long size) {
    long required = size * 8L;
    MemoryBlock page = taskMemoryManager.allocatePage(required, this);
    if (page == null || page.size() < required) {
      throwOom(page, required);
    }
    used += required;
    return new LongArray(page);
  }

  /** 释放之前分配的 LongArray 数组
   * Frees a LongArray.
   */
  public void freeArray(LongArray array) {
    freePage(array.memoryBlock());
  }

  /**
   * Allocate a memory block with at least `required` bytes.
   * 分配至少 required 字节的内存页。如果当前页面不能满足要求，抛出内存溢出错误。成功分配后更新已使用内存
   * @throws SparkOutOfMemoryError
   */
  protected MemoryBlock allocatePage(long required) {
    MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      throwOom(page, required);
    }
    used += page.size();
    return page;
  }

  /** 释放一个内存块，并更新已使用的内存
   * Free a memory block.
   */
  protected void freePage(MemoryBlock page) {
    used -= page.size();
    taskMemoryManager.freePage(page, this);
  }

  /** 请求分配指定大小的内存。如果系统有足够的内存，将返回分配的内存大小，并更新已使用的内存
   * Allocates memory of `size`.
   */
  public long acquireMemory(long size) {
    long granted = taskMemoryManager.acquireExecutionMemory(size, this);
    used += granted;
    return granted;
  }

  /** 释放指定大小的内存，并更新已使用的内存
   * Release N bytes of memory.
   */
  public void freeMemory(long size) {
    taskMemoryManager.releaseExecutionMemory(size, this);
    used -= size;
  }
  //在内存不足的情况下，抛出 OutOfMemoryError 异常
  private void throwOom(final MemoryBlock page, final long required) {
    long got = 0;
    if (page != null) {
      got = page.size();
      taskMemoryManager.freePage(page, this);
    }
    taskMemoryManager.showMemoryUsage();
    throw SparkCoreErrors.outOfMemoryError(required, got);
  }
}
