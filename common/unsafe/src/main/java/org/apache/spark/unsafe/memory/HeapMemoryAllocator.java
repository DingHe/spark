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

package org.apache.spark.unsafe.memory;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 */
//主要作用是在 JVM 堆内分配和管理内存
//通过创建**long 数组来表示内存块，并利用内存池（Buffer Pooling）** 机制来优化内存分配性能
//高效分配：通过复用之前分配但已释放的 long 数组，减少 JVM 垃圾回收的压力和内存分配的开销
//统一表示：将 JVM 堆内的 long 数组封装成 MemoryBlock 对象，使得上层应用代码（如 Tungsten 执行引擎）可以使用统一的 MemoryBlock API 来操作内存，而无需关心其是在堆内还是堆外
//简化内存管理：对于堆内内存，该分配器并不真正“释放”内存，而是将已使用的数组放回内存池中，等待后续被复用或由 JVM 垃圾回收器回收
public class HeapMemoryAllocator implements MemoryAllocator {
  //私有的内存池，用于存储已释放但可复用的 long 数组
  //键 (Long): 内存块的对齐后的大小（以字节为单位
  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<long[]>>> bufferPoolsBySize = new HashMap<>();
  //定义了内存块进入内存池的大小阈值。其值为 1MB
  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   */
  //判断给定大小的内存块是否应该走内存池机制
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    return size >= POOLING_THRESHOLD_BYTES;
  }
  //用于分配指定大小的内存
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    //对齐计算: 将请求的 size 对齐到 8 字节（long 的大小），以确保可以正确地用 long[] 存储
    int numWords = (int) ((size + 7) / 8);
    long alignedSize = numWords * 8L;
    assert (alignedSize >= size);
    if (shouldPool(alignedSize)) {
      synchronized (this) {
        final LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<long[]> arrayReference = pool.pop();
            final long[] array = arrayReference.get();
            //如果弱引用指向的数组没有被垃圾回收，则将其封装成 MemoryBlock 并返回
            if (array != null) {
              assert (array.length * 8L >= size);
              MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
              if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
                memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
              }
              //返回内存块
              return memory;
            }
          }
          //移除弱饮用
          bufferPoolsBySize.remove(alignedSize);
        }
      }
    }
    //如果内存池中没有可用的数组，或者请求的大小不满足入池条件，它会直接创建一个新的 long[] 数组
    long[] array = new long[numWords];
    MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }
  //释放内存块
  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj != null) :
      "baseObject was null; are you trying to use the on-heap allocator to free off-heap memory?";
    assert (memory.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "page has already been freed";
    assert ((memory.pageNumber == MemoryBlock.NO_PAGE_NUMBER)
            || (memory.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
      "TMM-allocated pages must first be freed via TMM.freePage(), not directly in allocator " +
        "free()";

    final long size = memory.size();
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }

    // Mark the page as freed (so we can detect double-frees).
    //将 memory.pageNumber 设置为 FREED_IN_ALLOCATOR_PAGE_NUMBER，表示该内存块已被分配器释放
    memory.pageNumber = MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER;

    // As an additional layer of defense against use-after-free bugs, we mutate the
    // MemoryBlock to null out its reference to the long[] array.
    long[] array = (long[]) memory.obj;
    //将 MemoryBlock 的基对象引用设置为 null，防止“使用已释放内存”的错误（use-after-free bug）
    memory.setObjAndOffset(null, 0);

    long alignedSize = ((size + 7) / 8) * 8;
    //如果内存块大小满足入池条件，它会进入同步块，将该 long[] 数组的弱引用添加到对应的内存池链表中，等待后续被复用
    if (shouldPool(alignedSize)) {
      synchronized (this) {
        LinkedList<WeakReference<long[]>> pool =
          bufferPoolsBySize.computeIfAbsent(alignedSize, k -> new LinkedList<>());
        pool.add(new WeakReference<>(array));
      }
    }
  }
}
