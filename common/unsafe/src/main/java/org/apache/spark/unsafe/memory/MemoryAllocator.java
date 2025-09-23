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
//Spark Tungsten 内存管理中的一个核心接口，它定义了内存分配器的通用行为。它的作用是为 Spark 应用程序提供一种抽象的、可插拔的内存分配机制
//接口将内存的分配（allocate） 和释放（free） 逻辑与具体的实现方式解耦。这意味着 Spark 的上层代码（如 TaskMemoryManager）不需要关心内存是从 JVM 堆内（On-Heap）还是从堆外（Off-Heap）分配的，只需通过这个统一的接口来请求内存即可
public interface MemoryAllocator {

  /**
   * Whether to fill newly allocated and deallocated memory with 0xa5 and 0x5a bytes respectively.
   * This helps catch misuse of uninitialized or freed memory, but imposes some overhead.
   */
  //用于控制是否启用内存调试填充功能
  boolean MEMORY_DEBUG_FILL_ENABLED = Boolean.parseBoolean(
    System.getProperty("spark.memory.debugFill", "false"));

  // Same as jemalloc's debug fill values.
  //调试填充时使用的字节值，用于填充新分配的内存
  byte MEMORY_DEBUG_FILL_CLEAN_VALUE = (byte)0xa5;
  //调试填充时使用的字节值，用于填充已释放的内存
  byte MEMORY_DEBUG_FILL_FREED_VALUE = (byte)0x5a;

  /**
   * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
   * to be zeroed out (call `fill(0)` on the result if this is necessary).
   */
  //用于分配一块指定大小的连续内存块
  MemoryBlock allocate(long size) throws OutOfMemoryError;
  //用于释放一个之前由 allocate 方法分配的内存块
  void free(MemoryBlock memory);

  //静态的 MemoryAllocator 实例，代表堆外内存分配器
  MemoryAllocator UNSAFE = new UnsafeMemoryAllocator();

  //静态的 MemoryAllocator 实例，代表堆内内存分配器
  //负责在 JVM 堆上分配 byte[] 数组
  MemoryAllocator HEAP = new HeapMemoryAllocator();
}
