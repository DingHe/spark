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

import javax.annotation.Nullable;

/**
 * A memory location. Tracked either by a memory address (with off-heap allocation),
 * or by an offset from a JVM object (on-heap allocation).
 */
//用于封装和表示一块内存的物理位置
  //能以统一的方式表示两种不同类型的内存：
  //堆内内存 (On-Heap)：JVM 堆中的内存
  //堆外内存 (Off-Heap)：直接从操作系统分配的、不受 JVM 管理的内存
public class MemoryLocation {

  //表示内存的基对象（base object）
  //如果 MemoryLocation 指向的是堆内内存（ON_HEAP），obj 就是一个 Java 数组对象（例如 byte[]），这块内存就在这个数组内部
  //如果 MemoryLocation 指向的是堆外内存（OFF_HEAP），obj 则为 null，因为堆外内存不属于任何 Java 对象
  @Nullable
  Object obj;

  //表示内存的偏移量（offset）
  //如果是堆内内存，offset 表示相对于 obj 数组起始地址的偏移量，用以定位具体的内存块
  //如果是堆外内存，offset 则直接存储了内存块的起始地址（一个 long 类型的内存指针）
  long offset;

  public MemoryLocation(@Nullable Object obj, long offset) {
    this.obj = obj;
    this.offset = offset;
  }

  public MemoryLocation() {
    this(null, 0);
  }

  public void setObjAndOffset(Object newObj, long newOffset) {
    this.obj = newObj;
    this.offset = newOffset;
  }

  public final Object getBaseObject() {
    return obj;
  }

  public final long getBaseOffset() {
    return offset;
  }
}
