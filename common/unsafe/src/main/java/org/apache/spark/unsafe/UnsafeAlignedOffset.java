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

package org.apache.spark.unsafe;

/**
 * Class to make changes to record length offsets uniform through out
 * various areas of Apache Spark core and unsafe.  The SPARC platform
 * requires this because using a 4 byte Int for record lengths causes
 * the entire record of 8 byte Items to become misaligned by 4 bytes.
 * Using a 8 byte long for record length keeps things 8 byte aligned.
 */
// Spark 内部 Unsafe 内存操作 模块中的一个工具类，
// 其核心作用是提供一个统一的、跨平台兼容的机制来读取和写入存储在内存中的记录长度（Record Length） 信息
//在 Spark 的 Unsafe 内存模型中，数据通常被组织成 8 字节（long）的倍数以实现内存对齐，从而提高访问速度
//对于大多数平台，记录长度可以使用一个 4 字节的整数 (int) 来存储
//然而，SPARC 等特定硬件平台在处理 8 字节数据项（例如 long 或 double）时，如果其记录长度字段只占 4 字节，会导致后续的 8 字节数据项发生 4 字节的错位（Misalignment）。这种错位会显著降低性能，甚至可能导致硬件错误
public class UnsafeAlignedOffset {
  //它根据底层平台是否需要严格对齐（Platform.unaligned()），动态地选择记录长度字段的大小
  //需要对齐的平台（如 SPARC）： 使用 8 字节 (long) 来存储记录长度，即使实际长度值可以用 4 字节表示。这样做是为了保持整个记录的 8 字节对齐
  //不需要对齐的平台（大多数 x86-64）： 使用标准的 4 字节 (int) 来存储记录长度，以节省空间
  private static final int UAO_SIZE = Platform.unaligned() ? 4 : 8;

  private static int TEST_UAO_SIZE = 0;

  // used for test only
  public static void setUaoSize(int size) {
    assert size == 0 || size == 4 || size == 8;
    TEST_UAO_SIZE = size;
  }
  //返回当前用于记录长度的字节大小
  public static int getUaoSize() {
    return TEST_UAO_SIZE == 0 ? UAO_SIZE : TEST_UAO_SIZE;
  }
  //从指定内存地址读取记录长度值
  public static int getSize(Object object, long offset) {
    //获取长度字段大小
    switch (getUaoSize()) {
      case 4:
        return Platform.getInt(object, offset);
      case 8:
        return (int)Platform.getLong(object, offset);
      default:
        // checkstyle.off: RegexpSinglelineJava
        throw new AssertionError("Illegal UAO_SIZE");
        // checkstyle.on: RegexpSinglelineJava
    }
  }

  public static void putSize(Object object, long offset, int value) {
    switch (getUaoSize()) {
      case 4:
        Platform.putInt(object, offset, value);
        break;
      case 8:
        Platform.putLong(object, offset, value);
        break;
      default:
        // checkstyle.off: RegexpSinglelineJava
        throw new AssertionError("Illegal UAO_SIZE");
        // checkstyle.on: RegexpSinglelineJava
    }
  }
}
