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

package org.apache.spark.sql.execution;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;

import java.nio.ByteOrder;
//用于比较两段二进制数据的大小。该类的核心功能是通过字节级别的比较来确定两个二进制数据是否相等或哪个更大
public final class RecordBinaryComparator extends RecordComparator {
  //这个属性检查当前平台是否支持非对齐访问。非对齐访问指的是数据的读取不要求数据地址是对齐的（比如8字节对齐）。Platform.unaligned()返回true表示支持非对齐访问，返回false表示不支持
  private static final boolean UNALIGNED = Platform.unaligned();
  private static final boolean LITTLE_ENDIAN =
      ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN); //这个属性判断当前平台的字节序是否是小端字节序
  //接受两个二进制数据（leftObj和rightObj），每个数据的起始偏移量（leftOff和rightOff），以及它们的长度（leftLen和rightLen）
  @Override
  public int compare(
      Object leftObj, long leftOff, int leftLen, Object rightObj, long rightOff, int rightLen) {
    int i = 0;
    //如果两个数组的长度不一样，返回它们的长度差。较长的数组被认为更大
    // If the arrays have different length, the longer one is larger.
    if (leftLen != rightLen) {
      return leftLen - rightLen;
    }

    // The following logic uses `leftLen` as the length for both `leftObj` and `rightObj`, since
    // we have guaranteed `leftLen` == `rightLen`.
    //如果当前平台不支持非对齐访问，并且两个数据的起始位置（leftOff和rightOff）在8字节边界上对齐，则进入对齐比较的分支,leftOff % 8 == rightOff % 8检查两个偏移量是否是8字节对齐的
    // check if stars align and we can get both offsets to be aligned
    if (!UNALIGNED && ((leftOff % 8) == (rightOff % 8))) {
      while ((leftOff + i) % 8 != 0 && i < leftLen) {
        final int v1 = Platform.getByte(leftObj, leftOff + i); //逐字节比较两个数据的内容，直到8字节对齐
        final int v2 = Platform.getByte(rightObj, rightOff + i);
        if (v1 != v2) {
          return (v1 & 0xff) > (v2 & 0xff) ? 1 : -1;
        }
        i += 1;
      }
    }  //如果支持非对齐访问或两个偏移量都已经是8字节对齐的，程序将每次比较8字节（long类型）的数据
    // for architectures that support unaligned accesses, chew it up 8 bytes at a time
    if (UNALIGNED || (((leftOff + i) % 8 == 0) && ((rightOff + i) % 8 == 0))) {
      while (i <= leftLen - 8) {
        long v1 = Platform.getLong(leftObj, leftOff + i); //通过Platform.getLong()方法获取8字节数据并进行比较
        long v2 = Platform.getLong(rightObj, rightOff + i);
        if (v1 != v2) {
          if (LITTLE_ENDIAN) {
            // if read as little-endian, we have to reverse bytes so that the long comparison result
            // is equivalent to byte-by-byte comparison result.
            // See discussion in https://github.com/apache/spark/pull/26548#issuecomment-554645859
            v1 = Long.reverseBytes(v1);
            v2 = Long.reverseBytes(v2);
          }
          return Long.compareUnsigned(v1, v2);
        }
        i += 8;
      }
    } //如果还有剩余的字节（即不能按8字节处理的部分），就逐字节比较，直到所有字节都比较完
    // this will finish off the unaligned comparisons, or do the entire aligned comparison
    // whichever is needed.
    while (i < leftLen) {
      final int v1 = Platform.getByte(leftObj, leftOff + i);
      final int v2 = Platform.getByte(rightObj, rightOff + i);
      if (v1 != v2) {
        return (v1 & 0xff) > (v2 & 0xff) ? 1 : -1;
      }
      i += 1;
    }

    // The two arrays are equal.
    return 0;
  }
}
