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

package org.apache.spark.util.collection.unsafe.sort;

import java.io.IOException;
//用于 Spark 中的排序操作
public abstract class UnsafeSorterIterator {

  public abstract boolean hasNext(); //判断是否还有更多的记录可以访问
  //当迭代器正在遍历一系列排序记录时，如果要访问下一个记录，调用此方法。通常它会将指针移动到下一条记录的位置，并从底层存储中读取该记录的数据
  public abstract void loadNext() throws IOException;
  //返回当前记录所存储的底层内存对象
  public abstract Object getBaseObject();
  //返回当前记录在底层内存中的偏移量
  public abstract long getBaseOffset();
  //返回当前记录的长度（字节数）
  public abstract int getRecordLength();
  //键前缀是排序的关键部分。在某些排序算法中，前缀可以用来优化比较操作，尤其是在多字段排序的情况下
  public abstract long getKeyPrefix();
  //当前页中的记录数
  public abstract int getNumRecords();
 //返回当前页的页号或页面编号
  public abstract long getCurrentPageNumber();
}
