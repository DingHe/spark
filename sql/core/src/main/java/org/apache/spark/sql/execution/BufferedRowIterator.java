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

import java.io.IOException;
import java.util.LinkedList;

import scala.collection.Iterator;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 * An iterator interface used to pull the output from generated function for multiple operators
 * (whole stage codegen).
 */
//主要作用是从多个算子（whole stage codegen）生成的代码中拉取输出数据。它的设计允许处理和缓存从多个数据源或操作中返回的行
public abstract class BufferedRowIterator {
  protected LinkedList<InternalRow> currentRows = new LinkedList<>(); //用于存储当前迭代器处理的行
  // used when there is no column in output
  protected UnsafeRow unsafeRow = new UnsafeRow(0); //当输出没有列时使用它
  private long startTimeNs = System.nanoTime();

  protected int partitionIndex = -1; //当前处理的数据分区的索引，默认值为 -1
  //判断是否有下一个元素
  public boolean hasNext() throws IOException {
    if (currentRows.isEmpty()) {
      processNext();  //检查 currentRows 是否为空，如果为空则调用 processNext() 生成更多的行
    }
    return !currentRows.isEmpty();
  }
  //返回并移除 currentRows 队列中的第一行，作为当前的输出行
  public InternalRow next() {
    return currentRows.remove();
  }

  /** 计算并返回自 BufferedRowIterator 对象创建以来的运行时间，单位为毫秒
   * Returns the elapsed time since this object is created. This object represents a pipeline so
   * this is a measure of how long the pipeline has been running.
   */
  public long durationMs() {
    return (System.nanoTime() - startTimeNs) / (1000 * 1000);
  }

  /**
   * Initializes from array of iterators of InternalRow.
   */
  //index：当前分区的索引；iters：一个 InternalRow 迭代器数组
  public abstract void init(int index, Iterator<InternalRow>[] iters);

  /*
   * Attributes of the following four methods are public. Thus, they can be also accessed from
   * methods in inner classes. See SPARK-23598
   */
  /** 将一行数据追加到 currentRows 中
   * Append a row to currentRows.
   */
  public void append(InternalRow row) {
    currentRows.add(row);
  }

  /**
   * Returns whether `processNext()` should stop processing next row from `input` or not.
   *
   * If it returns true, the caller should exit the loop (return from processNext()).
   */
  public boolean shouldStop() {
    return !currentRows.isEmpty();
  }

  /** 增加当前任务的最大执行内存
   * Increase the peak execution memory for current task.
   */
  public void incPeakExecutionMemory(long size) {
    TaskContext.get().taskMetrics().incPeakExecutionMemory(size);
  }

  /**
   * Processes the input until have a row as output (currentRow).
   * 以处理输入数据并生成输出行。它的目标是从输入数据中处理并生成至少一行输出数据
   * After it's called, if currentRow is still null, it means no more rows left.
   */
  protected abstract void processNext() throws IOException;
}
