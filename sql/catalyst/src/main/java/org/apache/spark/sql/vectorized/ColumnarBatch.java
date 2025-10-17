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
package org.apache.spark.sql.vectorized;

import java.util.*;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * This class wraps multiple ColumnVectors as a row-wise table. It provides a row view of this
 * batch so that Spark can access the data row by row. Instance of it is meant to be reused during
 * the entire data loading process. A data source may extend this class with customized logic.
 */
// Apache Spark SQL 向量化（Columnar）读取机制中的核心数据结构
// 作用是将底层的列式存储数据（如 Parquet、ORC）在内存中表示为一系列的 ColumnVector，并提供一个行式（Row-wise）视图，以便 Spark 的计算引擎可以像处理传统行一样访问数据
// 核心职责：
// 封装列式数据： 它封装了一个数组的 ColumnVector，每个 ColumnVector 包含一列数据的一个批次。这种列式表示方式极大地提高了数据处理效率（CPU 缓存友好、SIMD 优化）
// 提供行式兼容性： 通过内部的 ColumnarBatchRow，它允许 Spark 的处理逻辑以行为单位迭代和访问数据，从而兼容 Spark 的标准 InternalRow API。
// 批次处理： 它代表了数据源一次性读取并传递给查询引擎的固定大小（通常是 4096 行）数据块
@DeveloperApi
public class ColumnarBatch implements AutoCloseable {
  // 行数。
  // 存储当前批次中实际包含的逻辑行数
  protected int numRows;
  // 列向量数组。
  // 存储构成该批次的所有列的 ColumnVector 实例。它是该批次列式数据的核心。
  protected final ColumnVector[] columns;

  // Staging row returned from `getRow`.
  // 暂存行。 一个内部辅助对象，用于在调用 getRow(int rowId) 或通过迭代器访问时，重用并表示当前正在访问的行。
  // 它指向 columns 数组中的特定行索引，避免为每一行创建新的 InternalRow 对象
  protected final ColumnarBatchRow row;

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after
   * calling this. This must be called at the end to clean up memory allocations.
   */
  @Override
  public void close() {
    for (ColumnVector c: columns) {
      c.close();
    }
  }

  /**
   * Called to close all the columns if their resources are freeable between batches.
   * This is used to clean up memory allocated during columnar processing.
   */
  public void closeIfFreeable() {
    for (ColumnVector c: columns) {
      c.closeIfFreeable();
    }
  }

  /**
   * Returns an iterator over the rows in this batch.
   */
  // 获取行迭代器
  // 返回一个可以逐行迭代当前批次中数据的 Iterator<InternalRow>
  public Iterator<InternalRow> rowIterator() {
    final int maxRows = numRows;
    final ColumnarBatchRow row = new ColumnarBatchRow(columns);
    return new Iterator<InternalRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      @Override
      public InternalRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        row.rowId = rowId++;
        return row;
      }
    };
  }

  /**
   * Sets the number of rows in this batch.
   */
  public void setNumRows(int numRows) {
    this.numRows = numRows;
  }

  /**
   * Returns the number of columns that make up this batch.
   */
  public int numCols() { return columns.length; }

  /**
   * Returns the number of rows for read, including filtered rows.
   */
  public int numRows() { return numRows; }

  /**
   * Returns the column at `ordinal`.
   */
  public ColumnVector column(int ordinal) { return columns[ordinal]; }

  /**
   * Returns the row in this batch at `rowId`. Returned row is reused across calls.
   */
  public InternalRow getRow(int rowId) {
    assert(rowId >= 0 && rowId < numRows);
    row.rowId = rowId;
    return row;
  }

  public ColumnarBatch(ColumnVector[] columns) {
    this(columns, 0);
  }

  /**
   * Create a new batch from existing column vectors.
   * @param columns The columns of this batch
   * @param numRows The number of rows in this batch
   */
  public ColumnarBatch(ColumnVector[] columns, int numRows) {
    this.columns = columns;
    this.numRows = numRows;
    this.row = new ColumnarBatchRow(columns);
  }
}
