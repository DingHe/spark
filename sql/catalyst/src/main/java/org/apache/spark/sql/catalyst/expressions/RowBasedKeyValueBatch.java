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
package org.apache.spark.sql.catalyst.expressions;

import java.io.Closeable;
import java.io.IOException;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.memory.MemoryBlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RowBasedKeyValueBatch stores key value pairs in contiguous memory region.
 *
 * Each key or value is stored as a single UnsafeRow. Each record contains one key and one value
 * and some auxiliary data, which differs based on implementation:
 * i.e., `FixedLengthRowBasedKeyValueBatch` and `VariableLengthRowBasedKeyValueBatch`.
 *
 * We use `FixedLengthRowBasedKeyValueBatch` if all fields in the key and the value are fixed-length
 * data types. Otherwise we use `VariableLengthRowBasedKeyValueBatch`.
 * 存储键值对的一个抽象类。每个键值对由一个 UnsafeRow 组成，并存储在连续的内存区域中。这个类的实现支持两种不同的方式来处理数据：固定长度字段（FixedLengthRowBasedKeyValueBatch）和可变长度字段（VariableLengthRowBasedKeyValueBatch）
 * RowBasedKeyValueBatch is backed by a single page / MemoryBlock (ranges from 1 to 64MB depending
 * on the system configuration). If the page is full, the aggregate logic should fallback to a
 * second level, larger hash map. We intentionally use the single-page design because it simplifies
 * memory address encoding & decoding for each key-value pair. Because the maximum capacity for
 * RowBasedKeyValueBatch is only 2^16, it is unlikely we need a second page anyway. Filling the
 * page requires an average size for key value pairs to be larger than 1024 bytes.
 *
 */
public abstract class RowBasedKeyValueBatch extends MemoryConsumer implements Closeable {
  protected static final Logger logger = LoggerFactory.getLogger(RowBasedKeyValueBatch.class);

  private static final int DEFAULT_CAPACITY = 1 << 16; //默认的批量大小，设置为 65536（即 1 << 16），表示 RowBasedKeyValueBatch 可以存储的最大行数
  //分别表示键和值的 schema，定义了键和值的字段类型和结构
  protected final StructType keySchema;
  protected final StructType valueSchema;
  protected final int capacity; //能够存储的最大行数
  protected int numRows = 0; //当前批次中存储的行数

  // ids for current key row and value row being retrieved
  protected int keyRowId = -1;
  //用于存储键和值的 UnsafeRow 对象。每次从内存中读取一行数据时，都会复用这两个对象
  // placeholder for key and value corresponding to keyRowId.
  protected final UnsafeRow keyRow;
  protected final UnsafeRow valueRow;
  //存储数据的内存页。一个内存页用于存储一批数据
  protected MemoryBlock page = null;
  protected Object base = null; //内存页的基础对象，用于访问内存页中的数据
  protected final long recordStartOffset; //记录每个数据记录的起始偏移量
  protected long pageCursor = 0; //当前内存页中的游标，用于跟踪数据写入的进度

  public static RowBasedKeyValueBatch allocate(StructType keySchema, StructType valueSchema,
                                               TaskMemoryManager manager) {
    return allocate(keySchema, valueSchema, manager, DEFAULT_CAPACITY);
  }
  //根据传入的键值 schema 和内存管理器分配内存。如果键和值的字段都是固定长度的类型，则创建 FixedLengthRowBasedKeyValueBatch，否则创建 VariableLengthRowBasedKeyValueBatch
  public static RowBasedKeyValueBatch allocate(StructType keySchema, StructType valueSchema,
                                               TaskMemoryManager manager, int maxRows) {
    boolean allFixedLength = true;
    // checking if there is any variable length fields
    // there is probably a more succinct impl of this
    for (StructField field : keySchema.fields()) {
      allFixedLength = allFixedLength && UnsafeRow.isFixedLength(field.dataType());
    }
    for (StructField field : valueSchema.fields()) {
      allFixedLength = allFixedLength && UnsafeRow.isFixedLength(field.dataType());
    }

    if (allFixedLength) {
      return new FixedLengthRowBasedKeyValueBatch(keySchema, valueSchema, maxRows, manager);
    } else {
      return new VariableLengthRowBasedKeyValueBatch(keySchema, valueSchema, maxRows, manager);
    }
  }

  protected RowBasedKeyValueBatch(StructType keySchema, StructType valueSchema, int maxRows,
                                TaskMemoryManager manager) {
    super(manager, manager.getTungstenMemoryMode());

    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.capacity = maxRows;

    this.keyRow = new UnsafeRow(keySchema.length());
    this.valueRow = new UnsafeRow(valueSchema.length());

    if (!acquirePage(manager.pageSizeBytes())) {
      page = null;
      recordStartOffset = 0;
    } else {
      base = page.getBaseObject();
      recordStartOffset = page.getBaseOffset();
    }
  }
  //返回当前批次中存储的行数
  public final int numRows() { return numRows; }

  @Override
  public final void close() {
    if (page != null) {
      freePage(page);
      page = null;
    }
  }
  //尝试分配一个内存页。如果分配成功，返回 true 并设置相关的内存基础对象和偏移量。如果分配失败（内存不足），则返回 false
  private boolean acquirePage(long requiredSize) {
    try {
      page = allocatePage(requiredSize);
    } catch (SparkOutOfMemoryError e) {
      logger.warn("Failed to allocate page ({} bytes).", requiredSize);
      return false;
    }
    base = page.getBaseObject();
    pageCursor = 0;
    return true;
  }

  /** 将一个键值对追加到当前的批次中。该方法将数据复制到内存页中，并返回一个 UnsafeRow，指向该键值对的值。如果失败，则返回 null
   * Append a key value pair.
   * It copies data into the backing MemoryBlock.
   * Returns an UnsafeRow pointing to the value if succeeds, otherwise returns null.
   */
  public abstract UnsafeRow appendRow(Object kbase, long koff, int klen,
                                      Object vbase, long voff, int vlen);

  /** 根据给定的 rowId 获取对应的键。返回的 UnsafeRow 会在多次调用中复用
   * Returns the key row in this batch at `rowId`. Returned key row is reused across calls.
   */
  public abstract UnsafeRow getKeyRow(int rowId);

  /**
   * Returns the value row in this batch at `rowId`. Returned value row is reused across calls.
   * Because `getValueRow(id)` is always called after `getKeyRow(id)` with the same id, we use
   * `getValueFromKey(id) to retrieve value row, which reuses metadata from the cached key.
   */
  public final UnsafeRow getValueRow(int rowId) {
    return getValueFromKey(rowId);
  }

  /**
   * Returns the value row by two steps:
   * 1) looking up the key row with the same id (skipped if the key row is cached)
   * 2) retrieve the value row by reusing the metadata from step 1)
   * In most times, 1) is skipped because `getKeyRow(id)` is often called before `getValueRow(id)`.
   */
  protected abstract UnsafeRow getValueFromKey(int rowId);

  /** 此方法总是返回 0，并不会实际执行溢写操作，因为我们通常依赖其他内存消费者来进行溢写
   * Sometimes the TaskMemoryManager may call spill() on its associated MemoryConsumers to make
   * space for new consumers. For RowBasedKeyValueBatch, we do not actually spill and return 0.
   * We should not throw OutOfMemory exception here because other associated consumers might spill
   */
  @Override
  public final long spill(long size, MemoryConsumer trigger) throws IOException {
    logger.warn("Calling spill() on RowBasedKeyValueBatch. Will not spill but return 0.");
    return 0;
  }

  /**
   * Returns an iterator to go through all rows
   */
  public abstract org.apache.spark.unsafe.KVIterator<UnsafeRow, UnsafeRow> rowIterator();
}
