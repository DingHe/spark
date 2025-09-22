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

package org.apache.spark.sql.connector.read;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A factory used to create {@link PartitionReader} instances.
 * <p>
 * If Spark fails to execute any methods in the implementations of this interface or in the returned
 * {@link PartitionReader} (by throwing an exception), corresponding Spark task would fail and
 * get retried until hitting the maximum retry times.
 *
 * @since 3.0.0
 */
//数据源 V2 API 的一个核心接口，它的主要作用是 创建分区读取器（PartitionReader），用于 RDD 分区级数据读取
@Evolving
public interface PartitionReaderFactory extends Serializable {

  /**
   * Returns a row-based partition reader to read data from the given {@link InputPartition}.
   * <p>
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   */
  //创建 行式（row-based）分区读取器，用于读取 InternalRow 数据
  PartitionReader<InternalRow> createReader(InputPartition partition);

  /**
   * Returns a columnar partition reader to read data from the given {@link InputPartition}.
   * <p>
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   */
  //如果数据源支持 列式读取（columnar format，如 Parquet, ORC），需要 重写 该方法
  default PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    throw new UnsupportedOperationException("Cannot create columnar reader.");
  }

  /**
   * Returns true if the given {@link InputPartition} should be read by Spark in a columnar way.
   * This means, implementations must also implement {@link #createColumnarReader(InputPartition)}
   * for the input partitions that this method returns true.
   * <p>
   * As of Spark 2.4, Spark can only read all input partition in a columnar way, or none of them.
   * Data source can't mix columnar and row-based partitions. This may be relaxed in future
   * versions.
   */
  //返回 true 表示支持列式读取，
  //default 关键字用于 提供方法的默认实现。这允许接口定义具有可选实现的方法，而不强制所有实现类都必须提供自己的实现
  default boolean supportColumnarReads(InputPartition partition) {
    return false;
  }
}
