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

package org.apache.spark.sql.catalyst.plans.physical

import org.apache.spark.sql.catalyst.InternalRow

/**
 * Marker trait to identify the shape in which tuples are broadcasted. Typical examples of this are
 * identity (tuples remain unchanged) or hashed (tuples are converted into some hash index).
 */
//标识广播数据如何处理的特征，定义了转换数据的行为
trait BroadcastMode {
  def transform(rows: Array[InternalRow]): Any

  def transform(rows: Iterator[InternalRow], sizeHint: Option[Long]): Any

  def canonicalized: BroadcastMode
}

/** 表示广播模式是“原样模式”（即广播的行数据保持原样，不进行任何转换）
 * IdentityBroadcastMode requires that rows are broadcasted in their original form.
 */
case object IdentityBroadcastMode extends BroadcastMode {
  // TODO: pack the UnsafeRows into single bytes array.
  override def transform(rows: Array[InternalRow]): Array[InternalRow] = rows

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): Array[InternalRow] = rows.toArray

  override def canonicalized: BroadcastMode = this
}
