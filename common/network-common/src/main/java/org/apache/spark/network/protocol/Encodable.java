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

package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Interface for an object which can be encoded into a ByteBuf. Multiple Encodable objects are
 * stored in a single, pre-allocated ByteBuf, so Encodables must also provide their length.
 * 定义可以被编码为 ByteBuf 对象的类，要求实现类提供能够将对象转换为字节流的功能，并通过 ByteBuf 来传输或存储数据
 * Encodable objects should provide a static "decode(ByteBuf)" method which is invoked by
 * {@link MessageDecoder}. During decoding, if the object uses the ByteBuf as its data (rather than
 * just copying data from it), then you must retain() the ByteBuf.
 *
 * Additionally, when adding a new Encodable Message, add it to {@link Message.Type}.
 */
public interface Encodable {
  /** Number of bytes of the encoded form of this object. */
  int encodedLength(); //返回对象的编码后字节长度，即对象序列化后占用的字节数

  /**
   * Serializes this object by writing into the given ByteBuf.
   * This method must write exactly encodedLength() bytes.
   */
  void encode(ByteBuf buf); //负责将对象编码成字节数据，并写入到传入的 ByteBuf 缓冲区中
}
