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

package org.apache.spark.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This interface provides an immutable view for data in the form of bytes. The implementation
 * should specify how the data is provided:
 * 表示管理内存数据缓冲区的抽象类，提供了一些基本操作方法，用于管理底层的数据存取、引用计数以及数据转换等，实现类包括 FileSegmentManagedBuffer（基于文件的缓冲区）、NioManagedBuffer（基于 NIO ByteBuffer 的缓冲区）、NettyManagedBuffer（基于 Netty ByteBuf 的缓冲区）
 * - {@link FileSegmentManagedBuffer}: data backed by part of a file
 * - {@link NioManagedBuffer}: data backed by a NIO ByteBuffer
 * - {@link NettyManagedBuffer}: data backed by a Netty ByteBuf
 *
 * The concrete buffer implementation might be managed outside the JVM garbage collector.
 * For example, in the case of {@link NettyManagedBuffer}, the buffers are reference counted.
 * In that case, if the buffer is going to be passed around to a different thread, retain/release
 * should be called.
 */
public abstract class ManagedBuffer {

  /** 返回缓冲区的数据大小（字节数）
   * Number of bytes of the data. If this buffer will decrypt for all of the views into the data,
   * this is the size of the decrypted data.
   */
  public abstract long size();

  /** 将缓冲区的数据以 NIO ByteBuffer 的形式暴露出来
   * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
   * returned ByteBuffer should not affect the content of this buffer.
   */
  // TODO: Deprecate this, usage may require expensive memory mapping or allocation.
  public abstract ByteBuffer nioByteBuffer() throws IOException;

  /** 将缓冲区的数据以 InputStream 的形式暴露出来
   * Exposes this buffer's data as an InputStream. The underlying implementation does not
   * necessarily check for the length of bytes read, so the caller is responsible for making sure
   * it does not go over the limit.
   */
  public abstract InputStream createInputStream() throws IOException;

  /** 如果缓冲区的实现支持引用计数（例如，NettyManagedBuffer），调用该方法会将引用计数增加 1，表示该缓冲区被引用并且仍然在使用中
   * Increment the reference count by one if applicable.
   */
  public abstract ManagedBuffer retain();

  /** 如果缓冲区的实现支持引用计数，调用该方法会将引用计数减少 1。如果引用计数降到 0，则会释放缓冲区的内存
   * If applicable, decrement the reference count by one and deallocates the buffer if the
   * reference count reaches zero.
   */
  public abstract ManagedBuffer release();

  /**
   * Convert the buffer into a Netty object, used to write the data out. The return value is either
   * a {@link io.netty.buffer.ByteBuf} or a {@link io.netty.channel.FileRegion}.
   * 将缓冲区转换为 Netty 对象，通常是一个 ByteBuf 或 FileRegion，用于写出数据到网络
   * If this method returns a ByteBuf, then that buffer's reference count will be incremented and
   * the caller will be responsible for releasing this new reference.
   */
  public abstract Object convertToNetty() throws IOException;
}
