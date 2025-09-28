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

package org.apache.spark.network.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Callback for streaming data. Stream data will be offered to the
 * {@link #onData(String, ByteBuffer)} method as it arrives. Once all the stream data is received,
 * {@link #onComplete(String)} will be called.
 * <p>
 * The network library guarantees that a single thread will call these methods at a time, but
 * different call may be made by different threads.
 */
// 核心作用是为接收大规模、异步的流式数据提供一个客户端回调机制的契约
// 当客户端请求的数据量很大（例如，一个巨大的 Shuffle 文件或一个 RDD 块），为了避免一次性将所有数据加载到内存中，服务器会以数据流（Stream） 的形式分块发送。
// StreamCallback 的实现类就负责按块接收这些数据，并在流处理的不同生命周期（数据到达、流结束、流失败）触发相应的逻辑
public interface StreamCallback {
  /** Called upon receipt of stream data. */
  //streamId: 当前数据流的唯一标识符。 buf: 包含流数据的字节缓冲区
  // 当服务器发送的一个数据块到达客户端时被调用。客户端可以通过读取 buf 中的数据来处理流的一部分。这个方法会被重复调用，直到整个流接收完毕
  void onData(String streamId, ByteBuffer buf) throws IOException;

  /** Called when all data from the stream has been received. */
  // streamId: 当前数据流的唯一标识符。
  // 当客户端成功接收到数据流中的所有数据块后，服务器会发送一个完成信号，此时该方法被调用
  void onComplete(String streamId) throws IOException;

  /** Called if there's an error reading data from the stream. */
  // streamId: 当前数据流的唯一标识符。 cause: 导致失败的异常对象。
  // 如果在接收数据流的任何阶段发生错误（例如网络连接中断、磁盘 I/O 错误或服务器端异常），该方法会被调用。
  // 它接收一个 Throwable 对象，提供了失败的具体原因。客户端应该在此处执行错误处理和资源清理。
  void onFailure(String streamId, Throwable cause) throws IOException;
}
