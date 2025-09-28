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

import java.nio.ByteBuffer;
// 核心作用是为具有唯一标识符（ID）的流式数据回调提供一个扩展机制，并增加了在流处理完成时返回一个 RPC 响应的能力
// 主要用于处理客户端上传流式数据到服务器的场景（尽管接口定义在 client 包下，但其功能更侧重于服务器端对流式上传的处理
public interface StreamCallbackWithID extends StreamCallback {
  String getID();

  /**
   * Response to return to client upon the completion of a stream. Currently only invoked in
   * {@link org.apache.spark.network.server.TransportRequestHandler#processStreamUpload}
   */
  // 当整个数据流（由 onComplete 标记）接收并处理完成后，该方法会被调用。它返回一个 ByteBuffer 作为最终的 RPC 响应，发送回发起流式传输的客户端
  default ByteBuffer getCompletionResponse() {
    return ByteBuffer.allocate(0);
  }
}
