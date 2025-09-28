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

/**
 * Callback for the result of a single RPC. This will be invoked once with either success or
 * failure.
 */
// 核心作用是为 单个远程过程调用（RPC） 提供一个完整的、客户端回调机制的契约
// 当 Spark 客户端（例如，一个执行器）向远程服务器（例如，另一个执行器或外部 Shuffle 服务）发送一个 RPC 请求后，它需要一个对象来处理服务器返回的结果。RpcResponseCallback 正是这个结果的处理器：
public interface RpcResponseCallback extends BaseResponseCallback {
  /**
   * Successful serialized result from server.
   *
   * After `onSuccess` returns, `response` will be recycled and its content will become invalid.
   * Please copy the content of `response` if you want to use it after `onSuccess` returns.
   */
  //用于接收并处理服务器返回的序列化数据
  void onSuccess(ByteBuffer response);
}
