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

/**
 * A basic callback. This is extended by {@link RpcResponseCallback} and
 * {@link MergedBlockMetaResponseCallback} so that both RpcRequests and MergedBlockMetaRequests
 * can be handled in {@link TransportResponseHandler} a similar way.
 *
 * @since 3.2.0
 */
// 主要作用是作为一个基础的、通用的回调机制，用于处理 Spark 客户端向服务器发送请求后，在处理失败时所需的通用逻辑
// 以下情况下会被调用：
// 1. 服务器端传播的异常：服务器在处理请求时发生错误，并将异常信息返回给客户端。
// 2. 客户端引发的异常：在客户端与服务器通信过程中，由于网络问题（如连接断开、超时）或数据解析错误等，在客户端本地抛出的异常。 该方法接收一个 Throwable e 参数，代表失败的原因
public interface BaseResponseCallback {

  /** Exception either propagated from server or raised on client side. */
  void onFailure(Throwable e);
}
