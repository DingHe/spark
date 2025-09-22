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

import io.netty.channel.Channel;

/**
 * A bootstrap which is executed on a TransportClient before it is returned to the user.
 * This enables an initial exchange of information (e.g., SASL authentication tokens) on a once-per-
 * connection basis.
 * 提供一种在 TransportClient 被使用之前执行引导操作的方法，确保完成一些必要的步骤（如认证）
 * 由于连接（和 TransportClient）会被复用，这种引导操作一般只执行一次。虽然这些操作可能较为昂贵，但由于连接长期存在，因此可以接受
 * Since connections (and TransportClients) are reused as much as possible, it is generally
 * reasonable to perform an expensive bootstrapping operation, as they often share a lifespan with
 * the JVM itself.
 */
public interface TransportClientBootstrap {
  /** Performs the bootstrapping operation, throwing an exception on failure. */
  //用于在 TransportClient 上执行初始化操作。通常这种初始化操作是在连接的第一次建立时进行的，比如SASL认证或其他需要在客户端使用之前完成的操作
  void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}
