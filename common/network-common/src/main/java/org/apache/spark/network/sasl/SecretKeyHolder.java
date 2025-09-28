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

package org.apache.spark.network.sasl;

/**
 * Interface for getting a secret key associated with some application.
 */
// 作用是为 Spark 的 SASL (Simple Authentication and Security Layer) 认证机制提供一个安全凭证查找服务的契约
// 在 Spark 中，特别是使用 External Shuffle Service 或其他需要安全通信的组件时，SASL 被用于验证执行器或客户端的身份，防止未经授权的访问（例如，阻止一个应用的执行器拉取另一个应用的数据）
public interface SecretKeyHolder {
  /**
   * Gets an appropriate SASL User for the given appId.
   * @throws IllegalArgumentException if the given appId is not associated with a SASL user.
   */
  // appId: 应用程序的唯一标识符
  // 根据给定的应用程序 ID (appId)，返回用于 SASL 认证的用户名。
  // 在 Spark 的实现中，这个用户名通常用于标识一个应用程序的身份。如果 appId 没有关联的 SASL 用户，则抛出 IllegalArgumentException
  String getSaslUser(String appId);

  /**
   * Gets an appropriate SASL secret key for the given appId.
   * @throws IllegalArgumentException if the given appId is not associated with a SASL secret key.
   */
  //根据给定的应用程序 ID (appId)，返回用于 SASL 认证的密钥或密码。
  // 这是认证的关键凭证，用于客户端和服务器之间建立安全连接。如果 appId 没有关联的密钥，则抛出 IllegalArgumentException
  String getSecretKey(String appId);
}
