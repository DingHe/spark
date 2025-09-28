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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.util.JavaUtils;

/**
 * A class that manages shuffle secret used by the external shuffle service.
 */
// Spark 网络认证模块中专门用于管理 External Shuffle Service 的应用程序密钥（Shuffle Secret） 的组件
// 应用生命周期同步：与 YARN NodeManager 的 AuxiliaryService 回调同步，负责在 Spark 应用启动时注册密钥，在应用终止时注销密钥，确保密钥的及时更新和清理
public class ShuffleSecretManager implements SecretKeyHolder {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleSecretManager.class);
  //线程安全哈希表。用于存储应用程序 ID (appId，String 类型) 到其对应的 Shuffle Secret (String 类型) 的映射关系。这是管理所有活动应用密钥的核心数据结构
  private final ConcurrentHashMap<String, String> shuffleSecretMap;

  // Spark user used for authenticating SASL connections
  // Note that this must match the value in org.apache.spark.SecurityManager
  // 定义了所有 Spark 应用程序在进行 Shuffle SASL 认证时使用的固定用户名，其值为 "sparkSaslUser"。这个值需要与 Spark 应用内部 SecurityManager 中的定义保持一致。
  private static final String SPARK_SASL_USER = "sparkSaslUser";

  public ShuffleSecretManager() {
    shuffleSecretMap = new ConcurrentHashMap<>();
  }

  /**
   * Register an application with its secret.
   * Executors need to first authenticate themselves with the same secret before
   * fetching shuffle files written by other executors in this application.
   */
  // 将应用程序 ID 及其对应的 Shuffle Secret 存储到 shuffleSecretMap 中。如果 appId 已经存在，新的密钥会覆盖旧的密钥（这对于处理应用程序尝试重试很有用
  public void registerApp(String appId, String shuffleSecret) {
    // Always put the new secret information to make sure it's the most up to date.
    // Otherwise we have to specifically look at the application attempt in addition
    // to the applicationId since the secrets change between application attempts on yarn.
    shuffleSecretMap.put(appId, shuffleSecret);
    logger.info("Registered shuffle secret for application {}", appId);
  }

  /**
   * Register an application with its secret specified as a byte buffer.
   */
  //它将传入的 ByteBuffer 格式的密钥通过 JavaUtils.bytesToString 转换为字符串，然后调用上一个 registerApp 方法进行注册
  public void registerApp(String appId, ByteBuffer shuffleSecret) {
    registerApp(appId, JavaUtils.bytesToString(shuffleSecret));
  }

  /**
   * Unregister an application along with its secret.
   * This is called when the application terminates.
   */
  //当应用程序终止时被调用。从 shuffleSecretMap 中移除该应用程序 ID 及其关联的密钥，释放资源并确保密钥不再有效
  public void unregisterApp(String appId) {
    shuffleSecretMap.remove(appId);
    logger.info("Unregistered shuffle secret for application {}", appId);
  }

  /**
   * Return the Spark user for authenticating SASL connections.
   */
  @Override
  public String getSaslUser(String appId) {
    return SPARK_SASL_USER;
  }

  /**
   * Return the secret key registered with the given application.
   * This key is used to authenticate the executors before they can fetch shuffle files
   * written by this application from the external shuffle service. If the specified
   * application is not registered, return null.
   */
  @Override
  public String getSecretKey(String appId) {
    return shuffleSecretMap.get(appId);
  }
}
