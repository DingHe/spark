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

package org.apache.spark.network.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.sasl.SaslClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.util.TransportConf;

/**
 * Bootstraps a {@link TransportClient} by performing authentication using Spark's auth protocol.
 *
 * This bootstrap falls back to using the SASL bootstrap if the server throws an error during
 * authentication, and the configuration allows it. This is used for backwards compatibility
 * with external shuffle services that do not support the new protocol.
 *
 * It also automatically falls back to SASL if the new encryption backend is disabled, so that
 * callers only need to install this bootstrap when authentication is enabled.
 */
// Spark 网络通信模块中用于客户端身份验证和加密协商的核心引导类
// 在 TransportClient 连接建立后、数据传输之前执行身份验证和安全设置
// 执行 Spark 新身份验证协议： 使用基于 AES 加密的 Spark Auth Protocol（Spark 新认证协议）与服务器进行握手，协商出会话密钥。
// 如果配置中禁用 AES 加密 (encryptionEnabled 为 false)，它会自动回退到使用旧的 SASL 协议
public class AuthClientBootstrap implements TransportClientBootstrap {

  private static final Logger LOG = LoggerFactory.getLogger(AuthClientBootstrap.class);
  // 存储 Spark 网络传输模块的配置，包含加密开关、SASL 回退开关和认证 RPC 超时时间等关键参数
  private final TransportConf conf;
  // 当前 Spark 应用程序的唯一标识符。在新认证协议中，它被用作获取密钥的标识（尽管代码注释提到在 Executor 启动时它可能被硬编码为通用用户）
  private final String appId;
  // 用于根据 appId 获取身份验证所需的共享密钥（Secret Key）
  private final SecretKeyHolder secretKeyHolder;

  public AuthClientBootstrap(
      TransportConf conf,
      String appId,
      SecretKeyHolder secretKeyHolder) {
    this.conf = conf;
    // TODO: right now this behaves like the SASL backend, because when executors start up
    // they don't necessarily know the app ID. So they send a hardcoded "user" that is defined
    // in the SecurityManager, which will also always return the same secret (regardless of the
    // user name). All that's needed here is for this "user" to match on both sides, since that's
    // required by the protocol. At some point, though, it would be better for the actual app ID
    // to be provided here.
    this.appId = appId;
    this.secretKeyHolder = secretKeyHolder;
  }
  // 是认证流程的入口点。在网络连接成功建立后调用
  @Override
  public void doBootstrap(TransportClient client, Channel channel) {
    if (!conf.encryptionEnabled()) {
      // 如果配置中禁用 AES 加密，则直接打印日志并调用 doSaslAuth 回退到旧的 SASL 协议
      LOG.debug("AES encryption disabled, using old auth protocol.");
      doSaslAuth(client, channel);
      return;
    }

    try {
      doSparkAuth(client, channel);
      //认证成功后，设置客户端ID
      client.setClientId(appId);
    } catch (GeneralSecurityException | IOException e) {
      throw Throwables.propagate(e);
    } catch (RuntimeException e) {
      // There isn't a good exception that can be caught here to know whether it's really
      // OK to switch back to SASL (because the server doesn't speak the new protocol). So
      // try it anyway, unless it's a timeout, which is locally fatal. In the worst case
      // things will fail again.
      if (!conf.saslFallback() || e.getCause() instanceof TimeoutException) {
        throw e;
      }

      if (LOG.isDebugEnabled()) {
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        LOG.debug("New auth protocol failed, trying SASL.", cause);
      } else {
        LOG.info("New auth protocol failed, trying SASL.");
      }
      doSaslAuth(client, channel);
    }
  }
  // 负责执行基于 AES 加密的新身份验证和会话密钥协商协议
  private void doSparkAuth(TransportClient client, Channel channel)
    throws GeneralSecurityException, IOException {
    // 从 secretKeyHolder 获取用于身份验证的共享密钥
    String secretKey = secretKeyHolder.getSecretKey(appId);
    //创建 AuthEngine 实例，它封装了协议状态机和加密逻辑
    try (AuthEngine engine = new AuthEngine(appId, secretKey, conf)) {
      //  生成客户端的挑战消息（Challenge Message）
      AuthMessage challenge = engine.challenge();
      // 将挑战消息编码为 Netty 的 ByteBuf 格式
      ByteBuf challengeData = Unpooled.buffer(challenge.encodedLength());
      challenge.encode(challengeData);
      // 使用 TransportClient 同步发送挑战消息，并等待服务器返回响应数据。超时时间由配置控制
      ByteBuffer responseData =
          client.sendRpcSync(challengeData.nioBuffer(), conf.authRTTimeoutMs());
      // 将服务器返回的响应数据解码为 AuthMessage
      AuthMessage response = AuthMessage.decodeMessage(responseData);
      //根据客户端的挑战和服务端的响应，AuthEngine 派生出最终的会话密钥（Session Key）
      engine.deriveSessionCipher(challenge, response);
      //获取派生的会话加密器 (SessionCipher)，并将其添加到 Netty Channel 的管道中，启用数据传输加密
      engine.sessionCipher().addToChannel(channel);
    }
  }

  private void doSaslAuth(TransportClient client, Channel channel) {
    SaslClientBootstrap sasl = new SaslClientBootstrap(conf, appId, secretKeyHolder);
    sasl.doBootstrap(client, channel);
  }

}
