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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Bootstraps a {@link TransportClient} by performing SASL authentication on the connection. The
 * server should be setup with a {@link SaslRpcHandler} with matching keys for the given appId.
 */
// Spark 网络通信模块中用于在客户端和服务器之间执行 SASL (Simple Authentication and Security Layer) 身份验证的引导类
// 身份验证： 在 TransportClient 连接建立后，但在开始数据传输之前，通过标准的 SASL 挑战-响应（challenge-response）机制与服务器进行握手，证明客户端的身份（基于预共享密钥 PSK）
// 安全性协商： 协商并确定连接的安全级别。如果配置启用了 SASL 加密 (saslEncryption) 且服务器同意，它将设置 Netty Channel 管道，为后续的数据传输启用 数据加密。
// 兼容性保障： 在较新的 Spark 版本中，它通常作为 AuthClientBootstrap（使用新的 AES 认证协议）的回退机制而存在，用于兼容不支持新协议的旧组件（如旧版外部 Shuffle Service）
public class SaslClientBootstrap implements TransportClientBootstrap {
  private static final Logger logger = LoggerFactory.getLogger(SaslClientBootstrap.class);

  private final TransportConf conf;
  private final String appId;
  // 用于根据 appId 获取身份验证所需的预共享密钥（Secret Key）
  private final SecretKeyHolder secretKeyHolder;

  public SaslClientBootstrap(TransportConf conf, String appId, SecretKeyHolder secretKeyHolder) {
    this.conf = conf;
    this.appId = appId;
    this.secretKeyHolder = secretKeyHolder;
  }

  /**
   * Performs SASL authentication by sending a token, and then proceeding with the SASL
   * challenge-response tokens until we either successfully authenticate or throw an exception
   * due to mismatch.
   */
  @Override
  public void doBootstrap(TransportClient client, Channel channel) {
    // 是 Spark 对标准 Java SASL 客户端的包装，封装了挑战-响应逻辑
    SparkSaslClient saslClient = new SparkSaslClient(appId, secretKeyHolder, conf.saslEncryption());
    try {
      byte[] payload = saslClient.firstToken();

      while (!saslClient.isComplete()) {
        SaslMessage msg = new SaslMessage(appId, payload);
        ByteBuf buf = Unpooled.buffer(msg.encodedLength() + (int) msg.body().size());
        msg.encode(buf);
        ByteBuffer response;
        buf.writeBytes(msg.body().nioByteBuffer());
        try {
          response = client.sendRpcSync(buf.nioBuffer(), conf.authRTTimeoutMs());
        } catch (RuntimeException ex) {
          // We know it is a Sasl timeout here if it is a TimeoutException.
          if (ex.getCause() instanceof TimeoutException) {
            throw new SaslTimeoutException(ex.getCause());
          } else {
            throw ex;
          }
        }
        payload = saslClient.response(JavaUtils.bufferToArray(response));
      }

      client.setClientId(appId);

      if (conf.saslEncryption()) {
        if (!SparkSaslServer.QOP_AUTH_CONF.equals(saslClient.getNegotiatedProperty(Sasl.QOP))) {
          throw new RuntimeException(
            new SaslException("Encryption requests by negotiated non-encrypted connection."));
        }

        SaslEncryption.addToChannel(channel, saslClient, conf.maxSaslEncryptedBlockSize());
        saslClient = null;
        logger.debug("Channel {} configured for encryption.", client);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      if (saslClient != null) {
        try {
          // Once authentication is complete, the server will trust all remaining communication.
          saslClient.dispose();
        } catch (RuntimeException e) {
          logger.error("Error while disposing SASL client", e);
        }
      }
    }
  }

}
