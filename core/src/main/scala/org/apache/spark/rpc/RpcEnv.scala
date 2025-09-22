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

package org.apache.spark.rpc

import java.io.File
import java.net.URI
import java.nio.channels.ReadableByteChannel

import scala.concurrent.Future

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.util.RpcUtils


/**
 * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
 * so that it can be created via Reflection.
 */
private[spark] object RpcEnv {

  def create(
      name: String,
      host: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager,
      clientMode: Boolean = false): RpcEnv = {
    create(name, host, host, port, conf, securityManager, 0, clientMode)
  }

  def create(
      name: String,
      bindAddress: String, //绑定的地址（RPC环境监听的地址）
      advertiseAddress: String, //广告地址，用于告诉其他节点RPC环境的地址
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager,
      numUsableCores: Int,  //可用的CPU核心数，通常用于优化资源分配
      clientMode: Boolean): RpcEnv = { //是否为客户端模式（默认为false）
    val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
      numUsableCores, clientMode)
    new NettyRpcEnvFactory().create(config)
  }
}


/**
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
 * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
 * sender, or logging them if no such sender or `NotSerializableException`.
 *
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)  //用于指定RPC查找的超时时间

  /** 返回与给定RpcEndpoint关联的RpcEndpointRef。RpcEndpointRef是RPC端点的引用，可以用于发送消息
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
   */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /** 返回当前RpcEnv监听的地址。该地址是其他节点与其通信的目标地址
   * Return the address that [[RpcEnv]] is listening to.
   */
  def address: RpcAddress

  /** 注册一个RpcEndpoint，并返回其RpcEndpointRef。这是实现远程通信的关键
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /** 异步地根据URI获取RpcEndpointRef
   * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /** 同步地根据URI获取RpcEndpointRef，此方法会阻塞直到结果返回
   * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
   */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /** 根据地址和端点名称获取RpcEndpointRef，并返回其引用
   * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
   * This is a blocking action.
   */
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  /** 停止指定的RpcEndpoint，并释放相关资源
   * Stop [[RpcEndpoint]] specified by `endpoint`.
   */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
   * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
   * call [[awaitTermination()]] straight after [[shutdown()]].
   */
  def shutdown(): Unit

  /**
   * Wait until [[RpcEnv]] exits.
   * 阻塞直到RpcEnv完全关闭，确保资源完全释放
   * TODO do we need a timeout parameter?
   */
  def awaitTermination(): Unit

  /** 用于反序列化RpcEndpointRef，该方法允许在反序列化过程中进行操作
   * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
   * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
   */
  def deserialize[T](deserializationAction: () => T): T

  /** 返回文件服务器实例，用于在RPC环境中传输文件。如果没有启动文件服务，返回null
   * Return the instance of the file server used to serve files. This may be `null` if the
   * RpcEnv is not operating in server mode.
   */
  def fileServer: RpcEnvFileServer

  /**
   * Open a channel to download a file from the given URI. If the URIs returned by the
   * RpcEnvFileServer use the "spark" scheme, this method will be called by the Utils class to
   * retrieve the files.
   * 打开一个通道从指定的URI下载文件。该方法是从RpcEnvFileServer获取文件的接口
   * @param uri URI with location of the file.
   */
  def openChannel(uri: String): ReadableByteChannel
}

/**
 * A server used by the RpcEnv to server files to other processes owned by the application.
 * 负责处理文件的分发。Spark允许在分布式环境中传输文件
 * The file server can return URIs handled by common libraries (such as "http" or "hdfs"), or
 * it can return "spark" URIs which will be handled by `RpcEnv#fetchFile`.
 */
private[spark] trait RpcEnvFileServer {

  /**
   * Adds a file to be served by this RpcEnv. This is used to serve files from the driver
   * to executors when they're stored on the driver's local file system.
   * 将本地文件添加到RpcEnv中，并返回一个URI来访问该文件
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addFile(file: File): String

  /**
   * Adds a jar to be served by this RpcEnv. Similar to `addFile` but for jars added using
   * `SparkContext.addJar`.
   * 将本地JAR文件添加到RpcEnv中，并返回一个URI来访问该JAR
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addJar(file: File): String

  /**
   * Adds a local directory to be served via this file server.
   * 将本地目录添加到RpcEnv，并返回一个URI来访问该目录。baseUri是目录的基础URI路径
   * @param baseUri Leading URI path (files can be retrieved by appending their relative
   *                path to this base URI). This cannot be "files" nor "jars".
   * @param path Path to the local directory.
   * @return URI for the root of the directory in the file server.
   */
  def addDirectory(baseUri: String, path: File): String

    /**
   * Adds a local directory to be served via this file server.
   * If the directory is already registered with the file server, it will result in a no-op.
   * 如果目录尚未添加，则添加目录，否则不做任何操作
   * @param baseUri Leading URI path (files can be retrieved by appending their relative
   *                path to this base URI). This cannot be "files" nor "jars".
   * @param path Path to the local directory.
   * @return URI for the root of the directory in the file server.
   */
  def addDirectoryIfAbsent(baseUri: String, path: File): String
  //验证并规范化目录的URI，确保它符合特定要求。
  /** Validates and normalizes the base URI for directories. */
  protected def validateDirectoryUri(baseUri: String): String = {
    val baseCanonicalUri = new URI(baseUri).normalize().getPath
    val fixedBaseUri = "/" + baseCanonicalUri.stripPrefix("/").stripSuffix("/")
    require(fixedBaseUri != "/files" && fixedBaseUri != "/jars",
      "Directory URI cannot be /files nor /jars.")
    fixedBaseUri
  }

  /**
   * Removes a file from this RpcEnv.
   *
   * @param key Local file to remove.
   */
  def removeFile(key: String): Unit

  /**
   * Removes a jar to from this RpcEnv.
   *
   * @param key Local jar to remove.
   */
  def removeJar(key: String): Unit
}

private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    securityManager: SecurityManager,
    numUsableCores: Int,
    clientMode: Boolean)
