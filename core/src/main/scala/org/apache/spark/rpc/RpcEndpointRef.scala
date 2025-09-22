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

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

/** 用于表示远程 RpcEndpoint 的引用，它支持通过 RPC 进行消息发送和接收
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 */
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {

  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf) //默认的请求超时时间

  /**
   * return the address for the [[RpcEndpointRef]]
   */
  def address: RpcAddress //远程端点的地址

  def name: String //表示 RpcEndpointRef 的名称

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   */
  def send(message: Any): Unit  //异步方法，即发送消息后不等待响应，消息会被立即发送但没有任何回复处理

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a
   * [[AbortableRpcFuture]] to receive the reply within the specified timeout.
   * The [[AbortableRpcFuture]] instance wraps [[Future]] with additional `abort` method.
   *  发送消息并返回一个可以中止的 Future 对象，允许用户在某些情况下中止该 RPC 调用
   * This method only sends the message once and never retries.
   */
  def askAbortable[T: ClassTag](message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]  //发送消息到目标 RpcEndpoint，并返回一个 Future，该 Future 会在超时或接收到响应时完成

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * default timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
   * loop of [[RpcEndpoint]].

   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout) //通过调用 askSync（带超时的版本）来同步获取消息的返回值

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * specified timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }

}

/**
 * An exception thrown if the RPC is aborted.
 */
private[spark] class RpcAbortException(message: String) extends Exception(message)

/**
 * A wrapper for [[Future]] but add abort method.
 * This is used in long run RPC and provide an approach to abort the RPC.
 */
private[spark]
class AbortableRpcFuture[T: ClassTag](val future: Future[T], onAbort: Throwable => Unit) {
  def abort(t: Throwable): Unit = onAbort(t)
}
