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

package org.apache.spark.internal.plugin

import java.util

import com.codahale.metrics.MetricRegistry

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.RpcUtils
// PluginContextImpl 是 PluginContext 接口的实现类。如果把 SparkPlugin 比作在 Spark 集群中运行的“插件程序”，那么 PluginContextImpl 就是为这些程序提供各种基础设施支持的“多功能工具箱”。
// 核心作用是连接插件与 Spark 运行时环境。它为插件开发者提供了访问 Spark 内部系统的受控入口，主要解决以下三个核心需求：
// 节点信息获取：让插件知道自己运行在哪个节点、拥有哪些资源（如 GPU）。
// 跨节点通信 (RPC)：让运行在 Executor 上的插件能够向 Driver 发送消息并获取回复。
// 监控集成 (Metrics)：提供一个指标注册表，让插件的自定义监控数据能无缝集成到 Spark 的度量系统中。
private class PluginContextImpl(
    pluginName: String, // 当前插件的完整限定类名。用于在 RPC 消息中标识身份，以及在度量系统中作为前缀区分不同插件。
    rpcEnv: RpcEnv, // Spark 的远程过程调用环境。它负责底层的网络通信，是实现 send 和 ask 方法的基础。
    metricsSystem: MetricsSystem, // Spark 内部的度量系统引用。用于将插件收集的 registry 注册进去，使其能被 Prometheus、Graphite 等外部监控工具采集。
    override val conf: SparkConf, // 当前 Spark 应用的配置信息。插件可以通过它获取环境变量或自定义配置。
    override val executorID: String, // 标识当前插件所在的 Executor ID（如果是 Driver 则为 "driver"）。
    override val resources: util.Map[String, ResourceInformation]) // 当前节点分配给 Spark 的硬件资源信息（如 GPU、FPGA 的地址和 ID）。
  extends PluginContext with Logging {
  // 返回当前运行节点的 IP 或主机名。
  override def hostname(): String = rpcEnv.address.hostPort.split(":")(0)
  // 该插件私有的 Dropwizard Metrics 注册表。插件将自定义的 Counter、Gauge 等指标存入此处。
  private val registry = new MetricRegistry()
  // 核心组件。这是一个指向 Driver 端 PluginEndpoint 的引用。它是插件从 Executor 向 Driver 发送数据的“专线”。使用 lazy 加载，确保在需要通信时才尝试建立连接。
  private lazy val driverEndpoint = try {
    RpcUtils.makeDriverRef(classOf[PluginEndpoint].getName(), conf, rpcEnv)
  } catch {
    case e: Exception =>
      logWarning(s"Failed to create driver plugin endpoint ref.", e)
      null
  }

  override def metricRegistry(): MetricRegistry = registry
  // 单向发送消息到 Driver。
  override def send(message: AnyRef): Unit = {
    if (driverEndpoint == null) {
      throw new IllegalStateException("Driver endpoint is not known.")
    }
    driverEndpoint.send(PluginMessage(pluginName, message))
  }
  // 同步请求并等待 Driver 的回复。
  override def ask(message: AnyRef): AnyRef = {
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askSync[AnyRef](PluginMessage(pluginName, message))
      } else {
        throw new IllegalStateException("Driver endpoint is not known.")
      }
    } catch {
      case e: SparkException if e.getCause() != null =>
        throw e.getCause()
    }
  }

  def registerMetrics(): Unit = {
    if (!registry.getMetrics().isEmpty()) {
      val src = new PluginMetricsSource(s"plugin.$pluginName", registry)
      metricsSystem.registerSource(src)
    }
  }

  class PluginMetricsSource(
      override val sourceName: String,
      override val metricRegistry: MetricRegistry)
    extends Source

}
