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

package org.apache.spark.api.plugin;

import java.util.Collections;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Driver component of a {@link SparkPlugin}.
 *
 * @since 3.0.0
 */
// DriverPlugin 是 Spark 插件体系中专门运行在 Driver（驱动器）端 的组件接口。如果说 SparkPlugin 是插件的总入口，那么 DriverPlugin 就是该插件在“大脑”节点的具体实现。
// DriverPlugin 的核心作用是管理和协调。由于它运行在 Driver 端，它拥有全局视角，负责插件的初始化、与 Executor 通信、监控指标注册以及最后的资源回收。
// 全局初始化：在 Spark 任务开始前准备环境。
// 配置下发：将 Driver 端的配置或元数据传递给集群中的所有 Executor。
// RPC 消息处理：作为服务端，接收并响应来自各个 Executor 插件的消息。
// 生命周期管理：随着 SparkContext 的启动而启动，随其关闭而清理。
@DeveloperApi
public interface DriverPlugin {

  /**
   * Initialize the plugin.
   * <p>
   * This method is called early in the initialization of the Spark driver. Explicitly, it is
   * called before the Spark driver's task scheduler is initialized. This means that a lot
   * of other Spark subsystems may yet not have been initialized. This call also blocks driver
   * initialization.
   * <p>
   * It's recommended that plugins be careful about what operations are performed in this call,
   * preferably performing expensive operations in a separate thread, or postponing them until
   * the application has fully started.
   *
   * @param sc The SparkContext loading the plugin.
   * @param pluginContext Additional plugin-specific about the Spark application where the plugin
   *                      is running.
   * @return A map that will be provided to the {@link ExecutorPlugin#init(PluginContext,Map)}
   *         method.
   */
  // 插件的起点。在 Spark Driver 初始化早期被调用。
  // 调用时机：在 TaskScheduler（任务调度器）初始化之前。此时很多 Spark 子系统尚未就绪。
  // 阻塞性：该方法会阻塞 Driver 的启动。如果在此处执行耗时操作（如网络扫描），会导致整个 Spark 应用启动变慢。
  // 返回值：返回一个 Map<String, String>。这是该方法最精妙的地方：这个 Map 会被 Spark 自动发送到所有 Executor 节点，并作为参数传递给 ExecutorPlugin.init() 方法。
  default Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    return Collections.emptyMap();
  }

  /**
   * Register metrics published by the plugin with Spark's metrics system.
   * <p>
   * This method is called later in the initialization of the Spark application, after most
   * subsystems are up and the application ID is known. If there are metrics registered in
   * the registry ({@link PluginContext#metricRegistry()}), then a metrics source with the
   * plugin name will be created.
   * <p>
   * Note that even though the metric registry is still accessible after this method is called,
   * registering new metrics after this method is called may result in the metrics not being
   * available.
   *
   * @param appId The application ID from the cluster manager.
   * @param pluginContext Additional plugin-specific about the Spark application where the plugin
   *                      is running.
   */
  // 作用：将插件自定义的监控指标（Metrics）注册到 Spark 官方的度量系统中。
  // 调用时机：比 init 晚一些，此时大部分子系统已启动，且已获取到 appId。
  // 机制：你可以通过 pluginContext.metricRegistry() 获取注册表并添加指标。Spark 会自动创建一个以插件名为命名的指标源（Source）。
  default void registerMetrics(String appId, PluginContext pluginContext) {}

  /**
   * RPC message handler.
   * <p>
   * Plugins can use Spark's RPC system to send messages from executors to the driver (but not
   * the other way around, currently). Messages sent by the executor component of the plugin will
   * be delivered to this method, and the returned value will be sent back to the executor as
   * the reply, if the executor has requested one.
   * <p>
   * Any exception thrown will be sent back to the executor as an error, in case it is expecting
   * a reply. In case a reply is not expected, a log message will be written to the driver log.
   * <p>
   * The implementation of this handler should be thread-safe.
   * <p>
   * Note all plugins share RPC dispatch threads, and this method is called synchronously. So
   * performing expensive operations in this handler may affect the operation of other active
   * plugins. Internal Spark endpoints are not directly affected, though, since they use different
   * threads.
   * <p>
   * Spark guarantees that the driver component will be ready to receive messages through this
   * handler when executors are started.
   *
   * @param message The incoming message.
   * @return Value to be returned to the caller. Ignored if the caller does not expect a reply.
   */
  // RPC 消息处理器，负责接收来自 Executor 端插件的消息。
  // 通信流向：目前 Spark 只支持 Executor -> Driver 的单向发起通信（Driver 端被动响应）。
  // 线程安全：该方法会被多个 RPC 线程并发调用，因此实现必须是线程安全的。
  // 异常处理：抛出的异常会传回给 Executor；如果 Executor 不需要回复，则异常会被记录到日志。
  default Object receive(Object message) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * Informs the plugin that the Spark application is shutting down.
   * <p>
   * This method is called during the driver shutdown phase. It is recommended that plugins
   * not use any Spark functions (e.g. send RPC messages) during this call.
   */
  // 插件的终点。在 SparkContext 关闭（销毁）阶段调用。
  // 限制：在此阶段，Spark 的核心功能可能已经部分失效，因此不建议在此时再调用 Spark 的函数（如发送 RPC 消息）。
  default void shutdown() {}

}
