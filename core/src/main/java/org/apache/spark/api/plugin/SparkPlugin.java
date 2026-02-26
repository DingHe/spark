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

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * A plugin that can be dynamically loaded into a Spark application.
 * <p>
 * Plugins can be loaded by adding the plugin's class name to the appropriate Spark configuration.
 * Check the Spark monitoring guide for details.
 * <p>
 * Plugins have two optional components: a driver-side component, of which a single instance is
 * created per application, inside the Spark driver. And an executor-side component, of which one
 * instance is created in each executor that is started by Spark. Details of each component can be
 * found in the documentation for {@link DriverPlugin} and {@link ExecutorPlugin}.
 *
 * @since 3.0.0
 */
// SparkPlugin 是 Apache Spark 提供的一个开发者接口（Developer API），允许用户在 Spark 应用程序中动态注入自定义逻辑。它是 Spark 插件系统的核心入口。
// SparkPlugin 的主要作用是提供一种标准化的扩展机制，让开发者能够在不修改 Spark 源码的情况下，将自定义代码深度集成到 Spark 的运行周期中。
// 跨节点部署：它定义了插件在 Driver（驱动器） 和 Executor（执行器） 两端的操作逻辑。
// 资源监控与管理：通常用于实现自定义监控（如收集特定指标）、资源初始化、或者在 Spark 集群中建立辅助服务。
// 动态加载：通过 Spark 配置参数（如 spark.plugins）指定类名，Spark 启动时会自动实例化这些插件。
@DeveloperApi
public interface SparkPlugin {

  /**
   * Return the plugin's driver-side component.
   *
   * @return The driver-side component, or null if one is not needed.
   */
  // 返回插件在 Driver 端 的组件实例。Driver 是 Spark 应用的大脑，负责任务调度和状态管理。
  // 生命周期：在 SparkContext 初始化期间被调用。
  // 应用场景：例如，你想要在 Driver 端开启一个 Web Server 来展示实时处理进度。
  DriverPlugin driverPlugin();

  /**
   * Return the plugin's executor-side component.
   *
   * @return The executor-side component, or null if one is not needed.
   */
  // 返回插件在 Executor 端 的组件实例。Executor 是 Spark 真正执行计算任务的工作节点。
  // 生命周期：每当一个新的 Executor 进程启动时，Spark 都会调用此方法创建一个新的插件实例。
  // 职责：ExecutorPlugin 常用于初始化本地资源（如加载本地库、初始化硬件加速器 GPU/FPGA），或者监控每个节点的内存/ CPU 状态。
  // 应用场景：例如，在每个执行器启动时，预先建立一个指向高性能数据库的任务连接池。
  ExecutorPlugin executorPlugin();

}
