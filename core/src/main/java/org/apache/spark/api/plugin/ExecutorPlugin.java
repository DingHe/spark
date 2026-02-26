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

import java.util.Map;

import org.apache.spark.TaskFailedReason;
import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Executor component of a {@link SparkPlugin}.
 *
 * @since 3.0.0
 */
// ExecutorPlugin 是 Spark 插件体系中运行在 Executor（执行器）端 的核心接口。它是插件在实际执行计算任务的节点上的“代理人”。
// ExecutorPlugin 的主要作用是让开发者能够控制和监控 Spark 执行器进程的生命周期，并介入任务（Task）级别的执行过程。
// 本地环境准备：在 Executor 启动时初始化必要的本地资源（如 GPU 驱动、本地文件缓存、第三方库连接）。
// 节点监控：在具体计算节点上收集指标（Metrics），如磁盘 I/O、内存使用情况或自定义硬件监控。
// 任务拦截：在每个 Task 开始前或结束后执行特定逻辑，用于审计、性能采样或任务级的环境配置。
@DeveloperApi
public interface ExecutorPlugin {

  /**
   * Initialize the executor plugin.
   * <p>
   * When a Spark plugin provides an executor plugin, this method will be called during the
   * initialization of the executor process. It will block executor initialization until it
   * returns.
   * <p>
   * Executor plugins that publish metrics should register all metrics with the context's
   * registry ({@link PluginContext#metricRegistry()}) when this method is called. Metrics
   * registered afterwards are not guaranteed to show up.
   *
   * @param ctx Context information for the executor where the plugin is running.
   * @param extraConf Extra configuration provided by the driver component during its
   *                  initialization.
   */
  // Executor 插件的初始化入口。
  // 调用时机：在 Executor 进程启动的早期。它会阻塞 Executor 的初始化，直到该方法返回。
  // 参数 ctx：提供了访问该 Executor 配置和指标注册表（MetricRegistry）的能力。
  // 参数 extraConf：这是一个关键点。它接收来自 Driver 端 DriverPlugin.init() 方法返回的 Map。这实现了从 Driver 到 Executor 的配置下发。
  default void init(PluginContext ctx, Map<String, String> extraConf) {}

  /**
   * Clean up and terminate this plugin.
   * <p>
   * This method is called during the executor shutdown phase, and blocks executor shutdown.
   */
  // Executor 进程退出时的清理钩子。
  default void shutdown() {}

  /**
   * Perform any action before the task is run.
   * <p>
   * This method is invoked from the same thread the task will be executed.
   * Task-specific information can be accessed via {@link org.apache.spark.TaskContext#get}.
   * <p>
   * Plugin authors should avoid expensive operations here, as this method will be called
   * on every task, and doing something expensive can significantly slow down a job.
   * It is not recommended for a user to call a remote service, for example.
   * <p>
   * Exceptions thrown from this method do not propagate - they're caught,
   * logged, and suppressed. Therefore exceptions when executing this method won't
   * make the job fail.
   *
   * @since 3.1.0
   */
  // 在每个 Task（任务）运行之前执行。
  // 调用线程：在执行 Task 的同一个线程中被调用。这意味着你可以通过 org.apache.spark.TaskContext.get() 获取当前任务的详细上下文信息。
  // 性能警告：由于每个 Task 都会调用它，因此绝对不能在此执行耗时操作（如远程 RPC 调用），否则会极大地降低 Job 的并行效率。
  default void onTaskStart() {}

  /**
   * Perform an action after tasks completes without exceptions.
   * <p>
   * As {@link #onTaskStart() onTaskStart} exceptions are suppressed, this method
   * will still be invoked even if the corresponding {@link #onTaskStart} call for this
   * task failed.
   * <p>
   * Same warnings of {@link #onTaskStart() onTaskStart} apply here.
   *
   * @since 3.1.0
   */
  // 在 Task 成功完成（无异常抛出）后执行。
  // 应用场景：可用于记录任务耗时、清理任务产生的临时 ThreadLocal 变量等。
  default void onTaskSucceeded() {}

  /**
   * Perform an action after tasks completes with exceptions.
   * <p>
   * Same warnings of {@link #onTaskStart() onTaskStart} apply here.
   *
   * @param failureReason the exception thrown from the failed task.
   *
   * @since 3.1.0
   */
  // 作用：在 Task 执行失败（抛出异常）后执行。
  // 应用场景：用于诊断特定节点的故障原因。例如，如果某个节点因为硬件问题导致任务频繁失败，可以在此处捕获并上报。
  default void onTaskFailed(TaskFailedReason failureReason) {}
}
