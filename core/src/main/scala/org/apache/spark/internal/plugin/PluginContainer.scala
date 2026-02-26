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

import scala.collection.JavaConverters._
import scala.util.{Either, Left, Right}

import org.apache.spark.{SparkContext, SparkEnv, TaskFailedReason}
import org.apache.spark.api.plugin._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util.Utils

// PluginContainer 是 Spark 插件系统的实际执行者和管理器。如果说 SparkPlugin 是协议，那么 PluginContainer 就是实现该协议的“引擎”。
// 该类的主要作用是封装插件的复杂生命周期管理。它将 Driver 端和 Executor 端的插件逻辑进行了物理分离（通过两个子类实现），并屏蔽了底层复杂的初始化和配置传递细节。
// 统一接口：通过 PluginContainer 抽象类定义了插件在 Spark 运行期间必须响应的事件钩子。
// 配置桥接：实现了将 Driver 端插件生成的配置（extraConf）自动通过 SparkConf 序列化并同步到 Executor 端。
// 组件隔离：确保 Driver 端只运行 DriverPlugin 逻辑，Executor 端只运行 ExecutorPlugin 拦截任务逻辑。
// 异常兜底：负责捕获插件代码中的异常，防止由于某个插件的崩溃导致整个 Spark 作业失败（Suppressing errors）。
sealed abstract class PluginContainer {
  // 停止所有已加载的插件。
  def shutdown(): Unit
  // 触发插件的指标注册逻辑（仅 Driver 有效）
  def registerMetrics(appId: String): Unit
  // 任务生命周期的钩子函数（仅 Executor 有效）
  def onTaskStart(): Unit
  def onTaskSucceeded(): Unit
  def onTaskFailed(failureReason: TaskFailedReason): Unit

}

private class DriverPluginContainer(
    sc: SparkContext,
    resources: java.util.Map[String, ResourceInformation],
    plugins: Seq[SparkPlugin])
  extends PluginContainer with Logging {
  // 存储三元组 (插件类名, DriverPlugin 实例, PluginContextImpl)。它在初始化时就被填满。
  private val driverPlugins: Seq[(String, DriverPlugin, PluginContextImpl)] = plugins.flatMap { p =>
    val driverPlugin = p.driverPlugin()
    if (driverPlugin != null) {
      val name = p.getClass().getName()
      val ctx = new PluginContextImpl(name, sc.env.rpcEnv, sc.env.metricsSystem, sc.conf,
        sc.env.executorId, resources)
      // 调用插件的 init() 方法获取 extraConf
      val extraConf = driverPlugin.init(sc, ctx)
      if (extraConf != null) {
        // 将 extraConf 中的每一个键值对打上 spark.plugins.internal.conf.[插件名] 的前缀，存入 SparkConf。这样这些配置会自动跟随 Spark 广播到所有 Executor。
        extraConf.asScala.foreach { case (k, v) =>
          sc.conf.set(s"${PluginContainer.EXTRA_CONF_PREFIX}$name.$k", v)
        }
      }
      logInfo(s"Initialized driver component for plugin $name.")
      Some((p.getClass().getName(), driverPlugin, ctx))
    } else {
      None
    }
  }

  if (driverPlugins.nonEmpty) {
    // 如果存在 Driver 插件，会启动一个名为 PluginEndpoint 的 RPC 端点，接收 Executor 的消息。
    val pluginsByName = driverPlugins.map { case (name, plugin, _) => (name, plugin) }.toMap
    sc.env.rpcEnv.setupEndpoint(classOf[PluginEndpoint].getName(),
      new PluginEndpoint(pluginsByName, sc.env.rpcEnv))
  }
  // 执行插件自定义的指标注册。
  override def registerMetrics(appId: String): Unit = {
    driverPlugins.foreach { case (_, plugin, ctx) =>
      plugin.registerMetrics(appId, ctx)
      ctx.registerMetrics()
    }
  }

  override def shutdown(): Unit = {
    driverPlugins.foreach { case (name, plugin, _) =>
      try {
        logDebug(s"Stopping plugin $name.")
        plugin.shutdown()
      } catch {
        case t: Throwable =>
          logInfo(s"Exception while shutting down plugin $name.", t)
      }
    }
  }

  override def onTaskStart(): Unit = {
    throw new IllegalStateException("Should not be called for the driver container.")
  }

  override def onTaskSucceeded(): Unit = {
    throw new IllegalStateException("Should not be called for the driver container.")
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    throw new IllegalStateException("Should not be called for the driver container.")
  }
}

private class ExecutorPluginContainer(
    env: SparkEnv,
    resources: java.util.Map[String, ResourceInformation],
    plugins: Seq[SparkPlugin])
  extends PluginContainer with Logging {
  // 存储二元组 (类名, ExecutorPlugin 实例)
  private val executorPlugins: Seq[(String, ExecutorPlugin)] = {
    // 从 SparkConf 中筛选出前缀为 EXTRA_CONF_PREFIX 且属于当前插件的配置。
    val allExtraConf = env.conf.getAllWithPrefix(PluginContainer.EXTRA_CONF_PREFIX)

    plugins.flatMap { p =>
      val executorPlugin = p.executorPlugin()
      if (executorPlugin != null) {
        val name = p.getClass().getName()
        val prefix = name + "."
        val extraConf = allExtraConf
          .filter { case (k, v) => k.startsWith(prefix) }
          .map { case (k, v) => k.substring(prefix.length()) -> v }
          .toMap
          .asJava
        val ctx = new PluginContextImpl(name, env.rpcEnv, env.metricsSystem, env.conf,
          env.executorId, resources)
        // 调用 executorPlugin.init(ctx, extraConf)，将 Driver 传来的配置交还给插件。
        executorPlugin.init(ctx, extraConf)
        ctx.registerMetrics()

        logInfo(s"Initialized executor component for plugin $name.")
        Some(p.getClass().getName() -> executorPlugin)
      } else {
        None
      }
    }
  }

  override def registerMetrics(appId: String): Unit = {
    throw new IllegalStateException("Should not be called for the executor container.")
  }

  override def shutdown(): Unit = {
    executorPlugins.foreach { case (name, plugin) =>
      try {
        logDebug(s"Stopping plugin $name.")
        plugin.shutdown()
      } catch {
        case t: Throwable =>
          logInfo(s"Exception while shutting down plugin $name.", t)
      }
    }
  }

  override def onTaskStart(): Unit = {
    executorPlugins.foreach { case (name, plugin) =>
      try {
        plugin.onTaskStart()
      } catch {
        case t: Throwable =>
          logInfo(s"Exception while calling onTaskStart on plugin $name.", t)
      }
    }
  }

  override def onTaskSucceeded(): Unit = {
    executorPlugins.foreach { case (name, plugin) =>
      try {
        plugin.onTaskSucceeded()
      } catch {
        case t: Throwable =>
          logInfo(s"Exception while calling onTaskSucceeded on plugin $name.", t)
      }
    }
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    executorPlugins.foreach { case (name, plugin) =>
      try {
        plugin.onTaskFailed(failureReason)
      } catch {
        case t: Throwable =>
          logInfo(s"Exception while calling onTaskFailed on plugin $name.", t)
      }
    }
  }
}

object PluginContainer {

  val EXTRA_CONF_PREFIX = "spark.plugins.internal.conf."

  def apply(
      sc: SparkContext,
      resources: java.util.Map[String, ResourceInformation]): Option[PluginContainer] = {
    PluginContainer(Left(sc), resources)
  }

  def apply(
      env: SparkEnv,
      resources: java.util.Map[String, ResourceInformation]): Option[PluginContainer] = {
    PluginContainer(Right(env), resources)
  }


  private def apply(
      ctx: Either[SparkContext, SparkEnv],
      resources: java.util.Map[String, ResourceInformation]): Option[PluginContainer] = {
    val conf = ctx.fold(_.conf, _.conf)
    val plugins = Utils.loadExtensions(classOf[SparkPlugin], conf.get(PLUGINS).distinct, conf)
    if (plugins.nonEmpty) {
      ctx match {
        case Left(sc) => Some(new DriverPluginContainer(sc, resources, plugins))
        case Right(env) => Some(new ExecutorPluginContainer(env, resources, plugins))
      }
    } else {
      None
    }
  }
}
