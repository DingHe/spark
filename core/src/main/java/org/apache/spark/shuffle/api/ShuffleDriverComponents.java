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

package org.apache.spark.shuffle.api;

import java.util.Map;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * 为了支持 Driver 端的 Shuffle 组件管理而设计的。其方法可以用来初始化 Shuffle 模块、注册 Shuffle 阶段、清理 Shuffle 状态等
 * An interface for building shuffle support modules for the Driver.
 */
@Private
public interface ShuffleDriverComponents {

  /**
   * Called once in the driver to bootstrap this module that is specific to this application.
   * This method is called before submitting executor requests to the cluster manager.
   *
   * This method should prepare the module with its shuffle components i.e. registering against
   * an external file servers or shuffle services, or creating tables in a shuffle
   * storage data database.
   * 将在 Driver 启动时被调用，通常在提交 Executor 请求之前执行。
   * 它的作用是初始化 Shuffle 模块相关的资源，如注册外部文件服务器、Shuffle 服务，或为 Shuffle 存储配置数据库等
   * @return additional SparkConf settings necessary for initializing the executor components.
   * This would include configurations that cannot be statically set on the application, like
   * the host:port of external services for shuffle storage.
   */
  Map<String, String> initializeApplication();

  /** 应用程序结束时被调用，用于清理 Shuffle 模块的状态，释放相关的资源，例如关闭与 Shuffle 相关的服务、清理缓存或删除存储的 Shuffle 数据等
   * Called once at the end of the Spark application to clean up any existing shuffle state.
   */
  void cleanupApplication();

  /**
   * Called once per shuffle id when the shuffle id is first generated for a shuffle stage.
   * 当一个新的 Shuffle 阶段被创建时（即 shuffleId 被生成），该方法会被调用。它可以用来在模块内部注册这个 Shuffle 阶段，
   * 确保所有必要的资源和设置在 Shuffle 阶段开始时被准备好
   * @param shuffleId The unique identifier for the shuffle stage.
   */
  default void registerShuffle(int shuffleId) {}

  /**
   * Removes shuffle data associated with the given shuffle.
   * 删除与特定 shuffleId 相关的 Shuffle 数据
   * @param shuffleId The unique identifier for the shuffle stage.
   * @param blocking Whether this call should block on the deletion of the data.
   */
  default void removeShuffle(int shuffleId, boolean blocking) {}

  /** 用于指示 Shuffle 数据是否可以存储到可靠的存储系统中，比如分布式文件系统（例如 HDFS）或远程 Shuffle 服务
   * Does this shuffle component support reliable storage - external to the lifecycle of the
   * executor host ? For example, writing shuffle data to a distributed filesystem or
   * persisting it in a remote shuffle service.
   */
  default boolean supportsReliableStorage() {
    return false;
  }
}
