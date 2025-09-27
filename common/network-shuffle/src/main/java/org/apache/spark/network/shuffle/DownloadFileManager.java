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

package org.apache.spark.network.shuffle;

import org.apache.spark.network.util.TransportConf;

/**
 * A manager to create temp block files used when fetching remote data to reduce the memory usage.
 * It will clean files when they won't be used any more.
 */
// 定义了 Spark 在 Reduce Task 执行 Shuffle 读取时，用于管理临时下载文件的契约
  //管理临时文件创建： 提供一个统一的方式来创建用于存储远程 Shuffle 数据的临时文件 (DownloadFile)。这些文件用于将数据下载到磁盘，以减少内存使用（避免 OOM，Out of Memory）
  //生命周期与清理： 提供一个机制来注册这些临时文件，以便在它们不再被使用时（即数据读取完毕后）进行自动清理，防止磁盘空间泄漏
public interface DownloadFileManager {

  /** Create a temp block file. */
  //创建临时文件
  DownloadFile createTempFile(TransportConf transportConf);

  /**
   * Register a temp file to clean up when it won't be used any more. Return whether the
   * file is registered successfully. If `false`, the caller should clean up the file by itself.
   */
  //注册文件以便清理
  boolean registerTempFileToClean(DownloadFile file);
}
