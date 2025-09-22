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
package org.apache.spark.scheduler.cluster

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID

/**
 * :: DeveloperApi ::
 * Stores information about an executor to pass from the scheduler to SparkListeners.
 */
//用于存储执行器的信息
@DeveloperApi
class ExecutorInfo(
    val executorHost: String,  //执行器所在的主机地址
    val totalCores: Int,  //该执行器可用的总核心数（CPU cores）。它指示执行器在主机上可以分配的计算资源
    val logUrlMap: Map[String, String],  //存储执行器的日志文件的 URL 地址，可以通过这些 URL 来访问执行器的日志
    val attributes: Map[String, String],  //通常用于为执行器分配额外的标识或参数信息，例如标记执行器的特性或配置
    val resourcesInfo: Map[String, ResourceInformation], //存储了执行器的资源信息。ResourceInformation 是一个包含资源详细信息的类，它描述了执行器上使用的资源（如 GPU、内存等）的具体情况
    val resourceProfileId: Int,  //表示执行器所属的资源配置文件的 ID
    val registrationTime: Option[Long], //执行器的注册时间
    val requestTime: Option[Long]) {

  def this(executorHost: String, totalCores: Int, logUrlMap: Map[String, String],
      attributes: Map[String, String], resourcesInfo: Map[String, ResourceInformation],
      resourceProfileId: Int) = {
    this(executorHost, totalCores, logUrlMap, attributes, resourcesInfo, resourceProfileId,
      None, None)
  }
  def this(executorHost: String, totalCores: Int, logUrlMap: Map[String, String]) = {
    this(executorHost, totalCores, logUrlMap, Map.empty, Map.empty, DEFAULT_RESOURCE_PROFILE_ID,
      None, None)
  }

  def this(
      executorHost: String,
      totalCores: Int,
      logUrlMap: Map[String, String],
      attributes: Map[String, String]) = {
    this(executorHost, totalCores, logUrlMap, attributes, Map.empty, DEFAULT_RESOURCE_PROFILE_ID,
      None, None)
  }

  def this(
      executorHost: String,
      totalCores: Int,
      logUrlMap: Map[String, String],
      attributes: Map[String, String],
      resourcesInfo: Map[String, ResourceInformation]) = {
    this(executorHost, totalCores, logUrlMap, attributes, resourcesInfo,
      DEFAULT_RESOURCE_PROFILE_ID, None, None)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[ExecutorInfo]

  override def equals(other: Any): Boolean = other match {
    case that: ExecutorInfo =>
      (that canEqual this) &&
        executorHost == that.executorHost &&
        totalCores == that.totalCores &&
        logUrlMap == that.logUrlMap &&
        attributes == that.attributes &&
        resourcesInfo == that.resourcesInfo &&
        resourceProfileId == that.resourceProfileId
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(executorHost, totalCores, logUrlMap, attributes, resourcesInfo,
      resourceProfileId)
    state.filter(_ != null).map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
