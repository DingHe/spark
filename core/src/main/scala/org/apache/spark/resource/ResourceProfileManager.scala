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

package org.apache.spark.resource

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.HashMap

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.Tests._
import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerResourceProfileAdded}
import org.apache.spark.util.Utils
import org.apache.spark.util.Utils.isTesting

/**
 * Manager of resource profiles. The manager allows one place to keep the actual ResourceProfiles
 * and everywhere else we can use the ResourceProfile Id to save on space.
 * Note we never remove a resource profile at this point. Its expected this number is small
 * so this shouldn't be much overhead.
 */
// 负责 管理和协调所有资源配置信息（ResourceProfile） 的核心组件
// 它的主要作用包括：
// 集中存储： 作为一个中央注册中心，它维护着应用中创建的所有 ResourceProfile 对象的映射。
// ID 映射： 允许在程序中使用紧凑的 资源配置文件 ID（Int） 来引用完整的 ResourceProfile 对象，从而节省内存空间
// 默认配置： 确保应用始终有一个 默认资源配置 可用
@Evolving
private[spark] class ResourceProfileManager(sparkConf: SparkConf,
    listenerBus: LiveListenerBus) extends Logging {
  //用于存储资源配置文件 ID (Int) 到对应的 ResourceProfile 对象的映射。它是该类的核心数据结构
  private val resourceProfileIdToResourceProfile = new HashMap[Int, ResourceProfile]()

  private val (readLock, writeLock) = {
    val lock = new ReentrantReadWriteLock()
    (lock.readLock(), lock.writeLock())
  }
  //应用启动时根据 SparkConf 创建的默认 ResourceProfile 实例。所有未指定配置的任务和执行器都将使用此配置
  private val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
  addResourceProfile(defaultProfile)

  def defaultResourceProfile: ResourceProfile = defaultProfile
  //检查 Spark 配置中是否启用了动态资源分配（Dynamic Allocation）
  private val dynamicEnabled = Utils.isDynamicAllocationEnabled(sparkConf)
  // 用于识别当前的 Spark 部署模式。这些标志用于执行资源配置的支持性检查。
  private val master = sparkConf.getOption("spark.master")
  private val isYarn = master.isDefined && master.get.equals("yarn")
  private val isK8s = master.isDefined && master.get.startsWith("k8s://")
  private val isStandaloneOrLocalCluster = master.isDefined && (
      master.get.startsWith("spark://") || master.get.startsWith("local-cluster")
    )
  private val notRunningUnitTests = !isTesting
  private val testExceptionThrown = sparkConf.get(RESOURCE_PROFILE_MANAGER_TESTING)

  /**
   * If we use anything except the default profile, it's supported on YARN, Kubernetes and
   * Standalone with dynamic allocation enabled, and task resource profile with dynamic allocation
   * disabled on Standalone. Throw an exception if not supported.
   */
    //核心支持性验证。
  // 检查给定的 ResourceProfile 是否在当前集群部署模式（YARN、K8s、Standalone）和动态分配设置下被支持。如果不支持，并且不在测试模式中，则抛出 SparkException
  private[spark] def isSupported(rp: ResourceProfile): Boolean = {
    // 如果是任务资源配置且动态分配禁用，且当前部署模式不是 Standalone、YARN 或 Kubernetes，并且当前不是跳过异常的测试场景，则抛出异常
    if (rp.isInstanceOf[TaskResourceProfile] && !dynamicEnabled) {
      if ((notRunningUnitTests || testExceptionThrown) &&
        !(isStandaloneOrLocalCluster || isYarn || isK8s)) {
        throw new SparkException("TaskResourceProfiles are only supported for Standalone, " +
          "Yarn and Kubernetes cluster for now when dynamic allocation is disabled.")
      }
    } else {
      // 检查当前 rp 是否不是默认资源配置
      val isNotDefaultProfile = rp.id != ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
      // 不是默认配置 部署模式不是 YARN、Kubernetes 或 Standalone/Local Cluster
      val notYarnOrK8sOrStandaloneAndNotDefaultProfile =
        isNotDefaultProfile && !(isYarn || isK8s || isStandaloneOrLocalCluster)
      // 不是默认配置 部署模式是 YARN、Kubernetes 或 Standalone/Local Cluster  动态资源分配被禁用
      val YarnOrK8sOrStandaloneNotDynAllocAndNotDefaultProfile =
        isNotDefaultProfile && (isYarn || isK8s || isStandaloneOrLocalCluster) && !dynamicEnabled

      // We want the exception to be thrown only when we are specifically testing for the
      // exception or in a real application. Otherwise in all other testing scenarios we want
      // to skip throwing the exception so that we can test in other modes to make testing easier.
      if ((notRunningUnitTests || testExceptionThrown) &&
        (notYarnOrK8sOrStandaloneAndNotDefaultProfile ||
          YarnOrK8sOrStandaloneNotDynAllocAndNotDefaultProfile)) {
        //确保只在生产或特定测试场景下抛出
        throw new SparkException("ResourceProfiles are only supported on YARN and Kubernetes " +
          "and Standalone with dynamic allocation enabled.")
      }

      if (isStandaloneOrLocalCluster && dynamicEnabled && rp.getExecutorCores.isEmpty &&
        sparkConf.getOption(config.EXECUTOR_CORES.key).isEmpty) {
        logWarning("Neither executor cores is set for resource profile, nor spark.executor.cores " +
          "is explicitly set, you may get more executors allocated than expected. " +
          "It's recommended to set executor cores explicitly. " +
          "Please check SPARK-30299 for more details.")
      }
    }

    true
  }

  /**
   * Check whether a task with specific taskRpId can be scheduled to executors
   * with executorRpId.
   *
   * Here are the rules:
   * 1. When dynamic allocation is disabled, only [[TaskResourceProfile]] is supported,
   *    and tasks with [[TaskResourceProfile]] can be scheduled to executors with default
   *    resource profile.
   * 2. For other scenarios(when dynamic allocation is enabled), tasks can be scheduled to
   *    executors where resource profile exactly matches.
   */
  // 检查具有 taskRpId 的任务是否可以被调度到具有 executorRpId 的执行器上。
  // 规则： 1. 动态分配启用时： 任务 ID 必须与执行器 ID 完全匹配。
  // 2. 动态分配禁用时： 具有 TaskResourceProfile 的任务可以被调度到使用默认资源配置的执行器上
  private[spark] def canBeScheduled(taskRpId: Int, executorRpId: Int): Boolean = {
    assert(resourceProfileIdToResourceProfile.contains(taskRpId) &&
      resourceProfileIdToResourceProfile.contains(executorRpId),
      "Tasks and executors must have valid resource profile id")
    val taskRp = resourceProfileFromId(taskRpId)

    // When dynamic allocation disabled, tasks with TaskResourceProfile can always reuse
    // all the executors with default resource profile.
    taskRpId == executorRpId || (!dynamicEnabled && taskRp.isInstanceOf[TaskResourceProfile])
  }

  //增加ResourceProfile
  def addResourceProfile(rp: ResourceProfile): Unit = {
    isSupported(rp)
    var putNewProfile = false
    writeLock.lock()
    try {
      if (!resourceProfileIdToResourceProfile.contains(rp.id)) {
        val prev = resourceProfileIdToResourceProfile.put(rp.id, rp)
        if (prev.isEmpty) putNewProfile = true
      }
    } finally {
      writeLock.unlock()
    }
    // do this outside the write lock only when we add a new profile
    if (putNewProfile) {
      // force the computation of maxTasks and limitingResource now so we don't have cost later
      rp.limitingResource(sparkConf)
      logInfo(s"Added ResourceProfile id: ${rp.id}")
      listenerBus.post(SparkListenerResourceProfileAdded(rp))
    }
  }

  /*
   * Gets the ResourceProfile associated with the id, if a profile doesn't exist
   * it returns the default ResourceProfile created from the application level configs.
   */
  // 根据ID获取ResourceProfile
  def resourceProfileFromId(rpId: Int): ResourceProfile = {
    readLock.lock()
    try {
      resourceProfileIdToResourceProfile.getOrElse(rpId,
        throw new SparkException(s"ResourceProfileId $rpId not found!")
      )
    } finally {
      readLock.unlock()
    }
  }

  /*
   * If the ResourceProfile passed in is equivalent to an existing one, return the
   * existing one, other return None
   */
  // 判断是否存在与rp相等的ResourceProfile，如果存在则返回存在的ResourceProfile
  def getEquivalentProfile(rp: ResourceProfile): Option[ResourceProfile] = {
    readLock.lock()
    try {
      resourceProfileIdToResourceProfile.find { case (_, rpEntry) =>
        rpEntry.resourcesEqual(rp)
      }.map(_._2)
    } finally {
      readLock.unlock()
    }
  }
}
