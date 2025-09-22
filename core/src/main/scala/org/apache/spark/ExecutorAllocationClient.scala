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

package org.apache.spark

import org.apache.spark.scheduler.ExecutorDecommissionInfo

/**
 * A client that communicates with the cluster manager to request or kill executors.
 * This is currently supported only in YARN mode.
 */
//用于与集群管理器（Cluster Manager）进行交互，负责请求、杀死或废弃执行器（executors）
private[spark] trait ExecutorAllocationClient {

  /** Get the list of currently active executors */
  private[spark] def getExecutorIds(): Seq[String]  //获取当前集群中所有活动执行器的ID列表

  /**
   * Whether an executor is active. An executor is active when it can be used to execute tasks
   * for jobs submitted by the application.
   *
   * @return whether the executor with the given ID is currently active.
   */
  def isExecutorActive(id: String): Boolean  //检查指定ID的执行器是否处于活动状态

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   *
   * @param resourceProfileIdToNumExecutors The total number of executors we'd like to have per
   *                                        ResourceProfile id. The cluster manager shouldn't kill
   *                                        any running executor to reach this number, but, if all
   *                                        existing executors were to die, this is the number
   *                                        of executors we'd want to be allocated.
   * @param numLocalityAwareTasksPerResourceProfileId The number of tasks in all active stages that
   *                                                  have a locality preferences per
   *                                                  ResourceProfile id. This includes running,
   *                                                  pending, and completed tasks.
   * @param hostToLocalTaskCount A map of ResourceProfile id to a map of hosts to the number of
   *                             tasks from all active stages that would like to like to run on
   *                             that host. This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  //向集群管理器请求总执行器数量
  //resourceProfileIdToNumExecutors 每个资源配置（ResourceProfile）需要的执行器数量。该信息帮助集群管理器根据每个资源配置要求分配执行器
  //numLocalityAwareTasksPerResourceProfileId 每个资源配置下的任务本地性偏好数量
  //hostToLocalTaskCount  表示每个主机上待运行任务的数量，以及这些任务的本地性要求
  private[spark] def requestTotalExecutors(
      resourceProfileIdToNumExecutors: Map[Int, Int],
      numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
      hostToLocalTaskCount: Map[Int, Map[String, Int]]): Boolean

  /**
   * Request an additional number of executors from the cluster manager for the default
   * ResourceProfile.
   * @return whether the request is acknowledged by the cluster manager.
   */
  //请求集群管理器为默认资源配置（ResourceProfile）分配额外的执行器
  //numAdditionalExecutors：需要请求的执行器数量
  def requestExecutors(numAdditionalExecutors: Int): Boolean

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * @param executorIds identifiers of executors to kill
   * @param adjustTargetNumExecutors whether the target number of executors will be adjusted down
   *                                 after these executors have been killed
   * @param countFailures if there are tasks running on the executors when they are killed, whether
    *                     to count those failures toward task failure limits
   * @param force whether to force kill busy executors, default false
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  //请求集群管理器杀死指定的执行器
  //executorIds：需要被杀死的执行器ID列表
  //adjustTargetNumExecutors：如果指定的执行器被杀死，是否调整目标执行器数量
  //countFailures：如果执行器上有任务在运行，是否将这些任务的失败计入任务失败限制。
  //force：是否强制杀死正在运行任务的执行器。默认为false
  def killExecutors(
    executorIds: Seq[String],
    adjustTargetNumExecutors: Boolean,
    countFailures: Boolean,
    force: Boolean = false): Seq[String]

  /**
   * Request that the cluster manager decommission the specified executors.
   * Default implementation delegates to kill, scheduler must override
   * if it supports graceful decommissioning.
   *
   * @param executorsAndDecomInfo identifiers of executors & decom info.
   * @param adjustTargetNumExecutors whether the target number of executors will be adjusted down
   *                                 after these executors have been decommissioned.
   * @param triggeredByExecutor whether the decommission is triggered at executor.
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
    //请求集群管理器退役指定的执行器。默认实现调用killExecutors方法，调度器需要在支持优雅废弃的情况下重写此方法
  def decommissionExecutors(
      executorsAndDecomInfo: Array[(String, ExecutorDecommissionInfo)],
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean): Seq[String] = {
    killExecutors(executorsAndDecomInfo.map(_._1),
      adjustTargetNumExecutors,
      countFailures = false)
  }


  /**
   * Request that the cluster manager decommission the specified executor.
   * Delegates to decommissionExecutors.
   *
   * @param executorId identifiers of executor to decommission
   * @param decommissionInfo information about the decommission (reason, host loss)
   * @param adjustTargetNumExecutors if we should adjust the target number of executors.
   * @param triggeredByExecutor whether the decommission is triggered at executor.
   *                            (TODO: add a new type like `ExecutorDecommissionInfo` for the
   *                            case where executor is decommissioned at executor first, so we
   *                            don't need this extra parameter.)
   * @return whether the request is acknowledged by the cluster manager.
   */
    //请求集群管理器废弃单个指定执行器
  final def decommissionExecutor(
      executorId: String,
      decommissionInfo: ExecutorDecommissionInfo,
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean = false): Boolean = {
    val decommissionedExecutors = decommissionExecutors(
      Array((executorId, decommissionInfo)),
      adjustTargetNumExecutors = adjustTargetNumExecutors,
      triggeredByExecutor = triggeredByExecutor)
    decommissionedExecutors.nonEmpty && decommissionedExecutors(0).equals(executorId)
  }

  /**
   * Request that the cluster manager decommission every executor on the specified host.
   *
   * @return whether the request is acknowledged by the cluster manager.
   */
  //请求集群管理器废弃指定主机上的所有执行器
  def decommissionExecutorsOnHost(host: String): Boolean

  /**
   * Request that the cluster manager kill every executor on the specified host.
   *
   * @return whether the request is acknowledged by the cluster manager.
   */
  //请求集群管理器杀死指定主机上的所有执行器
  def killExecutorsOnHost(host: String): Boolean

  /**
   * Request that the cluster manager kill the specified executor.
   * @return whether the request is acknowledged by the cluster manager.
   */
    //请求集群管理器杀死指定的执行器
  def killExecutor(executorId: String): Boolean = {
    val killedExecutors = killExecutors(Seq(executorId), adjustTargetNumExecutors = true,
      countFailures = false)
    killedExecutors.nonEmpty && killedExecutors(0).equals(executorId)
  }
}
