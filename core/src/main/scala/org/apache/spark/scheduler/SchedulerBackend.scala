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

package org.apache.spark.scheduler

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.storage.BlockManagerId

/**
 * A backend interface for scheduling systems that allows plugging in different ones under
 * TaskSchedulerImpl. We assume a Mesos-like model where the application gets resource offers as
 * machines become available and can launch tasks on them.
 */
//Spark 中用于调度的后端接口。该接口允许插件式地替换不同的调度系统
private[spark] trait SchedulerBackend {
  private val appId = "spark-application-" + System.currentTimeMillis  //应用程序的唯一标识符

  def start(): Unit  //启动调度器后端
  def stop(): Unit   //停止调度器后端
  def stop(exitCode: Int): Unit = stop()
  /**
   * Update the current offers and schedule tasks
   */
  def reviveOffers(): Unit  //更新当前的资源提供信息，并调度任务
  def defaultParallelism(): Int  //获取默认的并行度（即同时执行的任务数）

  /**
   * Requests that an executor kills a running task.
   *
   * @param taskId Id of the task.
   * @param executorId Id of the executor the task is running on.
   * @param interruptThread Whether the executor should interrupt the task thread.
   * @param reason The reason for the task kill.
   */
    //请求杀死一个正在运行的任务
  def killTask(
      taskId: Long, //任务 ID
      executorId: String, //执行器 ID
      interruptThread: Boolean, //是否中断任务线程
      reason: String): Unit =  //杀死任务的原因
    throw new UnsupportedOperationException

  def isReady(): Boolean = true //判断调度器是否已经准备好进行任务调度

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
   *
   * @return The application attempt id, if available.
   */
  def applicationAttemptId(): Option[String] = None  //获取应用程序的尝试 ID

  /**
   * Get the URLs for the driver logs. These URLs are used to display the links in the UI
   * Executors tab for the driver.
   * @return Map containing the log names and their respective URLs
   */
  def getDriverLogUrls: Option[Map[String, String]] = None  //获取驱动程序日志的 URL

  /**
   * Get the attributes on driver. These attributes are used to replace log URLs when
   * custom log url pattern is specified.
   * @return Map containing attributes on driver.
   */
  def getDriverAttributes: Option[Map[String, String]] = None //获取驱动程序的属性

  /**
   * Get the max number of tasks that can be concurrent launched based on the ResourceProfile
   * could be used, even if some of them are being used at the moment.
   * Note that please don't cache the value returned by this method, because the number can change
   * due to add/remove executors.
   *
   * @param rp ResourceProfile which to use to calculate max concurrent tasks.
   * @return The max number of tasks that can be concurrent launched currently.
   */
  def maxNumConcurrentTasks(rp: ResourceProfile): Int  //获取当前可并发启动的最大任务数

  /**
   * Get the list of host locations for push based shuffle
   *
   * Currently push based shuffle is disabled for both stage retry and stage reuse cases
   * (for eg: in the case where few partitions are lost due to failure). Hence this method
   * should be invoked only once for a ShuffleDependency.
   * @return List of external shuffle services locations
   */
  def getShufflePushMergerLocations(
      numPartitions: Int,
      resourceProfileId: Int): Seq[BlockManagerId] = Nil  //获取推送式 shuffle 的主机位置

}
