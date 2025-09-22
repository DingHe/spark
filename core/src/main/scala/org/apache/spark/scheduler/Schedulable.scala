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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 */
//可调度的实体的基础接口，描述调度池中每个实体的一些通用属性和方法，通常用于池管理、任务调度等场景
private[spark] trait Schedulable {
  var parent: Pool  //当前 Schedulable 实体的父池
  // child queues
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]  //存储子实体
  def schedulingMode: SchedulingMode  //FAIR：公平调度，每个实体按比例获得资源，FIFO：先入先出调度，按照添加顺序执行
  def weight: Int  //每个 Schedulable 实体都有一个权重值，调度时根据权重来分配资源，权重较高的实体通常会得到更多资源
  def minShare: Int //表示当前实体能够保证分配的最小资源份额
  def runningTasks: Int  //当前运行的任务数
  def priority: Int //优先级用于调度中决定哪个实体优先执行
  def stageId: Int //阶段 ID
  def name: String  //每个 Schedulable 实体都有一个名称，通过名称可以唯一标识该实体

  def isSchedulable: Boolean  //判断当前实体是否可以被调度
  def addSchedulable(schedulable: Schedulable): Unit  //向当前实体添加一个子实体。这个方法将一个新的 Schedulable 实体添加到 schedulableQueue 中，表示该实体的子任务或子池
  def removeSchedulable(schedulable: Schedulable): Unit  //从当前实体中移除一个子实体。这个方法会从 schedulableQueue 中删除指定的 Schedulable 实体
  def getSchedulableByName(name: String): Schedulable //通过名称查找 Schedulable 实体。该方法接受一个名称，返回名称匹配的 Schedulable 实体。如果找不到，通常返回 null
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit  //执行器丢失时，调用该方法通知该 Schedulable 实体执行器丢失的事件
  def executorDecommission(executorId: String): Unit  //当一个执行器被退役时，调用此方法通知该 Schedulable 实体处理执行器废弃的事件
  def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean  //检查是否存在可推测执行的任务。
  def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] //获取排序后的任务集队列。
}
