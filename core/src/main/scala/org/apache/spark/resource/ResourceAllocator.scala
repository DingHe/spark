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

import scala.collection.mutable

import org.apache.spark.SparkException

/**
 * Trait used to help executor/worker allocate resources.
 * Please note that this is intended to be used in a single thread.
 */
private[spark] trait ResourceAllocator {

  protected def resourceName: String  //资源的名称。比如，这可能是执行器的名称或者特定资源的标识符
  protected def resourceAddresses: Seq[String]  //每个地址代表一个资源位置，通常是网络地址或节点标识符
  protected def slotsPerAddress: Int    //每个地址可用的槽位数。一个地址可能有多个槽位，每个槽位代表一个可以分配的资源单元。

  /**
   * Map from an address to its availability, a value > 0 means the address is available,
   * while value of 0 means the address is fully assigned.
   *
   * For task resources ([[org.apache.spark.scheduler.ExecutorResourceInfo]]), this value
   * can be a multiple, such that each address can be allocated up to [[slotsPerAddress]]
   * times.
   */
    //用来存储资源地址与可用槽位数之间的关系。每个地址与它的可用槽位数关联。例如，如果一个地址有 2 个槽位可用，它会存储为 { "address1" -> 2 }
  private lazy val addressAvailabilityMap = {
    mutable.HashMap(resourceAddresses.map(_ -> slotsPerAddress): _*)
  }

  /**
   * Sequence of currently available resource addresses.
   *
   * With [[slotsPerAddress]] greater than 1, [[availableAddrs]] can contain duplicate addresses
   * e.g. with [[slotsPerAddress]] == 2, availableAddrs for addresses 0 and 1 can look like
   * Seq("0", "0", "1"), where address 0 has two assignments available, and 1 has one.
   */
    //它根据 addressAvailabilityMap 中每个地址的可用槽位数，生成一个包含地址的序列。每个地址会根据可用槽位的数量重复出现
  def availableAddrs: Seq[String] = addressAvailabilityMap
    .flatMap { case (addr, available) =>
      (0 until available).map(_ => addr)
    }.toSeq.sorted

  /**
   * Sequence of currently assigned resource addresses.
   *
   * With [[slotsPerAddress]] greater than 1, [[assignedAddrs]] can contain duplicate addresses
   * e.g. with [[slotsPerAddress]] == 2, assignedAddrs for addresses 0 and 1 can look like
   * Seq("0", "1", "1"), where address 0 was assigned once, and 1 was assigned twice.
   */
    //返回所有已分配的资源地址，并按槽位数量重复并排序
  private[spark] def assignedAddrs: Seq[String] = addressAvailabilityMap
    .flatMap { case (addr, available) =>
      (0 until slotsPerAddress - available).map(_ => addr)
    }.toSeq.sorted

  /**
   * Acquire a sequence of resource addresses (to a launched task), these addresses must be
   * available. When the task finishes, it will return the acquired resource addresses.
   * Throw an Exception if an address is not available or doesn't exist.
   */
    //尝试获取一系列资源地址。如果某个地址可用（即其槽位大于 0），则分配该地址的一个槽位并减少可用槽位数；如果地址不可用或不存在，则抛出异常
  def acquire(addrs: Seq[String]): Unit = {
    addrs.foreach { address =>
      //检查每个地址在 addressAvailabilityMap 中的可用槽位数
      val isAvailable = addressAvailabilityMap.getOrElse(address,
        throw new SparkException(s"Try to acquire an address that doesn't exist. $resourceName " +
        s"address $address doesn't exist."))
      if (isAvailable > 0) {
        //如果地址存在且有可用槽位，则将可用槽位数减 1
        addressAvailabilityMap(address) -= 1
      } else {
        throw new SparkException("Try to acquire an address that is not available. " +
          s"$resourceName address $address is not available.")
      }
    }
  }

  /**
   * Release a sequence of resource addresses, these addresses must have been assigned. Resource
   * addresses are released when a task has finished.
   * Throw an Exception if an address is not assigned or doesn't exist.
   */
    //释放一系列已分配的资源地址。如果某个地址已经分配了槽位（即其槽位数小于 slotsPerAddress），则将该地址的可用槽位数加 1；如果地址未分配或不存在，则抛出异常
  def release(addrs: Seq[String]): Unit = {
    addrs.foreach { address =>
      val isAvailable = addressAvailabilityMap.getOrElse(address,
        throw new SparkException(s"Try to release an address that doesn't exist. $resourceName " +
          s"address $address doesn't exist."))
      if (isAvailable < slotsPerAddress) {
        addressAvailabilityMap(address) += 1
      } else {
        throw new SparkException(s"Try to release an address that is not assigned. $resourceName " +
          s"address $address is not assigned.")
      }
    }
  }
}
