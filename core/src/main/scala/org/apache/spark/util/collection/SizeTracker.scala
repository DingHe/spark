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

package org.apache.spark.util.collection

import scala.collection.mutable

import org.apache.spark.util.SizeEstimator

/** 估算集合的大小（以字节为单位），并通过采样的方式来跟踪集合在更新过程中的变化
 * A general interface for collections to keep track of their estimated sizes in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator is somewhat expensive (order of a few milliseconds).
 */
private[spark] trait SizeTracker {

  import SizeTracker._

  /** 这个值决定了采样的速度，即采样点之间的间隔会随着 numUpdates 的增加而逐步增大。例如，增量的采样点会依次为 1, 2, 4, 8 等，这样可以减少频繁采样时的性能开销
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1
  //每次采样会记录当前集合的大小和更新次数
  /** Samples taken since last resetSamples(). Only the last two are kept for extrapolation. */
  private val samples = new mutable.Queue[Sample]
  //表示从上一个采样到当前采样之间每次更新的平均字节数
  /** The average number of bytes per update between our last two samples. */
  private var bytesPerUpdate: Double = _
  //从上次采样以来的更新次数
  /** Total number of insertions and updates into the map since the last resetSamples(). */
  private var numUpdates: Long = _
  //下一次采样应该进行的更新次数
  /** The value of 'numUpdates' at which we will take our next sample. */
  private var nextSampleNum: Long = _

  resetSamples()

  /** 采样的重置和初始化
   * Reset samples collected so far.
   * This should be called after the collection undergoes a dramatic change in size.
   */
  protected def resetSamples(): Unit = {
    numUpdates = 1
    nextSampleNum = 1
    samples.clear()
    takeSample()
  }

  /** 集合更新会调用此函数，当达到下次采样数量时，执行采样
   * Callback to be invoked after every update.
   */
  protected def afterUpdate(): Unit = {
    numUpdates += 1
    if (nextSampleNum == numUpdates) {
      takeSample()
    }
  }

  /**
   * Take a new sample of the current collection's size.
   */
  private def takeSample(): Unit = {
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates)) //首先使用 SizeEstimator.estimate(this) 来估算集合当前的大小，并将当前的样本（包括大小和更新次数）加入 samples 队列中
    // Only use the last two samples to extrapolate
    if (samples.size > 2) { //队列中保存的样本数量超过了 2 个
      samples.dequeue()
    }
    val bytesDelta = samples.toList.reverse match { //计算了最近两个样本之间每次更新的平均字节数
      case latest :: previous :: tail =>
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates) //最新样本的大小−前一个样本的大小 / 最新样本的更新次数−前一个样本的更新次数
      // If fewer than 2 samples, assume no change
      case _ => 0
    }
    bytesPerUpdate = math.max(0, bytesDelta)
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  /** 估算集合当前的大小（单位为字节）
   * Estimate the current size of the collection in bytes. O(1) time.
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private object SizeTracker {
  case class Sample(size: Long, numUpdates: Long)
}
