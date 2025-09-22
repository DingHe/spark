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

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, ThreadUtils, Utils}

/**
 * Classes that represent cleaning tasks.
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask                //清理rdd
private case class CleanShuffle(shuffleId: Int) extends CleanupTask        //清理shuffle
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask   //清理广播变量
private case class CleanAccum(accId: Long) extends CleanupTask             //清理累计变量
private case class CleanCheckpoint(rddId: Int) extends CleanupTask         //清理检查点
private case class CleanSparkListener(listener: SparkListener) extends CleanupTask    //清理监听器

/**
 * A WeakReference associated with a CleanupTask.
 * 弱引用（Weak Reference）：只有在垃圾回收时，弱引用的对象才会被回收，无论内存是否充足
 * 与 ReferenceQueue 配合使用： WeakReference 通常与 ReferenceQueue 配合使用。
 * 当引用的对象被垃圾回收时，WeakReference 会被自动加入到 ReferenceQueue 中。这可以用于实现对象的清理工作
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)

/**
 * An asynchronous cleaner for RDD, shuffle, and broadcast state.
 *
 * This maintains a weak reference for each RDD, ShuffleDependency, and Broadcast of interest,
 * to be processed when the associated object goes out of scope of the application. Actual
 * cleanup is performed in a separate daemon thread.
 */
private[spark] class ContextCleaner(
    sc: SparkContext,
    shuffleDriverComponents: ShuffleDriverComponents) extends Logging {

  /**
   * A buffer to ensure that `CleanupTaskWeakReference`s are not garbage collected as long as they
   * have not been handled by the reference queue.
   */
  private val referenceBuffer =
    Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)  //缓存清理任务的集合

  private val referenceQueue = new ReferenceQueue[AnyRef] //管理与弱引用（WeakReference）关联的清理任务。当目标对象被垃圾回收时，相关的 CleanupTaskWeakReference 会自动加入到这个队列中

  private val listeners = new ConcurrentLinkedQueue[CleanerListener]() //监听器可以接收到清理任务的通知，如 RDD、shuffle、广播等资源被清除的事件

  private val cleaningThread = new Thread() { override def run(): Unit = keepCleaning() } //用于持续监听垃圾回收队列（referenceQueue），并执行清理任务

  private val periodicGCService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("context-cleaner-periodic-gc")  //定期触发垃圾回收的服务。通过定时调用 System.gc() 来触发垃圾回收

  /**
   * How often to trigger a garbage collection in this JVM.
   *
   * This context cleaner triggers cleanups only when weak references are garbage collected.
   * In long-running applications with large driver JVMs, where there is little memory pressure
   * on the driver, this may happen very occasionally or not at all. Not cleaning at all may
   * lead to executors running out of disk space after a while.
   */
  private val periodicGCInterval = sc.conf.get(CLEANER_PERIODIC_GC_INTERVAL)  //定义了垃圾回收的触发间隔，单位是秒

  /**
   * Whether the cleaning thread will block on cleanup tasks (other than shuffle, which
   * is controlled by the `spark.cleaner.referenceTracking.blocking.shuffle` parameter).
   *
   * Due to SPARK-3015, this is set to true by default. This is intended to be only a temporary
   * workaround for the issue, which is ultimately caused by the way the BlockManager endpoints
   * issue inter-dependent blocking RPC messages to each other at high frequencies. This happens,
   * for instance, when the driver performs a GC and cleans up all broadcast blocks that are no
   * longer in scope.
   */
    //控制在清理任务（如 RDD、广播等）执行时是否阻塞当前线程。默认情况下，它设置为 true，这是一种临时的解决方法，以避免高频次的阻塞 RPC 调用
  private val blockOnCleanupTasks = sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING)

  /**
   * Whether the cleaning thread will block on shuffle cleanup tasks.
   *
   * When context cleaner is configured to block on every delete request, it can throw timeout
   * exceptions on cleanup of shuffle blocks, as reported in SPARK-3139. To avoid that, this
   * parameter by default disables blocking on shuffle cleanups. Note that this does not affect
   * the cleanup of RDDs and broadcasts. This is intended to be a temporary workaround,
   * until the real RPC issue (referred to in the comment above `blockOnCleanupTasks`) is
   * resolved.
   */
    //控制是否在清理 shuffle 任务时阻塞当前线程。默认情况下，它不阻塞，以避免在清理 shuffle 时出现超时异常
  private val blockOnShuffleCleanupTasks =
    sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING_SHUFFLE)

  @volatile private var stopped = false
  //向 ContextCleaner 注册一个监听器，用于接收清理事件的通知（如 RDD、shuffle、广播等资源被清理的事件）
  /** Attach a listener object to get information of when objects are cleaned. */
  def attachListener(listener: CleanerListener): Unit = {
    listeners.add(listener)
  }

  /** Start the cleaner. */
  //启动清理线程 cleaningThread 和定时垃圾回收服务 periodicGCService。清理线程开始监听垃圾回收队列并执行清理任务
  def start(): Unit = {
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Context Cleaner")
    cleaningThread.start()
    periodicGCService.scheduleAtFixedRate(() => System.gc(),
      periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
  }

  /** 停止清理线程，等待清理线程完成当前任务后再退出
   * Stop the cleaning thread and wait until the thread has finished running its current task.
   */
  def stop(): Unit = {
    stopped = true
    // Interrupt the cleaning thread, but wait until the current task has finished before
    // doing so. This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkContext,
    // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
    synchronized {
      cleaningThread.interrupt()
    }
    cleaningThread.join()
    periodicGCService.shutdown()
  }

  /** Register an RDD for cleanup when it is garbage collected. */
  def registerRDDForCleanup(rdd: RDD[_]): Unit = {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit = {
    registerForCleanup(a, CleanAccum(a.id))
  }

  /** Register a ShuffleDependency for cleanup when it is garbage collected. */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]): Unit = {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Register a Broadcast for cleanup when it is garbage collected. */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]): Unit = {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** Register a RDDCheckpointData for cleanup when it is garbage collected. */
  def registerRDDCheckpointDataForCleanup[T](rdd: RDD[_], parentId: Int): Unit = {
    registerForCleanup(rdd, CleanCheckpoint(parentId))
  }

  /** Register a SparkListener to be cleaned up when its owner is garbage collected. */
  def registerSparkListenerForCleanup(
      listenerOwner: AnyRef,
      listener: SparkListener): Unit = {
    registerForCleanup(listenerOwner, CleanSparkListener(listener))
  }
  //注册一个cleanTask，当它被垃圾回收时会触发清理操作
  /** Register an object for cleanup. */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
  }

  /** Keep cleaning RDD, shuffle, and broadcast state. */
  private def keepCleaning(): Unit = Utils.tryOrStopSparkContext(sc) {
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            ref.task match {
              case CleanRDD(rddId) =>
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
              case CleanShuffle(shuffleId) =>
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
              case CleanBroadcast(broadcastId) =>
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
              case CleanAccum(accId) =>
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
              case CleanCheckpoint(rddId) =>
                doCleanCheckpoint(rddId)
              case CleanSparkListener(listener) =>
                doCleanSparkListener(listener)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** Perform RDD cleanup. */
  def doCleanupRDD(rddId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning RDD " + rddId)
      sc.unpersistRDD(rddId, blocking)
      listeners.asScala.foreach(_.rddCleaned(rddId))
      logDebug("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError("Error cleaning RDD " + rddId, e)
    }
  }

  /** Perform shuffle cleanup. */
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    try {
      if (mapOutputTrackerMaster.containsShuffle(shuffleId)) {
        logDebug("Cleaning shuffle " + shuffleId)
        // Shuffle must be removed before it's unregistered from the output tracker
        // to find blocks served by the shuffle service on deallocated executors
        shuffleDriverComponents.removeShuffle(shuffleId, blocking)
        mapOutputTrackerMaster.unregisterShuffle(shuffleId)
        listeners.asScala.foreach(_.shuffleCleaned(shuffleId))
        logDebug("Cleaned shuffle " + shuffleId)
      } else {
        logDebug("Asked to cleanup non-existent shuffle (maybe it was already removed)")
      }
    } catch {
      case e: Exception => logError("Error cleaning shuffle " + shuffleId, e)
    }
  }

  /** Perform broadcast cleanup. */
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean): Unit = {
    try {
      logDebug(s"Cleaning broadcast $broadcastId")
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.asScala.foreach(_.broadcastCleaned(broadcastId))
      logDebug(s"Cleaned broadcast $broadcastId")
    } catch {
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
  }

  /** Perform accumulator cleanup. */
  def doCleanupAccum(accId: Long, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning accumulator " + accId)
      AccumulatorContext.remove(accId)
      listeners.asScala.foreach(_.accumCleaned(accId))
      logDebug("Cleaned accumulator " + accId)
    } catch {
      case e: Exception => logError("Error cleaning accumulator " + accId, e)
    }
  }

  /**
   * Clean up checkpoint files written to a reliable storage.
   * Locally checkpointed files are cleaned up separately through RDD cleanups.
   */
  def doCleanCheckpoint(rddId: Int): Unit = {
    try {
      logDebug("Cleaning rdd checkpoint data " + rddId)
      ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
      listeners.asScala.foreach(_.checkpointCleaned(rddId))
      logDebug("Cleaned rdd checkpoint data " + rddId)
    }
    catch {
      case e: Exception => logError("Error cleaning rdd checkpoint data " + rddId, e)
    }
  }

  def doCleanSparkListener(listener: SparkListener): Unit = {
    try {
      logDebug(s"Cleaning Spark listener $listener")
      sc.listenerBus.removeListener(listener)
      logDebug(s"Cleaned Spark listener $listener")
    } catch {
      case e: Exception => logError(s"Error cleaning Spark listener $listener", e)
    }
  }

  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
 * Listener class used when any item has been cleaned by the Cleaner class.
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int): Unit
  def shuffleCleaned(shuffleId: Int): Unit
  def broadcastCleaned(broadcastId: Long): Unit
  def accumCleaned(accId: Long): Unit
  def checkpointCleaned(rddId: Long): Unit
}
