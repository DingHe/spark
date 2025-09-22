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

import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.APP_CALLER_CONTEXT
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util._

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]]
 *  - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 * @param numPartitions Total number of partitions in the stage that this task belongs to.
 * @param artifacts list of artifacts (may be session-specific) of the job this task belongs to.
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 * @param isBarrier whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                  at the same time for a barrier stage.
 */

// Task 是 Spark 中最小的执行单元，是一个抽象基类，
// 代表了在执行器（Executor）上运行的单个计算任务。
// 一个 Spark 作业（Job）被分解为多个阶段（Stage），每个阶段又由一个或多个任务（Task）组成。Task 类及其子类封装了要在数据分片上执行的实际计算逻辑
// Spark 主要有两种类型的 Task：
// ShuffleMapTask：负责执行一个阶段（stage）的计算，并为下一个阶段的 Shuffle 操作准备数据。它的输出被划分为多个分区（buckets），并写入本地磁盘，等待下游任务拉取
// ResultTask：负责执行作业的最终阶段（final stage）。它执行计算并直接将结果返回给驱动程序（Driver）
private[spark] abstract class Task[T](
    val stageId: Int,  // 此任务所属阶段的 ID
    val stageAttemptId: Int, // 此任务所属阶段的尝试 ID
    val partitionId: Int, // 此任务处理的 RDD 分区的索引
    val numPartitions: Int, // 此任务所属阶段的总分区数
    val artifacts: JobArtifactSet, // 与此任务所属作业关联的工件集合，例如 JARs、文件和归档
    @transient var localProperties: Properties = new Properties, // 从驱动程序端复制过来的线程局部属性，例如 spark.sql.execution.id，这些属性在任务执行时可用
    // The default value is only used in tests.
    //用于在任务执行期间收集度量信息
    serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    // 此任务所属作业的 ID
    val jobId: Option[Int] = None,
    // 此任务所属应用程序的 ID
    val appId: Option[String] = None,
    // 此任务所属应用程序尝试的 ID
    val appAttemptId: Option[String] = None,
    //指示此任务是否属于一个屏障（barrier）阶段。屏障阶段要求所有任务同时启动并完成
    val isBarrier: Boolean = false) extends Serializable {

  @transient lazy val metrics: TaskMetrics =
    SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(serializedTaskMetrics))

  /**
   * Called by [[org.apache.spark.executor.Executor]] to run this task.
   *
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @param resources other host resources (like gpus) that this task attempt can access
   * @return the result of the task along with updates of Accumulators.
   */
  // Spark 在执行器上运行单个任务的核心入口。
  // 它负责设置任务的执行环境、处理任务上下文、执行实际的用户代码，并确保在任务完成后进行必要的清理工作
  final def run(
      taskAttemptId: Long, // 此任务尝试在 SparkContext 中唯一的标识符
      attemptNumber: Int, // 此任务尝试的次数（从0开始）
      metricsSystem: MetricsSystem, // 度量系统实例，用于报告任务的性能指标
      cpus: Int, //分配给此任务的 CPU 核心数
      resources: Map[String, ResourceInformation], // 可选的、此任务可访问的其他主机资源（如 GPU）
      plugins: Option[PluginContainer]): T = { // 可选的插件容器，允许在任务生命周期中执行自定义操作

    require(cpus > 0, "CPUs per task should be > 0")

    SparkEnv.get.blockManager.registerTask(taskAttemptId)
    // TODO SPARK-24874 Allow create BarrierTaskContext based on partitions, instead of whether
    // the stage is barrier.
    //TaskContext 是任务的上下文对象，它将任务的元数据、状态、内存管理器和度量系统封装在一起，并提供给任务的用户代码使用
    val taskContext = new TaskContextImpl(
      stageId,
      stageAttemptId, // stageAttemptId and stageAttemptNumber are semantically equal
      partitionId,
      taskAttemptId,
      attemptNumber,
      numPartitions,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics,
      cpus,
      resources)
    // 根据任务是否为屏障（barrier）阶段来创建不同的上下文对象
    context = if (isBarrier) {
      new BarrierTaskContext(taskContext)
    } else {
      taskContext
    }
    // 初始化一个线程局部变量，用于存储任务正在读取的输入文件的信息
    InputFileBlockHolder.initialize()
    //将新创建的 TaskContext 实例设置到当前线程的线程局部变量中
    TaskContext.setTaskContext(context)
    taskThread = Thread.currentThread()

    if (_reasonIfKilled != null) {
      kill(interruptThread = false, _reasonIfKilled)
    }

    // 创建一个 CallerContext 对象并将其设置为当前上下文。
    // CallerContext 包含了作业、应用程序、阶段和任务的详细信息，通常用于日志和调试
    new CallerContext(
      "TASK",
      SparkEnv.get.conf.get(APP_CALLER_CONTEXT),
      appId,
      appAttemptId,
      jobId,
      Option(stageId),
      Option(stageAttemptId),
      Option(taskAttemptId),
      Option(attemptNumber)).setCurrentContext()

    plugins.foreach(_.onTaskStart())
    //这是一个重要的 try-finally 块，确保无论任务是否成功或失败，finally 块中的清理代码都会被执行
    try {
      // 执行实际任务逻辑的地方
      // 会调用 TaskRunner 中实现的 runTask 抽象方法，该方法包含了用户定义的计算代码
      context.runTaskWithListeners(this)
    } finally {
      try {
        Utils.tryLogNonFatalError {
          // Release memory used by this thread for unrolling blocks
          SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
          SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(
            MemoryMode.OFF_HEAP)
          // Notify any tasks waiting for execution memory to be freed to wake up and try to
          // acquire memory again. This makes impossible the scenario where a task sleeps forever
          // because there are no other tasks left to notify it. Since this is safe to do but may
          // not be strictly necessary, we should revisit whether we can remove this in the
          // future.
          val memoryManager = SparkEnv.get.memoryManager
          memoryManager.synchronized { memoryManager.notifyAll() }
        }
      } finally {
        // Though we unset the ThreadLocal here, the context member variable itself is still
        // queried directly in the TaskRunner to check for FetchFailedExceptions.
        TaskContext.unset()
        InputFileBlockHolder.unset()
      }
    }
  }
  //用于管理任务执行期间使用的内存，特别是用于内存密集型操作（如排序、哈希聚合）的内存
  private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }
  // 子类实现真正运行业务逻辑的地方
  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskSetManager.
  //Map 输出跟踪器（MapOutputTracker）的纪元。由 TaskSetManager 设置，用于在 Shuffle 失败时帮助执行器刷新其缓存的 Map 输出状态
  var epoch: Long = -1

  // Task context, to be initialized in run()
  // 任务的上下文对象。在 run 方法中初始化，为任务提供对执行环境的访问，如任务 ID、内存管理器、度量系统、累加器等.
  @transient var context: TaskContext = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  // 执行此任务的实际线程引用。在 run 方法中初始化
  @volatile @transient private var taskThread: Thread = _

  // If non-null, this task has been killed and the reason is as specified. This is used in case
  // context is not yet initialized when kill() is invoked.
  @volatile @transient private var _reasonIfKilled: String = null

  protected var _executorDeserializeTimeNs: Long = 0
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * If defined, this task has been killed and this option contains the reason.
   */
  def reasonIfKilled: Option[String] = Option(_reasonIfKilled)

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run.
   */
  def executorDeserializeTimeNs: Long = _executorDeserializeTimeNs
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * Collect the latest values of accumulators used in this task. If the task failed,
   * filter out the accumulators whose values should not be included on failures.
   */
  // 收集此任务中使用的累加器的最新值。如果任务失败，它可以根据累加器的配置决定是否包含其值
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) {
      // Note: internal accumulators representing task metrics always count failed values
      context.taskMetrics.nonZeroInternalAccums() ++
        // zero value external accumulators may still be useful, e.g. SQLMetrics, we should not
        // filter them out.
        context.taskMetrics.withExternalAccums(_.filter(a => !taskFailed || a.countFailedValues))
    } else {
      Seq.empty
    }
  }

  /**
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   */
  // 它设置 _reasonIfKilled 标志，并通过 TaskContext 中断任务。
  // 如果 interruptThread 为 true，它会调用 Thread.interrupt() 来中断任务线程
  def kill(interruptThread: Boolean, reason: String): Unit = {
    require(reason != null)
    _reasonIfKilled = reason
    if (context != null) {
      context.markInterrupted(reason)
    }
    if (interruptThread && taskThread != null) {
      // interrupt() 不会强制终止线程，而是给线程设置一个“中断标志位”
      // 如果线程在 阻塞状态（例如 sleep()、wait()、join()），调用 interrupt() 会让它抛出 InterruptedException，从而可以提前退出
      // 如果线程在 运行状态（普通代码执行），调用 interrupt() 只是设置中断标志，线程需要自己检查标志决定是否退出
      taskThread.interrupt()
    }
  }
}
