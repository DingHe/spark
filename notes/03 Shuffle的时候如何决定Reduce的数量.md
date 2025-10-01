# 概述

Shffule的Reduce数量 = 下游 ShuffleDependency使用的分区数(partitioner.numPartitions)，numPartitions有四个地方决定，按照优先级。

1） 算子里显式传入，如reduceByKey(numPartitions)、partitionBy(n)

2）默认并行度/配置决定，RDD 使用 sc.defaultParallelism，SQL/DataFrame 使用 spark.sql.shuffle.partitions，默认是200

2.1)spark.sql.shuffle.partitions参数在代码里面如何用？<br>
- org.apache.spark.sql.execution.exchange.ShuffleExchangeExec<br>
阅读sql层的类org.apache.spark.sql.execution.exchange.ShuffleExchangeExec，发现决定Shuffle后的分区数量
来自构建ShuffleExchangeExec时传入的Partitioning。

- EnsureRequirements<br>
阅读EnsureRequirements物理优化规则的ensureDistributionAndOrdering函数，发现在构建Partitioning类时，分区的数量由下面的规则决定。
  1. 优先取Distribution的requiredNumPartitions
  2. 如果启动spark.sql.adaptive.enabled和spark.sql.adaptive.coalescePartitions.enabled<br>
     则优先选择spark.sql.adaptive.coalescePartitions.initialPartitionNum
  3. 前面都没有设置，则取spark.sql.shuffle.partitions

2.2）Distribution的requiredNumPartitions是什么时候决定的？<br>
requiredNumPartitions 不是在运行时决定的，而是在物理计划生成阶段由不同算子决定。<br>
一些例子：  
Repartition / RepartitionByExpression：  
用户显式调用 df.repartition(n) / df.repartition(n, col("x"))。  
这里会在 physical planning 中生成一个 Distribution，带上 requiredNumPartitions = Some(n)

Aggregate / Join 等算子：  
一般不会要求固定的分区数（即 requiredNumPartitions = None）  
它们只要求“按照某些 key 分区”，但分区数可以由 Spark 默认值（spark.sql.shuffle.partitions）或 AQE 调整  


Range 分区排序（RangePartitioning）：  
requiredNumPartitions 也可能来自 spark.sql.shuffle.partitions 或 ORDER BY 的物理规则


3）开启SQL 的自适应执行（AQE）时可能被动态合并（coalesce）成更少的分区


# 调优建议

1） 显式设置：在关键 shuffle 算子处显式传入分区数，避免依赖默认。
RDD: rdd.reduceByKey(func, numPartitions) 或 rdd.partitionBy(new HashPartitioner(n))
DF: df.repartition(n) 或 df.repartition(col...)

2）调整默认并行度
spark.default.parallelism （影响 RDD API 的默认）
spark.sql.shuffle.partitions （影响 SQL/DataFrame）

3）启用 AQE
对于数据倾斜或 map 输出大小差异大、并且 initial partitions 过多的场景，开启 AQE 能减少 reduce 数量、合并小分区，常常带来性能提升

4）目标大小调优：若 AQE 合并后分区仍太大或太小，可调整
spark.sql.adaptive.shuffle.targetPostShuffleInputSize


# 常见误区

1） reduceByKey 不会自动根据数据量智能选择 partitions：除非显式传入或依赖 AQE，初始 partitions 是静态的（默认或你指定）

2） sc.defaultParallelism != spark.sql.shuffle.partitions：两者分别影响 RDD API 和 SQL API，分别调整更精确

3） AQE 不是在所有版本中行为完全相同：不同 Spark 版本对 AQE 的实现和默认参数可能变化，生产环境启用前需做 benchmark
