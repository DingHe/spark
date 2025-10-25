# 1、自适应执行（AQE）

在作业执行过程中根据实际数据量、分区大小和统计信息动态调整执行计划，从而提高性能、避免数据倾斜和 shuffle 过大等问题

- 动态分区裁剪（Dynamic Partition Pruning）

  在执行阶段，基于父查询结果裁剪 Hive/Parquet 分区。减少读取不必要的分区，提高查询效率。

- 动态分区合并（Shuffle Partition Coalescing）

  根据 shuffle 后每个分区的数据量动态合并小分区。避免大量小文件、减少 task 数量。

- 动态 join 策略切换（Dynamic Join Reordering / Shuffle Join Replacement）

  在执行阶段根据统计信息，将 Shuffle Join 改为 Broadcast Join 或者调整 join 顺序。避免 shuffle 过大、优化执行性能。

- 本地合并小文件（Local Shuffle Merge）

  合并 map 端输出的小文件，减少 shuffle 文件数量。

# 2、核心实现类

- AdaptiveSparkPlanExec

  AQE 的入口类，封装整个物理计划，负责根据统计信息动态修改计划。
  入口函数是doExecute函数
  
  - 为什么只处理Exchange、InMemoryTableScanLike和QueryStageExec三种类型的节点
  
  1. AQE 的主要目标不是优化每一个操作，而是将查询图分解成可独立执行的阶段，并在阶段完成后获取准确的统计信息。<br>
     只有 Exchange 节点才代表查询执行中的物理边界，适合作为阶段的划分点。
  2. Exchange (Shuffle 或 Broadcast)代表阶段边界，代表了数据在集群中移动和重新分布的物理边界。<br>
     这个边界之前的操作（上游计算）必须先完成才能启动数据交换。
  3. InMemoryTableScanLike代表已缓存数据,代表对用户显式缓存到内存中的数据进行读取。<br>
     读取缓存的数据也是一个独立的操作，其数据量是确定的。
  4. QueryStageExec代表已完成/进行中的阶段,表示其内部子树已经被封装为一个独立的执行阶段。
  5. 其他节点只进行递归调用 (case _) 的原因，对于所有其他操作（Project, Filter, Aggregate, Sort 等），它们代表了数据处理逻辑，而不是物理边界。

- QueryStageExec

  AQE 内部的执行阶段表示，表示一个 stage 的执行单元。

- ShuffleQueryStageExec

  对应 shuffle stage 的执行计划节点，AQE 会动态调整分区。

- BroadcastQueryStageExec

  对应 broadcast stage。AQE 可以在这里切换 join 策略。

- CoalescedPartitionSpec / PartialReducerPartitionSpec

  表示合并后的 shuffle 分区。

- SkewedPartitionSpec

  表示处理倾斜 key 的分区。

- ShuffledRowRDD

  通过ShuffleExchangeExec获取ShuffledRowRDD，ShuffledRowRDD是读取shuffle数据的rdd，通过不同的分区规范，创建不同的分区读取器

# 3、分区规范ShufflePartitionSpec决定Reduce端如何读取数据，那这个是如何创建的呢？

  - AdaptiveSparkPlanExec中执行对应的规则生成的（queryStageOptimizerRules方法和queryStagePreparationRules方法）

  -  OptimizeSkewedJoin
    
# 4、查询阶段QueryStageExec，表示一个可以独立执行的子图
  
   - 子类要实现doMaterialize方法，这个方法负责物化，物化的结果存储在_resultOption

   - BroadcastQueryStageExec

   - ShuffleQueryStageExec

# 5、什么条件才可以触发AQE

  - 由规则 InsertAdaptiveSparkPlan 确定

  - 而上面的规则 由 **QueryExecution** 执行，在**preparations** 中插入了这条规则

  - join的数据倾斜处理支持 **SortMergeJoin** 和 **ShuffledHashJoin**

  - 

# 7、参数
  
  - spark.sql.adaptive.enabled 决定是否开启自适应子查询，整个aqe的入口

  - spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes 控制认定数据倾斜的阀值

  - spark.sql.adaptive.skewJoin.skewedPartitionFactor 数据大小中值的因子

  - spark.sql.adaptive.skewJoin.enabled  决定是否启动join数据倾斜优化

  - spark.sql.adaptive.coalescePartitions.enabled 决定是否启动分区合并

  - spark.sql.adaptive.coalescePartitions.minPartitionNum 分区合并的最小分区数

  - spark.sql.adaptive.coalescePartitions.parallelismFirst 如何minPartitionNum没设置，并且此参数为true，则取session.sparkContext.defaultParallelism

  - spark.sql.adaptive.advisoryPartitionSizeInBytes  优化的目标分区大小

  - spark.sql.adaptive.customCostEvaluatorClass 可以配置自定义成本模型

# 6、测试类

  - AdaptiveQueryExecSuite