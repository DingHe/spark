# 1.Shuffle的两大阶段

- Shuffle Write

  上游 stage（mapper），将每个 task 的输出按分区规则写入本地磁盘（或推送）

- Shuffle Read

  下游 stage（reducer）， 根据分区规则，从上游所有 task 拉取对应分区的数据

# 2.Spark3 Shuffle涉及的组件

- ShuffleManager

  抽象接口，定义 shuffle 的整体行为；默认实现是 SortShuffleManager

- ShuffleWriter

  mapper 端写数据的组件

- ShuffleReader

  reducer 端读数据的组件

- ExternalSorter

  用于 mapper 端对数据进行排序、分区、溢写磁盘等

- PushBasedShuffle

  Spark 3 新增，可将 mapper 输出直接推送给 reducer 缓存，减少 shuffle read 拉取延迟

# 3.Shuffle Write 过程（以 SortShuffleManager 为例）

ShuffleWriter有三个选择，分别如下：

- `BypassMergeSortShuffleWriter`

  **条件：**<br>
  1. 不是map端combine，如果是,则直接跳过
  2. 上游的分区数少于 spark.shuffle.sort.bypassMergeThreshold，默认是200
  
  **特性：**<br>
  当 shuffle 分区数较少，且 没有 map-side combine 操作 时，<br>
  Spark 会选择这种「绕过排序」的方式来写出 Shuffle 数据，以减少不必要的排序开销。<br>
  跳过 map 端的排序过程，直接把数据写入多个分区文件中，然后再做一次简单的文件合并

- `SerializedShuffleHandle`

  **条件：**<br>
  1. 序列化器支持对象的重定位 
  2. 不是map端combine，应为map端的combine通常需要反序列化数据
  3. 分区数不能超过 16777215

  **特性：**<br>
  常见的是 KryoSerializer 和 UnsafeRowSerializer 等支持这种特性

- `SortShuffleWriter`

  如果不走前面两个，最终就是走这个<br>
  在 Map 端对数据进行 对象级的排序（sort），支持可选的 聚合（combine），然后将结果写出为单个数据文件和索引文件，供 Reduce 端读取。<br>
  Spark 的 “兜底方案”，功能最全，适用范围最广
  
# 4.Spark 3 的重大改进：Push-Based Shuffle

在传统的 shuffle（SortShuffle 或 BypassShuffle）中：<br>
Map 任务完成后会将 shuffle 文件保存在本地<br>
Reduce 任务启动后，再从所有 map 节点“拉取”自己的分区文件。

Push-Based Shuffle 改进了这一点：<br>
Map 端完成后，会主动将 shuffle 数据 推送（push） 到 shuffle 服务；<br>
Shuffle 服务对相同 reduce 分区的数据进行 合并（merge），形成更少的文件；<br>
Reduce 阶段只需从 merge 结果中拉取（加上极少数未推送成功的 fallback 文件），从而减少连接数和压力。

？代码在哪里