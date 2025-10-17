# Spark 读取 HDFS / Hive 数据时的分区机制

当 Spark 从 HDFS 或 Hive（Hive 元数据指向 HDFS 路径）读取数据时，**分区数量的决定权**在 Hadoop 的 `InputFormat` 和 Spark 配置之间。

---

## 1. 核心原则：HDFS 数据块（Block）决定初始分区数

对于存储在 HDFS 上的数据（Hive 表的底层存储），Spark 默认采用 **Hadoop 的输入分片（Input Split）机制** 来决定初始的读取分区数量。

### A. HDFS/Hive 数据的初始分区数

初始分区数量通常等于 **文件总大小 ÷ HDFS 块大小**（或可切分文件的切分大小），并受以下配置参数影响：

- `mapreduce.input.fileinputformat.split.minsize`  
  限制切分的最小大小（字节）
- `mapreduce.input.fileinputformat.split.maxsize`  
  限制切分的最大大小（最常用的控制分区数的配置）

**公式：**
Num Partitions = Total Size / Max(Block Size, Min Size)

### B. 针对 Parquet / ORC 文件的优化

对于 Hive/HDFS 中常见的列式存储格式（如 Parquet、ORC），Spark 还有额外的优化逻辑：

- **Row Group / Stripe 粒度切分**  
  Spark/Hadoop 通常以 Parquet 的 Row Group 或 ORC 的 Stripe 为单位创建输入分片，而不是严格遵循 HDFS 块。

- **文件内嵌索引信息**  
  Parquet/ORC 内部包含统计信息和索引，使 Spark 可以跳过不必要的 Row Group，实现更高效的分片和读取。

> spark 读取数据的入口类：`DataFrameReader`

## 2. Spark 的配置决定最终分区数

虽然初始分区数由 HDFS/Hadoop 决定，但 Spark 提供了自己的参数来影响或建议最终的分区数量。

### A. `spark.sql.files.minPartitionSize` — 建议的最小分区大小

该参数设置读取文件的最小分区大小（如 `128MB`）。  
它是一个建议值，不保证每个分区都恰好是这个大小。

### B. `spark.sql.files.maxPartitionBytes` — 核心控制参数

Spark SQL 中控制文件读取分区数量的最直接配置：

- 当读取大型文件时，Spark 会确保每个分区的大小不超过该值。
- **默认值：128MB**

**公式：** Num Partitions = Total Size / spark.sql.files.maxPartitionBytes

### C. 特殊情况：`sc.textFile()` 或 `spark.read.text()`

当使用 RDD 级别的 API 读取普通文本文件时，分区数量还会受到：

- `spark.default.parallelism` 配置的影响

但对于结构化数据（如 Parquet、Hive 表），通常以 `spark.sql.files.maxPartitionBytes` 为主导。


---

## 3. Spark 读取数据的逻辑路径

### SQL/Table API 入口

- `read.table` 首先解析为 `UnresolvedRelation`

#### 针对 V2 数据源
- 解析为 `BatchScanExec`，通过 `DataSourceV2Strategy`物理计划优化规则

- V2 接口的数据源需要实现 `TableProvider`，基于文件的数据源通用实现类`FileDataSourceV2`
 例如 `ParquetDataSourceV2`实现类
  - V2分片是通过`FilePartition`的方法`maxSplitBytes`计算
  - Parquet 实现类相关的配置属性在SQLConf类中以PARQUET开头

#### 针对 V1 数据源（传统/基于文件）
- 通过`DataSourceStrategy`物理计划优化规则转化为：
  - `FileScanExec`
  - `HadoopFsRelation`
  

## 4. Spark 读取 Hive 表的执行类

- 入口类：`org.apache.spark.sql.hive.execution.HiveTableScanExec`  
  内部持有 `HadoopTableReader`

### HadoopTableReader

- 最小分区数 = `mapreduce.job.maps` 与 `sparkContext.defaultMinPartitions` 的最大值
- **非分区表**  
  使用 `HadoopRDD` 或 `NewHadoopRDD`，实际的数据分片委托给具体的 Hadoop `InputFormat`。
- **分区表**  
  每个分区构建一个 `HadoopRDD` / `NewHadoopRDD`，最后使用 `UnionRDD` 合并所有分区。


## 5. Hadoop 文件读取机制

### 新接口位置
org.apache.hadoop.mapreduce

### 数据分片类型

- `InputSplit`
- `FileSplit`  
  包含文件路径、切片起始位置、长度、所在主机列表、是否在内存中
- `CombineFileSplit`  
  合并多个小文件为一个逻辑分片，包含路径数组、偏移量数组、长度数组
- `DBInputSplit`

### InputFormat 体系

- `InputFormat`  
  Hadoop 的输入格式抽象类


- `FileInputFormat`  
  影响分片大小的参数：
  - `mapreduce.input.fileinputformat.split.minsize`
  - `mapreduce.input.fileinputformat.split.maxsize`
  
  - 分片大小值根据三个参数决定，**本质上就是文件块的大小决定**
  ```java
  protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
      return Math.max(minSize, Math.min(maxSize, blockSize));
  }

- `TextInputFormat`

  重写了`isSplitable`接口，没有压缩格式或者压缩格式实现了`SplittableCompressionCodec`接口就是可拆分
  键是每行开头的偏移量，值是每行的内容分片大小，继承了`FileInputFormat`


- `NLineInputFormat`

    重写了split拆分的逻辑，按照每个split多少行来处理，参数是
    - mapreduce.input.lineinputformat.linespermap


- `CombineFileInputFormat`

   主要用于处理小文件的，把小文件合并成大小适中的split


- `CombineTextInputFormat`  

   TextInputFormat 的合并版本。它确保合并后的每个 Split 仍然能够以行为单位进行读取和处理


- `SequenceFileInputFormat` 

   Hadoop 特有的扁平化 (flat)、面向记录 (record-oriented) 的文件格式，它将键值对序列化成二进制格式存储


- `ParquetInputFormat`

   Parquet格式的Hadoop输入实现类，在Parquet-Java项目下。<br>
   Parquet 的切分机制是基于 Row Group（行组）粒度的，并且优先考虑数据本地性（Data Locality）和用户配置的最小/最大分片大小


   - 客户端元数据切分 (Client-Side Metadata Splitting) ，传统模式
 
     遍历 Row Group 列表，将连续的 Row Group 聚合成一个 SplitInfo 容器。<br>
     切分（即结束当前 Split，开始新的 Split）的决策是基于以下两个主要因素的： 
     1. 数据本地性优先： 如果当前 Row Group 跨越了 HDFS 块边界，并且当前聚合的 Split 已经达到了 minSplitSize，则进行切分，以确保这个 Split 的数据尽可能地保持在同一个 HDFS 节点上。
     2. 大小限制： 如果当前聚合的 Split 达到了 maxSplitSize，则强制切分。


   - 任务端元数据切分 (Task-Side Metadata Splitting) ，现代/默认模式<br>
     客户端只执行传统的`FileInputFormat`的切分逻辑，将文件粗略地切分成基于 HDFS 块边界 的 FileSplit<br>
     Task 端处理	当 Mapper 任务启动后，它接收一个 FileSplit，然后负责在该 Split 范围内读取 Parquet 元数据，并根据 Row Group 边界进一步细化或调整读取范围。

### 旧的接口

org.apache.hadoop.mapred


## 6. Hadoop 支持拆分的文件压缩格式

- `Bzip2`   
   
   Hadoop 可以直接识别 .bz2 文件，并根据 bzip2 的 block 结构来切分  缺点是压缩/解压速度相对 gzip 较慢。

- `LZO（带索引）`  

   原生 LZO 不可拆分，但 Hadoop-LZO 插件 + .lzo.index 文件可以实现切分。Hadoop 根据 index 将文件拆成多个 split

- Parquet / ORC with Snappy  

   虽然 .snappy 压缩文件不可拆分，但 Parquet / ORC 是列式文件，内部有 row group，每个 row group 可以被单独处理。因此 Spark/Hive 等框架可以并行读取一个大的 Parquet 文件（即使是 Snappy 压缩）


- Hadoop中实现SplittableCompressionCodec接口的只有BZip2Codec












