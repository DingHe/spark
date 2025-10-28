# 1、spark逻辑join转为物理join的规则

  - 执行这个操作的规则为`JoinSelection`规则

-  如果用户在 SQL 查询中使用了 Join Hints（例如 /*+ BROADCAST(table) */）， 优化器会严格按照以下优先级应用提示，并在满足 Join 类型要求时立即选中对应的策略

  | 优先级 | 提示类型 | 选中策略 | 决策细节 |
  |--------|-----------|-----------|-----------|
  | 1 | Broadcast Hint | 广播哈希连接 (BHJ) | 如果 Join 类型支持 BHJ，并且任一侧有广播提示，则选中。如果两侧都有广播提示，优化器会根据统计信息选择较小的一侧进行广播，以最大化效率。 |
  | 2 | Sort Merge Hint | Shuffle 排序合并连接 (SMJ) | 选中 SMJ，前提是 Join 键必须可排序（这是 SMJ 的基本要求）。 |
  | 3 | Shuffle Hash Hint | Shuffle 哈希连接 (SHJ) | 如果 Join 类型支持 SHJ，则选中。如果两侧都有 SHJ 提示，优化器会根据统计信息选择较小的一侧作为 Build Side（构建哈希表的一侧）。 |
  | 4 | Shuffle Replicate NL Hint | 笛卡尔积/Shuffle 嵌套循环连接 | 选中这个最昂贵的策略，前提是 Join 类型必须是内连接 (Inner like)。 |

  - 如果没有任何有效的 Join 提示，或者提示指定的策略不适用（例如，Join 键不可排序），优化器将依次尝试以下策略，直到找到第一个适用的方案

    | 序号 | Join 类型 | 适用场景 | 选择条件 |
    |------|------------|------------|------------|
    | 1 | **广播哈希连接 (BHJ)** | 一侧足够小 | 如果其中一侧的估计大小小于 `spark.sql.autoBroadcastJoinThreshold` 配置阈值，并且 Join 类型支持 BHJ，则选中。<br>如果两侧都足够小，则选择较小的一侧进行广播。 |
    | 2 | **Shuffle 哈希连接 (SHJ)** | 一侧小得多 | 当以下三个条件同时满足：<br>① 其中一侧足够小，可以在单个任务内存中构建本地哈希表（`canBuildLocalHashMapBySize`）；<br>② 满足条件的这一侧比另一侧小得多（通过 `SHUFFLE_HASH_JOIN_FACTOR` 判断）；<br>③ SQL 配置 `spark.sql.join.preferSortMergeJoin` 设置为 `false`（即不偏好 SMJ）。 |
    | 3 | **Shuffle 排序合并连接 (SMJ)** | 键可排序 | 如果 Join 键可排序（SMJ 的基本要求），则选中 SMJ。<br>SMJ 是等值连接的默认通用回退策略，因为它通常最稳定。 |
    | 4 | **笛卡尔积 / Shuffle 嵌套循环连接** | 内连接 | 仅当 Join 类型为内连接（Inner like）时选中此策略。 |
    | 5 | **广播嵌套循环连接 (BNLJ)** | 最终回退 | 作为最后的无奈选择，如果以上所有策略都不可用，则选中 BNLJ。<br>注：可能导致 OOM（内存溢出），但优化器别无选择时会使用。 |


## 1.1、 广播哈希连接 (Broadcast Hash Join, BHJ)

   - 仅支持等值连接 (Equi-joins)<br>
   - 连接键无需可排序（因为是基于哈希查找<br>
   - 支持除全外连接 (Full Outer Join) 外的所有 Join 类型<br>
   - 性能通常最快

## 1.2、 Shuffle 哈希连接 (Shuffle Hash Join)
  
   - 仅支持等值连接 (Equi-joins)<br>
   - 连接键无需可排序<br>
   - 支持所有 Join 类型<br>
   - 在连接之前，两侧数据都需要根据 Join 键进行 Shuffle。每个执行器会在其中一侧（Build Side）建立哈希表进行查找。 限制： 内存密集型操作。
   - 如果 Build Side（被用来构建哈希表的表）太大，可能导致执行器 OOM（内存溢出）

## 1.3、 Shuffle 排序合并连接 (Shuffle Sort Merge Join, SMJ)
 
   - 仅支持等值连接 (Equi-joins)<br>
   - 连接键必须可排序 (Sortable)<br>
   - 支持所有 Join 类型<br>
   - 这是最通用的 Join 策略。 它的过程分为三步： 1. Shuffle： 两侧数据都根据 Join 键进行 Shuffle。 2. Sort： Shuffle 后的每个分区内的数据根据 Join 键进行排序。 
   - 3. Merge： 扫描并合并两侧已排序的数据流，进行连接。<br>
   - spark.sql.join.preferSortMergeJoin 参数默认是true，表示系统偏好此算法

## 1.4、 广播嵌套循环连接 (Broadcast Nested Loop Join, BNLJ)

   - 支持等值连接和非等值连接 (Non-equi-joins)<br>
   - 支持所有 Join 类型，但对特定 Join 种类进行了优化。<br>
   - 将一侧表进行广播，另一侧的每个记录都与广播表中的所有记录进行连接条件检查（即嵌套循环）。 优化： 1. 右外连接 (Right Outer Join)： 优化为广播左侧。 2. 左外、左半、左反或存在连接 (Left/Semi/Anti/Existence Join)： 优化为广播右侧。 
   - 3. 内连接 (Inner-like Join)： 优化为广播任一侧。 限制： 如果 Join 类型或广播侧选择不满足上述优化条件，可能需要扫描数据多次，性能会非常慢

## 1.5、 Shuffle 并复制嵌套循环连接 (Shuffle-and-replicate Nested Loop Join)

   - 也称为 笛卡尔积连接 (Cartesian Product Join)<br>
   - 支持等值连接和非等值连接<br>
   - 这是最昂贵和最不推荐的 Join 策略，通常在没有任何连接键或条件时使用。它会通过 Shuffle 机制在各个执行器间复制数据，以确保每个分区都能完成笛卡尔积。如果数据量大，会产生天文数字的行数和极差的性能。


# 2、 可以通过hints指示spark采用哪种连接方法

   - hints的定义在 `JoinStrategyHint`


# 3、 hash表的构建

  - hash表构建的基类是 `HashedRelation`<br>
  - 主要有两个实现类`UnsafeHashedRelation`和`LongHashedRelation`，他们分别采用`BytesToBytesMap`和`LongToUnsafeRowMap`数据结构作为存储的后端。
  - 这两hash表采用的是Murmur3_x86_32算法