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

package org.apache.spark.sql.catalyst.plans.physical

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * Specifies how tuples that share common expressions will be distributed when a query is executed
 * in parallel on many machines.
 *
 * Distribution here refers to inter-node partitioning of data. That is, it describes how tuples
 * are partitioned across physical machines in a cluster. Knowing this property allows some
 * operators (e.g., Aggregate) to perform partition local operations instead of global ones.
 */
//关注的是约束、事前的要求。它回答的是：“为了执行 Join，我的两个输入必须满足什么条件？
//示例： SortMergeJoinExec 算子的 requiredChildDistribution 要求左右输入都是 ClusteredDistribution(joinKeys)，即数据必须按照 Join 键分组到同一分区上
//它是 ShuffleExchangeExec 算子是否需要被插入的依据。当一个算子的输入数据的 Partitioning 不满足其 Distribution 要求时，Spark 优化器就会在两者之间插入一个 ShuffleExchangeExec
//Distribution（分布要求）：消费者（某个物理算子）对其输入数据如何分布/排序的逻辑要求（“我需要数据被按某种方式分布或排序”）
sealed trait Distribution {
  /**
   * The required number of partitions for this distribution. If it's None, then any number of
   * partitions is allowed for this distribution.
   */
  def requiredNumPartitions: Option[Int] //表示该分布所要求的分区数。如果为 None，则表示分区数没有严格要求，任何分区数都可以接受

  /**
   * Creates a default partitioning for this distribution, which can satisfy this distribution while
   * matching the given number of partitions.
   */
  //根据指定的分区数，生成一个与分布相匹配的 Partitioning
  def createPartitioning(numPartitions: Int): Partitioning
}

/**
 * Represents a distribution where no promises are made about co-location of data.
 */
// 表示没有对数据分布有具体的要求
case object UnspecifiedDistribution extends Distribution {
  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    throw new IllegalStateException("UnspecifiedDistribution does not have default partitioning.")
  }
}

/**
 * Represents a distribution that only has a single partition and all tuples of the dataset
 * are co-located.
 */
//表示所有的数据都在一个分区
case object AllTuples extends Distribution {
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1, "The default partitioning of AllTuples can only have 1 partition.")
    SinglePartition
  }
}

/**
 * Represents data where tuples that share the same values for the `clustering`
 * [[Expression Expressions]] will be co-located in the same partition.
 *
 * @param requireAllClusterKeys When true, `Partitioning` which satisfies this distribution,
 *                              must match all `clustering` expressions in the same ordering.
 */
// 表示数据要按照clustering表达式集合进行分布
// 在这种分布方式下，相同的 clustering 值的元组会被放置到同一个分区中
case class ClusteredDistribution(
    clustering: Seq[Expression], //表示决定数据如何在分区中分布的列或字段
    requireAllClusterKeys: Boolean = SQLConf.get.getConf(
      SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_DISTRIBUTION), //用来决定是否要求分区必须按照 clustering 表达式的顺序完全匹配
    requiredNumPartitions: Option[Int] = None) extends Distribution { //表示该分布所需的分区数
  require(
    clustering != Nil,
    "The clustering expressions of a ClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")
  //数据会根据 clustering 表达式进行哈希分区
  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(requiredNumPartitions.isEmpty || requiredNumPartitions.get == numPartitions,
      s"This ClusteredDistribution requires ${requiredNumPartitions.get} partitions, but " +
        s"the actual number of partitions is $numPartitions.")
    HashPartitioning(clustering, numPartitions)
  }

  /**
   * Checks if `expressions` match all `clustering` expressions in the same ordering.
   *
   * `Partitioning` should call this to check its expressions when `requireAllClusterKeys`
   * is set to true.
   */
    //检查当前的 expressions 是否与 clustering 表达式完全匹配
  def areAllClusterKeysMatched(expressions: Seq[Expression]): Boolean = {
    expressions.length == clustering.length &&
      expressions.zip(clustering).forall {
        case (l, r) => l.semanticEquals(r)
      }
  }
}

/**
 * Represents the requirement of distribution on the stateful operator in Structured Streaming.
 *
 * Each partition in stateful operator initializes state store(s), which are independent with state
 * store(s) in other partitions. Since it is not possible to repartition the data in state store,
 * Spark should make sure the physical partitioning of the stateful operator is unchanged across
 * Spark versions. Violation of this requirement may bring silent correctness issue.
 *
 * Since this distribution relies on [[HashPartitioning]] on the physical partitioning of the
 * stateful operator, only [[HashPartitioning]] (and HashPartitioning in
 * [[PartitioningCollection]]) can satisfy this distribution.
 * When `_requiredNumPartitions` is 1, [[SinglePartition]] is essentially same as
 * [[HashPartitioning]], so it can satisfy this distribution as well.
 *
 * NOTE: This is applied only to stream-stream join as of now. For other stateful operators, we
 * have been using ClusteredDistribution, which could construct the physical partitioning of the
 * state in different way (ClusteredDistribution requires relaxed condition and multiple
 * partitionings can satisfy the requirement.) We need to construct the way to fix this with
 * minimizing possibility to break the existing checkpoints.
 *
 * TODO(SPARK-38204): address the issue explained in above note.
 */
//特定于结构化流处理（Structured Streaming）中的分布类型，表示在流式计算中的有状态操作（如流式连接）所需的分布方式
case class StatefulOpClusteredDistribution(
    expressions: Seq[Expression], //用于哈希分区的表达式的序列
    _requiredNumPartitions: Int) extends Distribution { //表示所需的分区数，该值在流式操作的物理执行中是固定的，并且不能发生变化
  require(
    expressions != Nil,
    "The expressions for hash of a StatefulOpClusteredDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override val requiredNumPartitions: Option[Int] = Some(_requiredNumPartitions)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(_requiredNumPartitions == numPartitions,
      s"This StatefulOpClusteredDistribution requires ${_requiredNumPartitions} " +
        s"partitions, but the actual number of partitions is $numPartitions.")
    HashPartitioning(expressions, numPartitions)
  }
}

/**
 * Represents data where tuples have been ordered according to the `ordering`
 * [[Expression Expressions]]. Its requirement is defined as the following:
 *   - Given any 2 adjacent partitions, all the rows of the second partition must be larger than or
 *     equal to any row in the first partition, according to the `ordering` expressions.
 *
 * In other words, this distribution requires the rows to be ordered across partitions, but not
 * necessarily within a partition.
 */
// 表示数据要按照ordering表达排序
// 它的要求是相邻的分区之间，第二个分区的所有行必须大于或等于第一个分区中的任何一行，排序是基于 ordering 表达式
case class OrderedDistribution(ordering: Seq[SortOrder]) extends Distribution {
  require(
    ordering != Nil,
    "The ordering expressions of an OrderedDistribution should not be Nil. " +
      "An AllTuples should be used to represent a distribution that only has " +
      "a single partition.")

  override def requiredNumPartitions: Option[Int] = None

  override def createPartitioning(numPartitions: Int): Partitioning = {
    RangePartitioning(ordering, numPartitions)
  }
}

/**
 * Represents data where tuples are broadcasted to every node. It is quite common that the
 * entire set of tuples is transformed into different data structure.
 */
//要求数据是广播分布
case class BroadcastDistribution(mode: BroadcastMode) extends Distribution {
  override def requiredNumPartitions: Option[Int] = Some(1)

  override def createPartitioning(numPartitions: Int): Partitioning = {
    assert(numPartitions == 1,
      "The default partitioning of BroadcastDistribution can only have 1 partition.")
    BroadcastPartitioning(mode)
  }
}

/**
 * Describes how an operator's output is split across partitions. It has 2 major properties:
 *   1. number of partitions.
 *   2. if it can satisfy a given distribution.
 */
//关注的是静态、事后的组织结构。它回答的是：“这个数据集现在是什么样子的？”
//示例： 如果一个 DataFrame 经过 repartition(10, 'key') 操作，那么它的 outputPartitioning 就是一个 HashPartitioning，分区数为 10，分区键是 'key'。
//它是 ShuffleSpec 的输出目标：Shuffle 操作的目标就是产生一个新的 Partitioning 状态。
//它是 Distribution 的实际现状：一个数据集当前的 Partitioning 状态，用于判断它是否满足下一个算子的 Distribution 要求。
//Partitioning（分区属性）：一个物理子算子/节点实际提供给上游的数据分区方式与分区数（“我产出的数据是怎么划分的”），用于判断是否满足某个 Distribution。
//核心作用是：
//量化并行度： 通过 numPartitions 属性，确定了数据集可以并行处理的最大任务数。
//约束检查（核心）： 通过 satisfies(required: Distribution) 方法，允许 Spark 优化器判断当前数据集的结构（Partitioning）是否满足下一个操作所需的输入条件（Distribution）。这是避免不必要的 Shuffle 操作、优化执行效率的关键机制。
//生成 Shuffle 规范： 能够将自身的分区状态转化为 ShuffleSpec，用于指导 Join 等操作如何进行数据对齐。
trait Partitioning {
  /** Returns the number of partitions that the data is split across */
  val numPartitions: Int  //表示数据被划分成多少个分区

  /**
   * Returns true iff the guarantees made by this [[Partitioning]] are sufficient
   * to satisfy the partitioning scheme mandated by the `required` [[Distribution]],
   * i.e. the current dataset does not need to be re-partitioned for the `required`
   * Distribution (it is possible that tuples within a partition need to be reorganized).
   *
   * A [[Partitioning]] can never satisfy a [[Distribution]] if its `numPartitions` doesn't match
   * [[Distribution.requiredNumPartitions]].
   */
  //检查当前的 Partitioning 是否能够满足该分布的需求。
  final def satisfies(required: Distribution): Boolean = {
    required.requiredNumPartitions.forall(_ == numPartitions) && satisfies0(required)
    //forall 方法是一个高阶函数，它用于对 Option 中的值执行条件检查，并返回一个布尔值。
    // 具体来说，它会检查 Option 中是否包含某个值，并且该值是否满足给定的条件。如果 Option 为 None，则直接返回 true；
    // 如果 Option 为 Some(value)，则会检查该值是否满足给定的条件。如果条件成立，则返回 true，否则返回 false
  }

  /**
   * Creates a shuffle spec for this partitioning and its required distribution. The
   * spec is used in the scenario where an operator has multiple children (e.g., join), and is
   * used to decide whether this child is co-partitioned with others, therefore whether extra
   * shuffle shall be introduced.
   *
   * @param distribution the required clustered distribution for this partitioning
   */
  //用于在具有多个子节点的操作符（如 join）中创建一个 Shuffle 规范。
  // 这是为了决定当前子节点是否与其他子节点是共分区的，以及是否需要额外的 shuffle 操作
  def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    throw new IllegalStateException(s"Unexpected partitioning: ${getClass.getSimpleName}")

  /**
   * The actual method that defines whether this [[Partitioning]] can satisfy the given
   * [[Distribution]], after the `numPartitions` check.
   *
   * By default a [[Partitioning]] can satisfy [[UnspecifiedDistribution]], and [[AllTuples]] if
   * the [[Partitioning]] only have one partition. Implementations can also overwrite this method
   * with special logic.
   */
  protected def satisfies0(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true  //不指定具体的分区要求），则当前的分区方案总是满足要求，返回 true
    case AllTuples => numPartitions == 1  //所有的元组可以在同一分区，则仅当当前分区数为 1 时，返回 true
    case _ => false
  }
}
//分区数量已知，但数据的分布规律或组织方式是未知的。通常是加载数据后的初始状态。
case class UnknownPartitioning(numPartitions: Int) extends Partitioning

/**
 * Represents a partitioning where rows are distributed evenly across output partitions
 * by starting from a random target partition number and distributing rows in a round-robin
 * fashion. This partitioning is used when implementing the DataFrame.repartition() operator.
 */
 //表示数据从一个随机目标分区开始，以轮询方式均匀分配到各个分区。通常由用户显式调用 DataFrame.repartition() 但不指定分区键时生成。
case class RoundRobinPartitioning(numPartitions: Int) extends Partitioning

//表示整个数据集被单一分区处理的情况，数据没有被拆分成多个分区
case object SinglePartition extends Partitioning {
  val numPartitions = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case _: BroadcastDistribution => false //返回 false，因为广播分布要求数据在多个节点之间进行分配，无法满足只有一个分区的情况
    case _ => true
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    SinglePartitionShuffleSpec
}
//最常见的按键分区方式。保证具有相同哈希键值的行必定在同一个分区
trait HashPartitioningLike extends Expression with Partitioning with Unevaluable {
  def expressions: Seq[Expression]  //表示与当前分区相关的表达式集合

  override def children: Seq[Expression] = expressions  //因为 expressions 是当前类中的关键属性，因此它也是该表达式的子节点
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: StatefulOpClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
          if (requireAllClusterKeys) {
            // Checks `HashPartitioning` is partitioned on exactly same clustering keys of
            // `ClusteredDistribution`.
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }
}

/**
 * Represents a partitioning where rows are split up across partitions based on the hash
 * of `expressions`.  All rows where `expressions` evaluate to the same values are guaranteed to be
 * in the same partition.
 *
 * Since [[StatefulOpClusteredDistribution]] relies on this partitioning and Spark requires
 * stateful operators to retain the same physical partitioning during the lifetime of the query
 * (including restart), the result of evaluation on `partitionIdExpression` must be unchanged
 * across Spark versions. Violation of this requirement may bring silent correctness issue.
 */
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends HashPartitioningLike {
  //expressions: 一个 Expression 类型的序列，表示用于哈希分区的表达式
  //numPartitions: 一个整数，表示分区的数量
  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    HashShuffleSpec(this, distribution)

  /**
   * Returns an expression that will produce a valid partition ID(i.e. non-negative and is less
   * than numPartitions) based on hashing expressions.
   */
  //它能够基于 expressions 中的值生成一个有效的分区 ID。该 ID 必须是非负的，并且小于分区数（numPartitions）
  def partitionIdExpression: Expression = Pmod(new Murmur3Hash(expressions), Literal(numPartitions))

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): HashPartitioning = copy(expressions = newChildren)
}

case class CoalescedBoundary(startReducerIndex: Int, endReducerIndex: Int)

/**
 * Represents a partitioning where partitions have been coalesced from a HashPartitioning into a
 * fewer number of partitions.
 */
case class CoalescedHashPartitioning(from: HashPartitioning, partitions: Seq[CoalescedBoundary])
  extends HashPartitioningLike {
  //from: 一个 HashPartitioning 实例，表示原始的哈希分区。
  // partitions: 一个 CoalescedBoundary 类型的序列，表示合并后的分区边界。
  // 每个 CoalescedBoundary 包含两个整数：startReducerIndex 和 endReducerIndex，表示合并后的分区范围
  override def expressions: Seq[Expression] = from.expressions

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    CoalescedHashShuffleSpec(from.createShuffleSpec(distribution), partitions)

  override val numPartitions: Int = partitions.length

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): CoalescedHashPartitioning =
    copy(from = from.copy(expressions = newChildren))
}

/**
 * Represents a partitioning where rows are split across partitions based on transforms defined
 * by `expressions`. `partitionValuesOpt`, if defined, should contain value of partition key(s) in
 * ascending order, after evaluated by the transforms in `expressions`, for each input partition.
 * In addition, its length must be the same as the number of input partitions (and thus is a 1-1
 * mapping). The `partitionValues` may contain duplicated partition values.
 *
 * For example, if `expressions` is `[years(ts_col)]`, then a valid value of `partitionValuesOpt` is
 * `[0, 1, 2]`, which represents 3 input partitions with distinct partition values. All rows
 * in each partition have the same value for column `ts_col` (which is of timestamp type), after
 * being applied by the `years` transform.
 *
 * On the other hand, `[0, 0, 1]` is not a valid value for `partitionValuesOpt` since `0` is
 * duplicated twice.
 *
 * @param expressions partition expressions for the partitioning.
 * @param numPartitions the number of partitions
 * @param partitionValues the values for the cluster keys of the distribution, must be
 *                        in ascending order.
 */
//键分组分区。
//用于描述基于转换表达式（如 years(ts_col)）进行分区的情况，常见于 Hive/Delta Lake 分区表的读取或 Bucket Join 场景。
case class KeyGroupedPartitioning(
    expressions: Seq[Expression], //定义了用于分区的转换函数
    numPartitions: Int,
    partitionValues: Seq[InternalRow] = Seq.empty) extends Partitioning {  //partitionValues 包含每个输入分区的分区值。这些值应按升序排列，并且与输入分区一一对应

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
          if (requireAllClusterKeys) {
            // Checks whether this partitioning is partitioned on exactly same clustering keys of
            // `ClusteredDistribution`.
            c.areAllClusterKeysMatched(expressions)
          } else {
            // We'll need to find leaf attributes from the partition expressions first.
            val attributes = expressions.flatMap(_.collectLeaves())
            attributes.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }

        case _ =>
          false
      }
    }
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    KeyGroupedShuffleSpec(this, distribution)

  lazy val uniquePartitionValues: Seq[InternalRow] = {
    partitionValues
        .map(InternalRowComparableWrapper(_, expressions))
        .distinct
        .map(_.row)
  }
}

object KeyGroupedPartitioning {
  def apply(
      expressions: Seq[Expression],
      partitionValues: Seq[InternalRow]): KeyGroupedPartitioning = {
    KeyGroupedPartitioning(expressions, partitionValues.size, partitionValues)
  }
  //检查表达式是否是支持的变换表达式或简单的引用类型。如果所有表达式都符合这些条件，则返回 true
  def supportsExpressions(expressions: Seq[Expression]): Boolean = {
    def isSupportedTransform(transform: TransformExpression): Boolean = {
      transform.children.size == 1 && isReference(transform.children.head)
    }

    @tailrec
    def isReference(e: Expression): Boolean = e match {
      case _: Attribute => true
      case g: GetStructField => isReference(g.child)
      case _ => false
    }

    expressions.forall {
      case t: TransformExpression if isSupportedTransform(t) => true
      case e: Expression if isReference(e) => true
      case _ => false
    }
  }
}

/**
 * Represents a partitioning where rows are split across partitions based on some total ordering of
 * the expressions specified in `ordering`.  When data is partitioned in this manner, it guarantees:
 * Given any 2 adjacent partitions, all the rows of the second partition must be larger than any row
 * in the first partition, according to the `ordering` expressions.
 *
 * This is a strictly stronger guarantee than what `OrderedDistribution(ordering)` requires, as
 * there is no overlap between partitions.
 *
 * This class extends expression primarily so that transformations over expression will descend
 * into its child.
 */
// 基于数据的总顺序（Total Ordering）进行分区，保证相邻分区之间的数据是严格有序且无重叠的。
case class RangePartitioning(ordering: Seq[SortOrder], numPartitions: Int)
  extends Expression with Partitioning with Unevaluable {
  //ordering: 这是一个 SortOrder 序列，定义了对数据进行排序的方式
  //numPartitions: 这是一个整数，表示数据分区的总数

  override def children: Seq[SortOrder] = ordering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case OrderedDistribution(requiredOrdering) =>
          // If `ordering` is a prefix of `requiredOrdering`:
          //   Let's say `ordering` is [a, b] and `requiredOrdering` is [a, b, c]. According to the
          //   RangePartitioning definition, any [a, b] in a previous partition must be smaller
          //   than any [a, b] in the following partition. This also means any [a, b, c] in a
          //   previous partition must be smaller than any [a, b, c] in the following partition.
          //   Thus `RangePartitioning(a, b)` satisfies `OrderedDistribution(a, b, c)`.
          //
          // If `requiredOrdering` is a prefix of `ordering`:
          //   Let's say `ordering` is [a, b, c] and `requiredOrdering` is [a, b]. According to the
          //   RangePartitioning definition, any [a, b, c] in a previous partition must be smaller
          //   than any [a, b, c] in the following partition. If there is a [a1, b1] from a previous
          //   partition which is larger than a [a2, b2] from the following partition, then there
          //   must be a [a1, b1 c1] larger than [a2, b2, c2], which violates RangePartitioning
          //   definition. So it's guaranteed that, any [a, b] in a previous partition must not be
          //   greater(i.e. smaller or equal to) than any [a, b] in the following partition. Thus
          //   `RangePartitioning(a, b, c)` satisfies `OrderedDistribution(a, b)`.
          val minSize = Seq(requiredOrdering.size, ordering.size).min
          requiredOrdering.take(minSize) == ordering.take(minSize)
        case c @ ClusteredDistribution(requiredClustering, requireAllClusterKeys, _) =>
          val expressions = ordering.map(_.child)
          if (requireAllClusterKeys) {
            // Checks `RangePartitioning` is partitioned on exactly same clustering keys of
            // `ClusteredDistribution`.
            c.areAllClusterKeysMatched(expressions)
          } else {
            expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
          }
        case _ => false
      }
    }
  }

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec =
    RangeShuffleSpec(this.numPartitions, distribution)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): RangePartitioning =
    copy(ordering = newChildren.asInstanceOf[Seq[SortOrder]])
}

/**
 * A collection of [[Partitioning]]s that can be used to describe the partitioning
 * scheme of the output of a physical operator. It is usually used for an operator
 * that has multiple children. In this case, a [[Partitioning]] in this collection
 * describes how this operator's output is partitioned based on expressions from
 * a child. For example, for a Join operator on two tables `A` and `B`
 * with a join condition `A.key1 = B.key2`, assuming we use HashPartitioning schema,
 * there are two [[Partitioning]]s can be used to describe how the output of
 * this Join operator is partitioned, which are `HashPartitioning(A.key1)` and
 * `HashPartitioning(B.key2)`. It is also worth noting that `partitionings`
 * in this collection do not need to be equivalent, which is useful for
 * Outer Join operators.
 */
//分区集合。
//用于描述一个操作符（如 Join）的输出可以同时以多种方式被分区的情况。例如，Inner Join 的输出可以同时被左表的键和右表的键来描述分区
case class PartitioningCollection(partitionings: Seq[Partitioning]) //包含多个 Partitioning 对象的集合，描述了操作符输出的多个分区方式
  extends Expression with Partitioning with Unevaluable {

  require(  //要求所有分区的分区数量一致
    partitionings.map(_.numPartitions).distinct.length == 1,
    s"PartitioningCollection requires all of its partitionings have the same numPartitions.")
  //返回 partitionings 中的所有 Expression 子节点
  override def children: Seq[Expression] = partitionings.collect {
    case expr: Expression => expr
  }
  //分区表达式本身不会包含 null 值
  override def nullable: Boolean = false
  //表示该分区方式的输出数据类型是整数
  override def dataType: DataType = IntegerType
  //PartitioningCollection 的分区数
  override val numPartitions = partitionings.map(_.numPartitions).distinct.head

  /**
   * Returns true if any `partitioning` of this collection satisfies the given
   * [[Distribution]].
   */ //方法检查 partitionings 集合中的任何分区方式是否满足给定的 Distribution 要求
  override def satisfies0(required: Distribution): Boolean =
    partitionings.exists(_.satisfies(required))

  override def createShuffleSpec(distribution: ClusteredDistribution): ShuffleSpec = {
    val filtered = partitionings.filter(_.satisfies(distribution))
    ShuffleSpecCollection(filtered.map(_.createShuffleSpec(distribution)))
  }

  override def toString: String = {
    partitionings.map(_.toString).mkString("(", " or ", ")")
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): PartitioningCollection =
    super.legacyWithNewChildren(newChildren).asInstanceOf[PartitioningCollection]
}

/**
 * Represents a partitioning where rows are collected, transformed and broadcasted to each
 * node in the cluster.
 */
//表示数据已被收集、转换并广播到集群中的每个节点，是 Broadcast Join 的输出分区状态。
case class BroadcastPartitioning(mode: BroadcastMode) extends Partitioning {
  override val numPartitions: Int = 1

  override def satisfies0(required: Distribution): Boolean = required match {
    case UnspecifiedDistribution => true
    case BroadcastDistribution(m) if m == mode => true
    case _ => false
  }
}

/**
 * This is used in the scenario where an operator has multiple children (e.g., join) and one or more
 * of which have their own requirement regarding whether its data can be considered as
 * co-partitioned from others. This offers APIs for:
 *
 *   - Comparing with specs from other children of the operator and check if they are compatible.
 *      When two specs are compatible, we can say their data are co-partitioned, and Spark will
 *      potentially be able to eliminate shuffle if necessary.
 *   - Creating a partitioning that can be used to re-partition another child, so that to make it
 *      having a compatible partitioning as this node.
 */
// 关注的是动作、过程的蓝图。它回答的是：“如果需要 Shuffle，具体应该怎么做？”
// ShuffleSpec 结合了所需的分区键和分区器类型，为数据重分布提供了完整的执行方案。它定义了如何将前一个阶段的数据（Map 端）高效地写入磁盘，以便后一个阶段（Reduce 端）能够正确地读取
//Spark SQL 物理执行计划中用于精确描述数据洗牌（Shuffle）操作的规范
//核心作用在于：
//定义分区数量： 明确 Shuffle 后数据将被分成多少个分区。
//判断兼容性（isCompatibleWith）： 这是最关键的作用。它提供了一种机制，用于判断两个数据集的当前分区方式是否足够相似，以至于在执行 Join、Coalesce 等需要数据对齐的操作时，可以跳过昂贵的数据重分布（Shuffle）步骤，
// 从而实现性能优化。如果两个 Shuffle 规范兼容，则认为它们是 "Co-partitioned" (共同分区) 的
//提供分区创建能力（canCreatePartitioning/createPartitioning）： 在需要进行 Shuffle 对齐但当前分区不兼容时，提供了一种基于当前规范为另一侧数据创建兼容的新分区方案的能力
trait ShuffleSpec {
  /**
   * Returns the number of partitions of this shuffle spec
   */
  //描述的分区数量。
  //返回此 Shuffle 规范执行后，数据集将被划分成的分区总数。
  def numPartitions: Int

  /**
   * Returns true iff this spec is compatible with the provided shuffle spec.
   *
   * A true return value means that the data partitioning from this spec can be seen as
   * co-partitioned with the `other`, and therefore no shuffle is required when joining the two
   * sides.
   *
   * Note that Spark assumes this to be reflexive, symmetric and transitive.
   */
  //检查兼容性。
  //判断当前 ShuffleSpec 是否与另一个 other 兼容。如果返回 true，则表示两个数据集的分区方式是共同对齐的，可以进行高效的 Join 等操作而无需再次 Shuffle。Spark 假定此关系具有自反性、对称性和传递性
  def isCompatibleWith(other: ShuffleSpec): Boolean

  /**
   * Whether this shuffle spec can be used to create partitionings for the other children.
   */
  //是否能创建对齐分区。
  //指示当前的 Shuffle 规范是否可以用来为其他子节点（即另一个输入数据集）创建兼容的 Partitioning 方案
  def canCreatePartitioning: Boolean

  /**
   * Creates a partitioning that can be used to re-partition the other side with the given
   * clustering expressions.
   *
   * This will only be called when:
   *  - [[isCompatibleWith]] returns false on the side where the `clustering` is from.
   */
  //创建对齐分区方案。
  //仅在不兼容且需要对齐时调用。 它基于当前的 Shuffle 规范（本侧）和另一个数据集的聚簇表达式（clustering），生成一个新的 Partitioning 方案，用于对另一侧数据进行重新分区，以实现数据对齐。
  def createPartitioning(clustering: Seq[Expression]): Partitioning =
    throw new UnsupportedOperationException("Operation unsupported for " +
        s"${getClass.getCanonicalName}")
}
//表示数据被强制放入单个分区的 Shuffle 规范。常用于 Broadcast Join 后的输入或需要进行本地聚合的场景
case object SinglePartitionShuffleSpec extends ShuffleSpec {
  //分区数量都是1就能兼容
  override def isCompatibleWith(other: ShuffleSpec): Boolean = {
    other.numPartitions == 1
  }

  override def canCreatePartitioning: Boolean = false

  override def createPartitioning(clustering: Seq[Expression]): Partitioning =
    SinglePartition

  override def numPartitions: Int = 1
}
// 表示基于**范围分区（RangePartitioning）**的 Shuffle 规范。
// 用于需要对数据进行排序的操作（如 SortMergeJoin 的排序阶段）
//numPartitions: 分区数量。
//distribution: 所需的聚簇分布 (ClusteredDistribution) 要求。
case class RangeShuffleSpec(
    numPartitions: Int,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  // `RangePartitioning` is not compatible with any other partitioning since it can't guarantee
  // data are co-partitioned for all the children, as range boundaries are randomly sampled. We
  // can't let `RangeShuffleSpec` to create a partitioning.
  // 大多数情况下返回 false。因为范围分区的边界是基于数据样本随机确定的，无法保证两个数据集的分区边界完全一致，因此通常不被认为是兼容或可用于创建对齐分区的
  override def canCreatePartitioning: Boolean = false

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec => numPartitions == 1
    case ShuffleSpecCollection(specs) => specs.exists(isCompatibleWith)
    // `RangePartitioning` is not compatible with any other partitioning since it can't guarantee
    // data are co-partitioned for all the children, as range boundaries are randomly sampled.
    case _ => false
  }
}
//最常见的 Shuffle 规范，表示基于**哈希分区（HashPartitioning）**的 Shuffle。它确保具有相同哈希键的数据行都被发送到同一个分区。
//partitioning: 具体的哈希分区方案 (HashPartitioning)，包含分区键表达式和分区数
//distribution: 所需的聚簇分布 (ClusteredDistribution) 要求。
case class HashShuffleSpec(
    partitioning: HashPartitioning,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  /**
   * A sequence where each element is a set of positions of the hash partition key to the cluster
   * keys. For instance, if cluster keys are [a, b, b] and hash partition keys are [a, b], the
   * result will be [(0), (1, 2)].
   *
   * This is useful to check compatibility between two `HashShuffleSpec`s. If the cluster keys are
   * [a, b, b] and [x, y, z] for the two join children, and the hash partition keys are
   * [a, b] and [x, z], they are compatible. With the positions, we can do the compatibility check
   * by looking at if the positions of hash partition keys from two sides have overlapping.
   */
  //用于快速检查兼容性。它记录了 HashPartitioning 的分区键在 ClusteredDistribution 的聚簇键序列中的位置
  lazy val hashKeyPositions: Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    partitioning.expressions.map(k => distKeyToPos.getOrElse(k.canonicalized, mutable.BitSet.empty))
  }
  //检查两个 HashShuffleSpec 是否满足四个条件：聚簇键数量相同、分区数量相同、哈希分区键数量相同，并且最关键的是，每一对哈希分区键在它们各自的聚簇键中必须有重叠的位置（通过 hashKeyPositions 的交集判断）。
  // 这确保了两个数据集虽然可能使用不同的列进行 Shuffle，但它们都有效地按照公共的 Join 键对齐了
  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec =>
      partitioning.numPartitions == 1
    case otherHashSpec @ HashShuffleSpec(otherPartitioning, otherDistribution) =>
      // we need to check:
      //  1. both distributions have the same number of clustering expressions
      //  2. both partitioning have the same number of partitions
      //  3. both partitioning have the same number of expressions
      //  4. each pair of partitioning expression from both sides has overlapping positions in their
      //     corresponding distributions.
      //表达式数量一致
      distribution.clustering.length == otherDistribution.clustering.length &&
      //分区数量一致
      partitioning.numPartitions == otherPartitioning.numPartitions &&
      partitioning.expressions.length == otherPartitioning.expressions.length && {
        val otherHashKeyPositions = otherHashSpec.hashKeyPositions
        hashKeyPositions.zip(otherHashKeyPositions).forall { case (left, right) =>
          left.intersect(right).nonEmpty
        }
      }
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ =>
      false
  }

  override def canCreatePartitioning: Boolean = {
    // To avoid potential data skew, we don't allow `HashShuffleSpec` to create partitioning if
    // the hash partition keys are not the full join keys (the cluster keys). Then the planner
    // will add shuffles with the default partitioning of `ClusteredDistribution`, which uses all
    // the join keys.
    if (SQLConf.get.getConf(SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION)) {
      distribution.areAllClusterKeysMatched(partitioning.expressions)
    } else {
      true
    }
  }
  //基于另一侧的聚簇键（clustering），创建一个新的 HashPartitioning 方案，以确保数据对齐
  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    val exprs = hashKeyPositions.map(v => clustering(v.head))
    HashPartitioning(exprs, partitioning.numPartitions)
  }

  override def numPartitions: Int = partitioning.numPartitions
}
//表示一个**合并（Coalesced）**后的哈希 Shuffle 规范。通常用于在 Shuffle 结果上执行 coalesce() 缩小分区数的场景，且不涉及完整 Shuffle
case class CoalescedHashShuffleSpec(
    from: ShuffleSpec,
    partitions: Seq[CoalescedBoundary]) extends ShuffleSpec {

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec =>
      numPartitions == 1
    case CoalescedHashShuffleSpec(otherParent, otherPartitions) =>
      partitions == otherPartitions && from.isCompatibleWith(otherParent)
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ =>
      false
  }

  override def canCreatePartitioning: Boolean = false

  override def numPartitions: Int = partitions.length
}
// 表示基于 Key-Grouped 分区（KeyGroupedPartitioning） 的 Shuffle 规范，
// 常用于 Bucket Join 或使用 Transform 表达式（如 bucket, years）进行分区对齐的场景
//partitioning: 具体的 Key-Grouped 分区方案
//distribution: 所需的聚簇分布要求。
case class KeyGroupedShuffleSpec(
    partitioning: KeyGroupedPartitioning,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  /**
   * A sequence where each element is a set of positions of the partition expression to the cluster
   * keys. For instance, if cluster keys are [a, b, b] and partition expressions are
   * [bucket(4, a), years(b)], the result will be [(0), (1, 2)].
   *
   * Note that we only allow each partition expression to contain a single partition key.
   * Therefore the mapping here is very similar to that from `HashShuffleSpec`.
   */
    //用于将分区表达式（可能包含 TransformExpression）映射到聚簇键的位置
  lazy val keyPositions: Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    partitioning.expressions.map { e =>
      val leaves = e.collectLeaves()
      assert(leaves.size == 1, s"Expected exactly one child from $e, but found ${leaves.size}")
      distKeyToPos.getOrElse(leaves.head.canonicalized, mutable.BitSet.empty)
    }
  }

  override def numPartitions: Int = partitioning.numPartitions
  // 聚簇键数量相同、分区数量相同、分区键表达式兼容（通过位置重叠和转换函数 TransformExpression 的一致性判断），以及分区值必须遵循相同的顺序。
  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    // Here we check:
    //  1. both distributions have the same number of clustering keys
    //  2. both partitioning have the same number of partitions
    //  3. partition expressions from both sides are compatible, which means:
    //    3.1 both sides have the same number of partition expressions
    //    3.2 for each pair of partition expressions at the same index, the corresponding
    //        partition keys must share overlapping positions in their respective clustering keys.
    //    3.3 each pair of partition expressions at the same index must share compatible
    //        transform functions.
    //  4. the partition values from both sides are following the same order.
    case otherSpec @ KeyGroupedShuffleSpec(otherPartitioning, otherDistribution) =>
      distribution.clustering.length == otherDistribution.clustering.length &&
        numPartitions == other.numPartitions && areKeysCompatible(otherSpec) &&
          partitioning.partitionValues.zip(otherPartitioning.partitionValues).forall {
            case (left, right) =>
              InternalRowComparableWrapper(left, partitioning.expressions)
                .equals(InternalRowComparableWrapper(right, partitioning.expressions))
          }
    case ShuffleSpecCollection(specs) =>
      specs.exists(isCompatibleWith)
    case _ => false
  }

  // Whether the partition keys (i.e., partition expressions) are compatible between this and the
  // `other` spec.
  def areKeysCompatible(other: KeyGroupedShuffleSpec): Boolean = {
    val expressions = partitioning.expressions
    val otherExpressions = other.partitioning.expressions

    expressions.length == otherExpressions.length && {
      val otherKeyPositions = other.keyPositions
      keyPositions.zip(otherKeyPositions).forall { case (left, right) =>
        left.intersect(right).nonEmpty
      }
    } && expressions.zip(otherExpressions).forall {
      case (l, r) => isExpressionCompatible(l, r)
    }
  }

  private def isExpressionCompatible(left: Expression, right: Expression): Boolean =
    (left, right) match {
      case (_: LeafExpression, _: LeafExpression) => true
      case (left: TransformExpression, right: TransformExpression) =>
        left.isSameFunction(right)
      case _ => false
    }

  override def canCreatePartitioning: Boolean = false
}
// 用于将多个可能的 ShuffleSpec 聚合在一起。例如，当一个数据集可能以多种方式满足下游算子的分布要求时
case class ShuffleSpecCollection(specs: Seq[ShuffleSpec]) extends ShuffleSpec {
  //检查 specs 中的每一个 ShuffleSpec 是否与 other 兼容，若其中任何一个 ShuffleSpec 兼容 other，就返回 true
  override def isCompatibleWith(other: ShuffleSpec): Boolean = {
    specs.exists(_.isCompatibleWith(other))
  }

  override def canCreatePartitioning: Boolean =
    specs.forall(_.canCreatePartitioning)

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    // as we only consider # of partitions as the cost now, it doesn't matter which one we choose
    // since they should all have the same # of partitions.
    //检查所有 specs 的 numPartitions 是否一致
    require(specs.map(_.numPartitions).toSet.size == 1, "expected all specs in the collection " +
      "to have the same number of partitions")
    specs.head.createPartitioning(clustering)
  }

  override def numPartitions: Int = {
    require(specs.nonEmpty, "expected specs to be non-empty")
    specs.head.numPartitions
  }
}
