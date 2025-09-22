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
//定义了查询在执行时如何将数据划分到集群中的各个物理机器
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
//表示数据分布没有明确规定的分布类型。在 Spark 中，这意味着在进行分布式计算时，Spark 不对数据的分布方式做出任何保证。这通常用于没有明确分布要求的情况
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
//单分区分布
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
// 表示数据在分区中根据 clustering 表达式的值进行分布的分布类型。
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
//表示数据按给定的排序规则（ordering）分布。
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
//描述如何将操作符的输出数据分布到多个分区中的特征
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
//通常用于表示无法确定具体分区方案的情况，numPartitions，表示期望的分区数量，但不明确数据如何在这些分区中分布
case class UnknownPartitioning(numPartitions: Int) extends Partitioning

/**
 * Represents a partitioning where rows are distributed evenly across output partitions
 * by starting from a random target partition number and distributing rows in a round-robin
 * fashion. This partitioning is used when implementing the DataFrame.repartition() operator.
 */
 //采用轮询方式将数据均匀地分配到指定的分区中。它从一个随机的目标分区开始，并以轮询的方式将数据行分配到输出分区
 //通常用于 DataFrame 操作符 repartition()
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
//定义了与哈希分区相关的一些逻辑操作，尤其是针对 HashPartitioning 的分区要求和验证
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
//是一个表示哈希分区在经过合并后产生的新分区的类。
// 它继承自 HashPartitioningLike，并基于原始的 HashPartitioning 和合并后的分区边界信息来创建新的分区方案
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
//表示基于指定表达式的分区方案的类，其中行会根据 expressions 中定义的转换被划分到不同的分区。
// 它不仅包含分区的数量，还包括每个输入分区的分区键值（partitionValues）
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
//基于指定排序顺序对数据进行分区的类。数据在分区过程中会依据 ordering 中指定的排序规则进行划分，并且保证相邻分区之间的数据是严格有序的。
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
 *///表示多个分区方式的集合,用于描述一个物理操作符的输出分区方案，特别是在该操作符有多个子节点时
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
// 主要用于操作符（如连接操作符）具有多个子节点的情况，尤其是当其中一个或多个子节点对数据的分区方式有特殊要求时。
// 它提供了一些方法来检查分区是否兼容以及如何创建可以用于重新分区的方案
trait ShuffleSpec {
  /**
   * Returns the number of partitions of this shuffle spec
   */
  def numPartitions: Int  //描述的洗牌方案中的分区数量

  /**
   * Returns true iff this spec is compatible with the provided shuffle spec.
   *
   * A true return value means that the data partitioning from this spec can be seen as
   * co-partitioned with the `other`, and therefore no shuffle is required when joining the two
   * sides.
   *
   * Note that Spark assumes this to be reflexive, symmetric and transitive.
   */
  //当两个 ShuffleSpec 兼容时，表示它们的数据分区方式可以认为是共同分区的，因此在进行连接等操作时，Spark 可以跳过洗牌操作，避免不必要的数据传输
  def isCompatibleWith(other: ShuffleSpec): Boolean

  /**
   * Whether this shuffle spec can be used to create partitionings for the other children.
   */
  //表示当前的 ShuffleSpec 是否可以用来为其他子节点创建分区
  def canCreatePartitioning: Boolean

  /**
   * Creates a partitioning that can be used to re-partition the other side with the given
   * clustering expressions.
   *
   * This will only be called when:
   *  - [[isCompatibleWith]] returns false on the side where the `clustering` is from.
   */
    //创建一个新的分区方案（Partitioning），该方案可以用来重新分区另一侧的数据，使其与当前节点的数据分区方式兼容
    //当 isCompatibleWith 返回 false 时，才会调用此方法，目的是将当前节点的数据分区方式与另一个节点的数据分区方式对齐
  def createPartitioning(clustering: Seq[Expression]): Partitioning =
    throw new UnsupportedOperationException("Operation unsupported for " +
        s"${getClass.getCanonicalName}")
}

case object SinglePartitionShuffleSpec extends ShuffleSpec {
  override def isCompatibleWith(other: ShuffleSpec): Boolean = {
    other.numPartitions == 1
  }

  override def canCreatePartitioning: Boolean = false

  override def createPartitioning(clustering: Seq[Expression]): Partitioning =
    SinglePartition

  override def numPartitions: Int = 1
}
//专门用于处理 RangePartitioning 的 ShuffleSpec 类型。它用于表示通过范围分区方式（RangePartitioning）进行数据重分区时的分区信息和分布要求
case class RangeShuffleSpec(
    numPartitions: Int,
    distribution: ClusteredDistribution) extends ShuffleSpec {

  // `RangePartitioning` is not compatible with any other partitioning since it can't guarantee
  // data are co-partitioned for all the children, as range boundaries are randomly sampled. We
  // can't let `RangeShuffleSpec` to create a partitioning.
  override def canCreatePartitioning: Boolean = false

  override def isCompatibleWith(other: ShuffleSpec): Boolean = other match {
    case SinglePartitionShuffleSpec => numPartitions == 1
    case ShuffleSpecCollection(specs) => specs.exists(isCompatibleWith)
    // `RangePartitioning` is not compatible with any other partitioning since it can't guarantee
    // data are co-partitioned for all the children, as range boundaries are randomly sampled.
    case _ => false
  }
}
//常用于通过哈希算法对数据进行分区，从而实现数据的分布式处理
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
  lazy val hashKeyPositions: Seq[mutable.BitSet] = {
    val distKeyToPos = mutable.Map.empty[Expression, mutable.BitSet]
    distribution.clustering.zipWithIndex.foreach { case (distKey, distKeyPos) =>
      distKeyToPos.getOrElseUpdate(distKey.canonicalized, mutable.BitSet.empty).add(distKeyPos)
    }
    partitioning.expressions.map(k => distKeyToPos.getOrElse(k.canonicalized, mutable.BitSet.empty))
  }

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
      distribution.clustering.length == otherDistribution.clustering.length &&
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

  override def createPartitioning(clustering: Seq[Expression]): Partitioning = {
    val exprs = hashKeyPositions.map(v => clustering(v.head))
    HashPartitioning(exprs, partitioning.numPartitions)
  }

  override def numPartitions: Int = partitioning.numPartitions
}

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
//将多个 ShuffleSpec 聚集在一起的类型，它表示一个包含多个 ShuffleSpec 的集合
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
