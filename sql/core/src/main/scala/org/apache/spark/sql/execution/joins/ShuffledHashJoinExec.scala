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

package org.apache.spark.sql.execution.joins

import java.util.concurrent.TimeUnit._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
//执行 hash join 操作的一个类，尤其用于通过 shuffle 交换数据来连接两个子查询的记录
case class ShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,  //表示连接中用于构建哈希表的一方
    condition: Option[Expression],  //可选的连接条件，如果存在的话，它将限制匹配的行
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false)  //是否为倾斜连接，默认为 false
  extends HashJoin with ShuffledJoin {
  //numOutputRows: 输出的行数   buildDataSize: 构建哈希表的大小   buildTime: 构建哈希表所花费的时间
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def output: Seq[Attribute] = super[ShuffledJoin].output

  override def outputPartitioning: Partitioning = super[ShuffledJoin].outputPartitioning
  //对于外连接（如 FullOuter、LeftOuter、RightOuter），由于哈希关系的特殊处理，无法保证排序
  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For outer joins where the outer side is build-side, order cannot be guaranteed.
    // The algorithm performs an additional un-ordered iteration on build-side (HashedRelation)
    // to find unmatched rows to satisfy the outer join semantic.
    case FullOuter => Nil
    case LeftOuter if buildSide == BuildLeft => Nil
    case RightOuter if buildSide == BuildRight => Nil
    case _ => super.outputOrdering
  }

  // Exposed for testing
  @transient lazy val ignoreDuplicatedKey = joinType match {
    //如果连接类型为 LeftExistence，且连接条件只涉及流式输出和构建连接键，则会忽略重复的连接键
    case LeftExistence(_) =>
      // For building hash relation, ignore duplicated rows with same join keys if:
      // 1. Join condition is empty, or
      // 2. Join condition only references streamed attributes and build join keys.
      val streamedOutputAndBuildKeys = AttributeSet(streamedOutput ++ buildKeys)
      condition.forall(_.references.subsetOf(streamedOutputAndBuildKeys))
    case _ => false
  }

  /**  该方法用于构建哈希关系（HashedRelation），它会将输入数据迭代器转化为哈希表
   * This is called by generated Java class, should be public.
   */
  def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")
    val start = System.nanoTime()
    val context = TaskContext.get()
    val relation = HashedRelation(
      iter,
      buildBoundKeys,
      taskMemoryManager = context.taskMemoryManager(),
      // build-side or full outer join needs support for NULL key in HashedRelation.
      allowsNullKey = joinType == FullOuter ||
        (joinType == LeftOuter && buildSide == BuildLeft) ||
        (joinType == RightOuter && buildSide == BuildRight),
      ignoresDuplicatedKey = ignoreDuplicatedKey)
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
    buildDataSize += relation.estimatedSize
    // This relation is usually used until the end of task.
    context.addTaskCompletionListener[Unit](_ => relation.close())
    relation
  }
  //通过 zipPartitions 操作并行地处理左右子查询的数据，构建哈希表后根据连接类型执行不同的连接策略
  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
      val hashed = buildHashedRelation(buildIter)
      joinType match {
        case FullOuter => buildSideOrFullOuterJoin(streamIter, hashed, numOutputRows,
          isFullOuterJoin = true)
        case LeftOuter if buildSide.equals(BuildLeft) =>
          buildSideOrFullOuterJoin(streamIter, hashed, numOutputRows, isFullOuterJoin = false)
        case RightOuter if buildSide.equals(BuildRight) =>
          buildSideOrFullOuterJoin(streamIter, hashed, numOutputRows, isFullOuterJoin = false)
        case _ => join(streamIter, hashed, numOutputRows)
      }
    }
  }
  //用于执行根据哈希关系进行连接的操作，特别是处理外连接（如 FullOuter、LeftOuter、RightOuter）和构建侧的连接
  private def buildSideOrFullOuterJoin(
      streamIter: Iterator[InternalRow],  //流侧的数据输入
      hashedRelation: HashedRelation,  //哈希表中的每个项都是一个连接键和对应的记录集合，用于快速查找匹配项
      numOutputRows: SQLMetric,  //计数器，用来统计输出的行数
      isFullOuterJoin: Boolean): Iterator[InternalRow] = {  //指示当前是否为全外连接（FullOuter）
    val joinKeys = streamSideKeyGenerator()
    val joinRow = new JoinedRow
    val (joinRowWithStream, joinRowWithBuild) = {
      buildSide match {
        case BuildLeft => (joinRow.withRight _, joinRow.withLeft _)  //如果构建侧是左侧（BuildLeft），则流式数据放在 joinRow 的左侧，构建数据放在右侧
        case BuildRight => (joinRow.withLeft _, joinRow.withRight _) //如果构建侧是右侧（BuildRight），则流式数据放在右侧，构建数据放在左侧
      }
    }
    //初始化用于处理空值的行
    val buildNullRow = new GenericInternalRow(buildOutput.length)
    val streamNullRow = new GenericInternalRow(streamedOutput.length)
    lazy val streamNullJoinRowWithBuild = {
      buildSide match {
        case BuildLeft =>
          joinRow.withRight(streamNullRow)
          joinRow.withLeft _
        case BuildRight =>
          joinRow.withLeft(streamNullRow)
          joinRow.withRight _
      }
    }

    val iter = if (hashedRelation.keyIsUnique) { //唯一键的情况：连接操作通常较为简单，每个连接键对应一个唯一的值
      buildSideOrFullOuterJoinUniqueKey(streamIter, hashedRelation, joinKeys, joinRowWithStream,
        joinRowWithBuild, streamNullJoinRowWithBuild, buildNullRow, isFullOuterJoin)
    } else { //非唯一键的情况：可能有多个值与同一个键匹配，因此需要特别处理。
      buildSideOrFullOuterJoinNonUniqueKey(streamIter, hashedRelation, joinKeys, joinRowWithStream,
        joinRowWithBuild, streamNullJoinRowWithBuild, buildNullRow, isFullOuterJoin)
    }
    //输出投影
    val resultProj = UnsafeProjection.create(output, output)
    iter.map { r =>
      numOutputRows += 1
      resultProj(r)
    }
  }

  /**
   * Shuffled hash join with unique join keys, where an outer side is the build side.
   * 1. Process rows from stream side by looking up hash relation.
   *    Mark the matched rows from build side be looked up.
   *    A bit set is used to track matched rows with key index.
   * 2. Process rows from build side by iterating hash relation.
   *    Filter out rows from build side being matched already,
   *    by checking key index from bit set.
   */
  //用于处理 唯一连接键 的 ShuffledHashJoinExec 中的关键方法，特别是在构建侧（build side）是外连接的一方的场景下。
  // 此方法通过 哈希表 来高效地进行连接，处理流式数据和构建数据之间的匹配，特别适用于有唯一连接键的情况
  private def buildSideOrFullOuterJoinUniqueKey(
      streamIter: Iterator[InternalRow],  //流式数据（如左侧或右侧表）的行
      hashedRelation: HashedRelation,    //哈希表
      joinKeys: UnsafeProjection,
      joinRowWithStream: InternalRow => JoinedRow, //用于将流式数据（stream）的行与构建数据行（build）合并成一个连接后的行
      joinRowWithBuild: InternalRow => JoinedRow,  //用于将构建数据行与流式数据行合并成连接后的行
      streamNullJoinRowWithBuild: => InternalRow => JoinedRow, //用于在全外连接中处理没有匹配行的情况。当流式数据没有找到匹配的构建数据时，会生成一个包含 NULL 值的连接行
      buildNullRow: GenericInternalRow,  //用于生成空行（NULL）的构建侧数据行
      isFullOuterJoin: Boolean): Iterator[InternalRow] = { //指示是否为全外连接（FullOuterJoin）
    val matchedKeys = new BitSet(hashedRelation.maxNumKeysIndex)  //追踪哪些连接键已经匹配过
    longMetric("buildDataSize") += matchedKeys.capacity / 8

    def noMatch = if (isFullOuterJoin) {
      Some(joinRowWithBuild(buildNullRow))
    } else {
      None
    }

    // Process stream side with looking up hash relation
    //处理流式数据并查找匹配的构建数据
    val streamResultIter = streamIter.flatMap { srow =>
      joinRowWithStream(srow)
      val keys = joinKeys(srow) //提取当前流式行的连接键
      if (keys.anyNull) {  //没找到Key
        noMatch
      } else {
        val matched = hashedRelation.getValueWithKeyIndex(keys)
        if (matched != null) {  //遭到匹配的行
          val keyIndex = matched.getKeyIndex
          val buildRow = matched.getValue
          val joinRow = joinRowWithBuild(buildRow)  //关联在一起
          if (boundCondition(joinRow)) { //根据条件过滤
            matchedKeys.set(keyIndex)
            Some(joinRow)
          } else {
            noMatch
          }
        } else {
          noMatch
        }
      }
    }

    // Process build side with filtering out the matched rows
    //处理构建侧数据并过滤已匹配的行
    val buildResultIter = hashedRelation.valuesWithKeyIndex().flatMap { //遍历构建数据的所有值
      valueRowWithKeyIndex =>
        val keyIndex = valueRowWithKeyIndex.getKeyIndex
        val isMatched = matchedKeys.get(keyIndex)
        if (!isMatched) {  //未匹配过
          val buildRow = valueRowWithKeyIndex.getValue
          Some(streamNullJoinRowWithBuild(buildRow))
        } else {
          None
        }
    }
    //返回最终的结果
    streamResultIter ++ buildResultIter
  }

  /**
   * Shuffled hash join with non-unique join keys, where an outer side is the build side.
   * 1. Process rows from stream side by looking up hash relation.
   *    Mark the matched rows from build side be looked up.
   *    A [[OpenHashSet]] (Long) is used to track matched rows with
   *    key index (Int) and value index (Int) together.
   * 2. Process rows from build side by iterating hash relation.
   *    Filter out rows from build side being matched already,
   *    by checking key index and value index from [[OpenHashSet]].
   *
   * The "value index" is defined as the index of the tuple in the chain
   * of tuples having the same key. For example, if certain key is found thrice,
   * the value indices of its tuples will be 0, 1 and 2.
   * Note that value indices of tuples with different keys are incomparable.
   */
    //实现了带有非唯一连接键的哈希连接，其中外侧（build side）是构建边。该方法通过使用OpenHashSet（一个高效的哈希集合）来跟踪哪些行已经匹配，以处理连接键不是唯一的情况
  private def buildSideOrFullOuterJoinNonUniqueKey(
      streamIter: Iterator[InternalRow],  //流侧的迭代器
      hashedRelation: HashedRelation, //构建侧的哈希表
      joinKeys: UnsafeProjection,
      joinRowWithStream: InternalRow => JoinedRow,  //将流侧的行与构建侧的行进行连接
      joinRowWithBuild: InternalRow => JoinedRow,   //将构建侧的行与流侧的行进行连接
      streamNullJoinRowWithBuild: => InternalRow => JoinedRow, //当没有匹配时，生成一个带有 NULL 构建侧的连接行（用于全外连接）
      buildNullRow: GenericInternalRow,
      isFullOuterJoin: Boolean): Iterator[InternalRow] = {
    val matchedRows = new OpenHashSet[Long]
    TaskContext.get().addTaskCompletionListener[Unit](_ => {
      // At the end of the task, update the task's memory usage for this
      // [[OpenHashSet]] to track matched rows, which has two parts:
      // [[OpenHashSet._bitset]] and [[OpenHashSet._data]].
      val bitSetEstimatedSize = matchedRows.getBitSet.capacity / 8
      val dataEstimatedSize = matchedRows.capacity * 8
      longMetric("buildDataSize") += bitSetEstimatedSize + dataEstimatedSize
    })
    //将特定的行标记为已匹配，通过将keyIndex和valueIndex组合成一个唯一值，添加到OpenHashSet中
    def markRowMatched(keyIndex: Int, valueIndex: Int): Unit = {
      val rowIndex: Long = (keyIndex.toLong << 32) | valueIndex
      matchedRows.add(rowIndex)
    }
    //检查特定的行是否已匹配
    def isRowMatched(keyIndex: Int, valueIndex: Int): Boolean = {
      val rowIndex: Long = (keyIndex.toLong << 32) | valueIndex
      matchedRows.contains(rowIndex)
    }

    // Process stream side with looking up hash relation
    val streamResultIter = streamIter.flatMap { srow =>
      val joinRow = joinRowWithStream(srow)
      val keys = joinKeys(srow)
      if (keys.anyNull) { //如果连接键包含NULL
        // return row with build side NULL row to satisfy full outer join semantics if enabled
        if (isFullOuterJoin) {
          Iterator.single(joinRowWithBuild(buildNullRow))
        } else {
          Iterator.empty
        }
      } else {
        val buildIter = hashedRelation.getWithKeyIndex(keys) //查找所有与连接键匹配的构建侧行
        new RowIterator {
          private var found = false
          private var valueIndex = -1
          override def advanceNext(): Boolean = {
            while (buildIter != null && buildIter.hasNext) {
              val buildRowWithKeyIndex = buildIter.next()
              val keyIndex = buildRowWithKeyIndex.getKeyIndex
              val buildRow = buildRowWithKeyIndex.getValue
              valueIndex += 1
              if (boundCondition(joinRowWithBuild(buildRow))) {
                markRowMatched(keyIndex, valueIndex)
                found = true
                return true
              }
            }
            // When we reach here, it means no match is found for this key.
            // So we need to return one row with build side NULL row,
            // to satisfy the full outer join semantic if enabled.
            if (!found && isFullOuterJoin) {
              joinRowWithBuild(buildNullRow)
              // Set `found` to be true as we only need to return one row
              // but no more.
              found = true
              return true
            }
            false
          }
          override def getRow: InternalRow = joinRow
        }.toScala
      }
    }

    // Process build side with filtering out the matched rows
    var prevKeyIndex = -1
    var valueIndex = -1
    val buildResultIter = hashedRelation.valuesWithKeyIndex().flatMap {
      valueRowWithKeyIndex =>
        val keyIndex = valueRowWithKeyIndex.getKeyIndex
        if (prevKeyIndex == -1 || keyIndex != prevKeyIndex) {
          valueIndex = 0
          prevKeyIndex = keyIndex
        } else {
          valueIndex += 1
        }

        val isMatched = isRowMatched(keyIndex, valueIndex)
        if (!isMatched) {
          val buildRow = valueRowWithKeyIndex.getValue
          Some(streamNullJoinRowWithBuild(buildRow))
        } else {
          None
        }
    }

    streamResultIter ++ buildResultIter
  }

  override def supportCodegen: Boolean = joinType match {
    case FullOuter => conf.getConf(SQLConf.ENABLE_FULL_OUTER_SHUFFLED_HASH_JOIN_CODEGEN)
    case LeftOuter if buildSide == BuildLeft =>
      conf.getConf(SQLConf.ENABLE_BUILD_SIDE_OUTER_SHUFFLED_HASH_JOIN_CODEGEN)
    case RightOuter if buildSide == BuildRight =>
      conf.getConf(SQLConf.ENABLE_BUILD_SIDE_OUTER_SHUFFLED_HASH_JOIN_CODEGEN)
    case _ => true
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.execute() :: buildPlan.execute() :: Nil
  }

  override def needCopyResult: Boolean = true

  protected override def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    val thisPlan = ctx.addReferenceObj("plan", this)
    val clsName = classOf[HashedRelation].getName

    // Inline mutable state since not many join operations in a task
    val relationTerm = ctx.addMutableState(clsName, "relation",
      v => s"$v = $thisPlan.buildHashedRelation(inputs[1]);", forceInline = true)
    HashedRelationInfo(relationTerm, keyIsUnique = false, isEmpty = false)
  }

  override def doProduce(ctx: CodegenContext): String = {
    // Specialize `doProduce` code for full outer join and build-side outer join,
    // because we need to iterate streamed and build side separately.
    val specializedProduce = joinType match {
      case FullOuter => true
      case LeftOuter if buildSide == BuildLeft => true
      case RightOuter if buildSide == BuildRight => true
      case _ => false
    }
    if (!specializedProduce) {
      return super.doProduce(ctx)
    }

    val HashedRelationInfo(relationTerm, _, _) = prepareRelation(ctx)

    // Inline mutable state since not many join operations in a task
    val keyIsUnique = ctx.addMutableState("boolean", "keyIsUnique",
      v => s"$v = $relationTerm.keyIsUnique();", forceInline = true)
    val streamedInput = ctx.addMutableState("scala.collection.Iterator", "streamedInput",
      v => s"$v = inputs[0];", forceInline = true)
    val buildInput = ctx.addMutableState("scala.collection.Iterator", "buildInput",
      v => s"$v = $relationTerm.valuesWithKeyIndex();", forceInline = true)
    val streamedRow = ctx.addMutableState("InternalRow", "streamedRow", forceInline = true)
    val buildRow = ctx.addMutableState("InternalRow", "buildRow", forceInline = true)

    // Generate variables and related code from streamed side
    val streamedVars = genOneSideJoinVars(ctx, streamedRow, streamedPlan, setDefaultValue = false)
    val streamedKeyVariables = evaluateRequiredVariables(streamedOutput, streamedVars,
      AttributeSet.fromAttributeSets(streamedKeys.map(_.references)))
    ctx.currentVars = streamedVars
    val streamedKeyExprCode = GenerateUnsafeProjection.createCode(ctx, streamedBoundKeys)
    val streamedKeyEv =
      s"""
         |$streamedKeyVariables
         |${streamedKeyExprCode.code}
       """.stripMargin
    val streamedKeyAnyNull = s"${streamedKeyExprCode.value}.anyNull()"

    // Generate code for join condition
    val (_, conditionCheck, _) =
      getJoinCondition(ctx, streamedVars, streamedPlan, buildPlan, Some(buildRow))

    // Generate code for result output in separate function, as we need to output result from
    // multiple places in join code.
    val streamedResultVars = genOneSideJoinVars(
      ctx, streamedRow, streamedPlan, setDefaultValue = true)
    val buildResultVars = genOneSideJoinVars(
      ctx, buildRow, buildPlan, setDefaultValue = true)
    val resultVars = buildSide match {
      case BuildLeft => buildResultVars ++ streamedResultVars
      case BuildRight => streamedResultVars ++ buildResultVars
    }
    val consumeOuterJoinRow = ctx.freshName("consumeOuterJoinRow")
    ctx.addNewFunction(consumeOuterJoinRow,
      s"""
         |private void $consumeOuterJoinRow() throws java.io.IOException {
         |  ${metricTerm(ctx, "numOutputRows")}.add(1);
         |  ${consume(ctx, resultVars)}
         |}
       """.stripMargin)

    val isFullOuterJoin = joinType == FullOuter
    val joinWithUniqueKey = codegenBuildSideOrFullOuterJoinWithUniqueKey(
      ctx, (streamedRow, buildRow), (streamedInput, buildInput), streamedKeyEv, streamedKeyAnyNull,
      streamedKeyExprCode.value, relationTerm, conditionCheck, consumeOuterJoinRow,
      isFullOuterJoin)
    val joinWithNonUniqueKey = codegenBuildSideOrFullOuterJoinNonUniqueKey(
      ctx, (streamedRow, buildRow), (streamedInput, buildInput), streamedKeyEv, streamedKeyAnyNull,
      streamedKeyExprCode.value, relationTerm, conditionCheck, consumeOuterJoinRow,
      isFullOuterJoin)

    s"""
       |if ($keyIsUnique) {
       |  $joinWithUniqueKey
       |} else {
       |  $joinWithNonUniqueKey
       |}
     """.stripMargin
  }

  /**
   * Generates the code for build-side or full outer join with unique join keys.
   * This is code-gen version of `buildSideOrFullOuterJoinUniqueKey()`.
   */
  private def codegenBuildSideOrFullOuterJoinWithUniqueKey(
      ctx: CodegenContext,
      rows: (String, String),
      inputs: (String, String),
      streamedKeyEv: String,
      streamedKeyAnyNull: String,
      streamedKeyValue: ExprValue,
      relationTerm: String,
      conditionCheck: String,
      consumeOuterJoinRow: String,
      isFullOuterJoin: Boolean): String = {
    // Inline mutable state since not many join operations in a task
    val matchedKeySetClsName = classOf[BitSet].getName
    val matchedKeySet = ctx.addMutableState(matchedKeySetClsName, "matchedKeySet",
      v => s"$v = new $matchedKeySetClsName($relationTerm.maxNumKeysIndex());", forceInline = true)
    val rowWithIndexClsName = classOf[ValueRowWithKeyIndex].getName
    val rowWithIndex = ctx.freshName("rowWithIndex")
    val foundMatch = ctx.freshName("foundMatch")
    val (streamedRow, buildRow) = rows
    val (streamedInput, buildInput) = inputs

    val joinStreamSide =
      s"""
         |while ($streamedInput.hasNext()) {
         |  $streamedRow = (InternalRow) $streamedInput.next();
         |
         |  // generate join key for stream side
         |  $streamedKeyEv
         |
         |  // find matches from HashedRelation
         |  boolean $foundMatch = false;
         |  $buildRow = null;
         |  $rowWithIndexClsName $rowWithIndex = $streamedKeyAnyNull ? null:
         |    $relationTerm.getValueWithKeyIndex($streamedKeyValue);
         |
         |  if ($rowWithIndex != null) {
         |    $buildRow = $rowWithIndex.getValue();
         |    // check join condition
         |    $conditionCheck {
         |      // set key index in matched keys set
         |      $matchedKeySet.set($rowWithIndex.getKeyIndex());
         |      $foundMatch = true;
         |    }
         |
         |    if (!$foundMatch) {
         |      $buildRow = null;
         |    }
         |  }
         |
         |  if ($foundMatch || $isFullOuterJoin) {
         |    $consumeOuterJoinRow();
         |  }
         |
         |  if (shouldStop()) return;
         |}
       """.stripMargin

    val filterBuildSide =
      s"""
         |$streamedRow = null;
         |
         |// find non-matched rows from HashedRelation
         |while ($buildInput.hasNext()) {
         |  $rowWithIndexClsName $rowWithIndex = ($rowWithIndexClsName) $buildInput.next();
         |
         |  // check if key index is not in matched keys set
         |  if (!$matchedKeySet.get($rowWithIndex.getKeyIndex())) {
         |    $buildRow = $rowWithIndex.getValue();
         |    $consumeOuterJoinRow();
         |  }
         |
         |  if (shouldStop()) return;
         |}
       """.stripMargin

    s"""
       |$joinStreamSide
       |$filterBuildSide
     """.stripMargin
  }

  /**
   * Generates the code for build-side or full outer join with non-unique join keys.
   * This is code-gen version of `buildSideOrFullOuterJoinNonUniqueKey()`.
   */
  private def codegenBuildSideOrFullOuterJoinNonUniqueKey(
      ctx: CodegenContext,
      rows: (String, String),
      inputs: (String, String),
      streamedKeyEv: String,
      streamedKeyAnyNull: String,
      streamedKeyValue: ExprValue,
      relationTerm: String,
      conditionCheck: String,
      consumeOuterJoinRow: String,
      isFullOuterJoin: Boolean): String = {
    // Inline mutable state since not many join operations in a task
    val matchedRowSetClsName = classOf[OpenHashSet[_]].getName
    val matchedRowSet = ctx.addMutableState(matchedRowSetClsName, "matchedRowSet",
      v => s"$v = new $matchedRowSetClsName(scala.reflect.ClassTag$$.MODULE$$.Long());",
      forceInline = true)
    val prevKeyIndex = ctx.addMutableState("int", "prevKeyIndex",
      v => s"$v = -1;", forceInline = true)
    val valueIndex = ctx.addMutableState("int", "valueIndex",
      v => s"$v = -1;", forceInline = true)
    val rowWithIndexClsName = classOf[ValueRowWithKeyIndex].getName
    val rowWithIndex = ctx.freshName("rowWithIndex")
    val buildIterator = ctx.freshName("buildIterator")
    val foundMatch = ctx.freshName("foundMatch")
    val keyIndex = ctx.freshName("keyIndex")
    val (streamedRow, buildRow) = rows
    val (streamedInput, buildInput) = inputs

    val rowIndex = s"(((long)$keyIndex) << 32) | $valueIndex"

    val joinStreamSide =
      s"""
         |while ($streamedInput.hasNext()) {
         |  $streamedRow = (InternalRow) $streamedInput.next();
         |
         |  // generate join key for stream side
         |  $streamedKeyEv
         |
         |  // find matches from HashedRelation
         |  boolean $foundMatch = false;
         |  $buildRow = null;
         |  scala.collection.Iterator $buildIterator = $streamedKeyAnyNull ? null:
         |    $relationTerm.getWithKeyIndex($streamedKeyValue);
         |
         |  int $valueIndex = -1;
         |  while ($buildIterator != null && $buildIterator.hasNext()) {
         |    $rowWithIndexClsName $rowWithIndex = ($rowWithIndexClsName) $buildIterator.next();
         |    int $keyIndex = $rowWithIndex.getKeyIndex();
         |    $buildRow = $rowWithIndex.getValue();
         |    $valueIndex++;
         |
         |    // check join condition
         |    $conditionCheck {
         |      // set row index in matched row set
         |      $matchedRowSet.add($rowIndex);
         |      $foundMatch = true;
         |      $consumeOuterJoinRow();
         |    }
         |  }
         |
         |  if (!$foundMatch) {
         |    $buildRow = null;
         |    if ($isFullOuterJoin) {
         |      $consumeOuterJoinRow();
         |    }
         |  }
         |
         |  if (shouldStop()) return;
         |}
       """.stripMargin

    val filterBuildSide =
      s"""
         |$streamedRow = null;
         |
         |// find non-matched rows from HashedRelation
         |while ($buildInput.hasNext()) {
         |  $rowWithIndexClsName $rowWithIndex = ($rowWithIndexClsName) $buildInput.next();
         |  int $keyIndex = $rowWithIndex.getKeyIndex();
         |  if ($prevKeyIndex == -1 || $keyIndex != $prevKeyIndex) {
         |    $valueIndex = 0;
         |    $prevKeyIndex = $keyIndex;
         |  } else {
         |    $valueIndex += 1;
         |  }
         |
         |  // check if row index is not in matched row set
         |  if (!$matchedRowSet.contains($rowIndex)) {
         |    $buildRow = $rowWithIndex.getValue();
         |    $consumeOuterJoinRow();
         |  }
         |
         |  if (shouldStop()) return;
         |}
       """.stripMargin

    s"""
       |$joinStreamSide
       |$filterBuildSide
     """.stripMargin
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): ShuffledHashJoinExec =
    copy(left = newLeft, right = newRight)
}
