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

import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{CodegenSupport, ExplainUtils, RowIterator}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{BooleanType, IntegralType, LongType}

/**
 * @param relationTerm variable name for HashedRelation
 * @param keyIsUnique  indicate whether keys of HashedRelation known to be unique in code-gen time
 * @param isEmpty indicate whether it known to be EmptyHashedRelation in code-gen time
 */
private[joins] case class HashedRelationInfo(
    relationTerm: String,
    keyIsUnique: Boolean,
    isEmpty: Boolean)
//专门处理基于哈希表的连接算法，支持不同类型的连接操作，比如内连接、外连接、半连接等
trait HashJoin extends JoinCodegenSupport {
  def buildSide: BuildSide  //表示在哈希连接中，哪一侧是“构建侧” (build side)，哪一侧是“流侧” (stream side)，构建侧是基于哈希表的构建操作，而流侧则是基于流式扫描的操作

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType ${buildSide} ($opId)".trim
  }
  //返回HashJoin操作的输出列
  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output  //左侧和右侧的输出列都会包含
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))  //左侧的输出列和右侧的输出列（可为空）都会包含
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output  //右侧的输出列和左侧的输出列（可为空）都会包含
      case j: ExistenceJoin =>
        left.output :+ j.exists  //仅返回左侧的输出列加上exists列（是否存在匹配项）
      case LeftExistence(_) =>
        left.output    //仅返回左侧的输出列
      case x =>
        throw new IllegalArgumentException(s"HashJoin should not take $x as the JoinType")
    }
  }
  //返回HashJoin操作的输出分区策略，它决定了如何将数据划分到不同的任务中。分区策略取决于连接的类型和构建侧
  override def outputPartitioning: Partitioning = buildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike | RightOuter => right.outputPartitioning  //如果连接类型是InnerLike或RightOuter，则分区策略是右侧的输出分区
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin =>
          left.outputPartitioning   //如果连接类型是InnerLike、LeftOuter、LeftSemi、LeftAnti或ExistenceJoin，则分区策略是左侧的输出分区
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building right side")
      }
  }
  //返回HashJoin操作的输出排序策略，它决定了如何对连接结果进行排序。排序规则取决于连接类型和构建侧
  override def outputOrdering: Seq[SortOrder] = buildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike | RightOuter => right.outputOrdering
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin =>
          left.outputOrdering
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building right side")
      }
  }
  //buildPlan和streamedPlan分别表示构建侧和流侧的执行计划（SparkPlan）
  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }
  //buildKeys和streamedKeys分别表示构建侧和流侧的连接键（即用于连接的列）
  protected lazy val (buildKeys, streamedKeys) = {
    require(leftKeys.length == rightKeys.length &&
      leftKeys.map(_.dataType)
        .zip(rightKeys.map(_.dataType))
        .forall(types => DataTypeUtils.sameType(types._1, types._2)),
      "Join keys from two sides should have same length and types")
    buildSide match {
      case BuildLeft => (leftKeys, rightKeys)
      case BuildRight => (rightKeys, leftKeys)
    }
  }

  @transient protected lazy val (buildOutput, streamedOutput) = {
    buildSide match {
      case BuildLeft => (left.output, right.output)
      case BuildRight => (right.output, left.output)
    }
  }
  //buildBoundKeys和streamedBoundKeys是分别绑定构建侧和流侧连接键的表达式。bindReferences用于将连接键与输出列绑定，确保在生成代码时能够正确访问这些列
  @transient protected lazy val buildBoundKeys =
    bindReferences(HashJoin.rewriteKeyExpr(buildKeys), buildOutput)

  @transient protected lazy val streamedBoundKeys =
    bindReferences(HashJoin.rewriteKeyExpr(streamedKeys), streamedOutput)
  //分别为构建侧和流侧生成UnsafeProjection。UnsafeProjection是一个高效的投影操作，能够将输入行转化为目标输出行
  protected def buildSideKeyGenerator(): UnsafeProjection =
    UnsafeProjection.create(buildBoundKeys)

  protected def streamSideKeyGenerator(): UnsafeProjection =
    UnsafeProjection.create(streamedBoundKeys)

  @transient protected[this] lazy val boundCondition = if (condition.isDefined) {
    if ((joinType == FullOuter || joinType == LeftOuter) && buildSide == BuildLeft) {
      // Put join left side before right side.
      Predicate.create(condition.get, buildPlan.output ++ streamedPlan.output).eval _
    } else {
      Predicate.create(condition.get, streamedPlan.output ++ buildPlan.output).eval _
    }
  } else {
    (r: InternalRow) => true
  }

  protected def createResultProjection(): (InternalRow) => InternalRow = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output, (streamedPlan.output ++ buildPlan.output).map(_.withNullability(true)))
  }
  //基于哈希表来进行高效查找匹配项，处理的核心思想是通过构建侧的哈希关系来查找流侧的匹配项
  private def innerJoin(
      streamIter: Iterator[InternalRow], //流侧（stream side）的行的迭代器。流侧是输入的另一个集合，在内连接过程中需要与构建侧进行匹配
      hashedRelation: HashedRelation): Iterator[InternalRow] = { //构建侧的哈希关系。它是通过哈希表存储构建侧的所有行，能够根据连接键快速查找匹配项
    val joinRow = new JoinedRow   //用于将流侧和构建侧的行合并为一行。JoinedRow会在流侧和构建侧有匹配时，将两侧的行合并成一个结果行
    val joinKeys = streamSideKeyGenerator()

    if (hashedRelation == EmptyHashedRelation) {  //判断hashedRelation是否为空
      Iterator.empty
    } else if (hashedRelation.keyIsUnique) {  //处理唯一键的情况
      streamIter.flatMap { srow =>
        joinRow.withLeft(srow)
        val matched = hashedRelation.getValue(joinKeys(srow))
        if (matched != null) {
          Some(joinRow.withRight(matched)).filter(boundCondition)
        } else {
          None
        }
      }
    } else {  //处理非唯一键的情况
      streamIter.flatMap { srow =>
        joinRow.withLeft(srow)
        val matches = hashedRelation.get(joinKeys(srow))  //这里返回多行
        if (matches != null) {
          matches.map(joinRow.withRight).filter(boundCondition)
        } else {
          Seq.empty
        }
      }
    }
  }
  //用于实现外连接（Outer Join）的方法，主要用于处理像左外连接（Left Outer Join）、右外连接（Right Outer Join）或全外连接（Full Outer Join）等。
  // 外连接的目的是返回两个集合中所有符合连接条件的行，以及那些没有匹配的行（填充为 null）
  private def outerJoin(
      streamedIter: Iterator[InternalRow],  //流侧（stream side）的行的迭代器。流侧是其中一个输入集合，通常是查询中右侧的表
      hashedRelation: HashedRelation): Iterator[InternalRow] = {  //构建侧（build side）的哈希关系。它是通过哈希表存储构建侧的所有行，能够根据连接键快速查找匹配项
    val joinedRow = new JoinedRow()
    val keyGenerator = streamSideKeyGenerator()
    val nullRow = new GenericInternalRow(buildPlan.output.length)  //如果流侧的某一行没有找到匹配的构建侧行，那么就需要用 nullRow 来填充构建侧的内容

    if (hashedRelation.keyIsUnique) { //唯一键情况下
      streamedIter.map { currentRow =>
        val rowKey = keyGenerator(currentRow)
        joinedRow.withLeft(currentRow)
        val matched = hashedRelation.getValue(rowKey)
        if (matched != null && boundCondition(joinedRow.withRight(matched))) {
          joinedRow
        } else {
          joinedRow.withRight(nullRow)
        }
      }
    } else {  //非唯一键情况下
      streamedIter.flatMap { currentRow =>
        val rowKey = keyGenerator(currentRow)
        joinedRow.withLeft(currentRow)
        val buildIter = hashedRelation.get(rowKey)
        new RowIterator {
          private var found = false
          override def advanceNext(): Boolean = {
            while (buildIter != null && buildIter.hasNext) {
              val nextBuildRow = buildIter.next()
              if (boundCondition(joinedRow.withRight(nextBuildRow))) {
                found = true
                return true
              }
            }
            if (!found) {
              joinedRow.withRight(nullRow)
              found = true
              return true
            }
            false
          }
          override def getRow: InternalRow = joinedRow
        }.toScala
      }
    }
  }
  //用于实现半连接（Semi Join）。
  // 半连接的目的是从流侧（stream side）返回所有有匹配构建侧（build side）行的记录。与普通连接不同，半连接只返回流侧的行，而不包含构建侧的任何信息
  private def semiJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = {
    val joinKeys = streamSideKeyGenerator()
    val joinedRow = new JoinedRow

    if (hashedRelation == EmptyHashedRelation) {
      Iterator.empty
    } else if (hashedRelation.keyIsUnique) { //唯一键情况下
      streamIter.filter { current =>
        val key = joinKeys(current)
        lazy val matched = hashedRelation.getValue(key)
        //如果连接键不为空且在构建侧找到了匹配的行（matched != null），且连接条件满足（boundCondition），则返回该行
        !key.anyNull && matched != null &&
          (condition.isEmpty || boundCondition(joinedRow(current, matched)))
      }
    } else {
      streamIter.filter { current =>
        val key = joinKeys(current)
        lazy val buildIter = hashedRelation.get(key)
        //如果连接键不为空且在构建侧找到了匹配的行（buildIter != null），且连接条件满足（boundCondition），则返回该行
        !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
          (row: InternalRow) => boundCondition(joinedRow(current, row))
        })
      }
    }
  }
  //用于实现 存在性连接（Existence Join）。
  // 这个连接类型的目的是检查流侧（stream side）每一行是否在构建侧（build side）中有匹配的行。
  // 如果存在匹配行，则返回一个标志，通常是一个布尔值，表示该流侧行是否匹配到构建侧的行。
  // 与半连接类似，存在性连接也不返回构建侧的字段，只返回流侧的字段和一个表示匹配情况的布尔值
  private def existenceJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = {
    val joinKeys = streamSideKeyGenerator()
    val result = new GenericInternalRow(Array[Any](null)) //包含一个布尔类型的字段，用于表示流侧行是否在构建侧中找到了匹配项
    val joinedRow = new JoinedRow

    if (hashedRelation.keyIsUnique) { //唯一键情况下
      streamIter.map { current =>
        val key = joinKeys(current)
        lazy val matched = hashedRelation.getValue(key)
        //如果连接键不为空且在构建侧找到了匹配的行（matched != null），则通过 boundCondition 检查连接条件是否满足。如果满足，设置 exists 为 true
        val exists = !key.anyNull && matched != null &&
          (condition.isEmpty || boundCondition(joinedRow(current, matched)))
        result.setBoolean(0, exists) //将 exists 结果保存在 result 中
        joinedRow(current, result)
      }
    } else {  //非唯一键情况下
      streamIter.map { current =>
        val key = joinKeys(current)
        lazy val buildIter = hashedRelation.get(key)
        //如果连接键不为空且在构建侧找到了匹配的行（buildIter != null），
        // 则通过 boundCondition 检查连接条件是否满足。如果有任意一行匹配且满足条件，exists 设置为 true
        val exists = !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
          (row: InternalRow) => boundCondition(joinedRow(current, row))
        })
        result.setBoolean(0, exists)
        joinedRow(current, result)
      }
    }
  }
  //用于执行 反连接（Anti Join） 操作。反连接与半连接（Semi Join）类似，
  // 都是基于流侧的每一行是否在构建侧找到匹配项来进行过滤，但反连接返回的是流侧那些 没有匹配到构建侧 的行
  private def antiJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation): Iterator[InternalRow] = {
    // If the right side is empty, AntiJoin simply returns the left side.
    if (hashedRelation == EmptyHashedRelation) {
      return streamIter
    }

    val joinKeys = streamSideKeyGenerator()
    val joinedRow = new JoinedRow

    if (hashedRelation.keyIsUnique) { //唯一键情况
      streamIter.filter { current =>
        val key = joinKeys(current)
        lazy val matched = hashedRelation.getValue(key)
        //如果连接键为空（key.anyNull）或者在构建侧没有匹配（matched == null），那么这一行流侧行会被保留
        //如果构建侧有匹配行，还会通过 boundCondition 来检查连接条件是否满足，如果条件不满足，流侧行也会被保留
        key.anyNull || matched == null ||
          (condition.isDefined && !boundCondition(joinedRow(current, matched)))
      }
    } else {  //非唯一键情况
      streamIter.filter { current =>
        val key = joinKeys(current)
        lazy val buildIter = hashedRelation.get(key)
        key.anyNull || buildIter == null || (condition.isDefined && !buildIter.exists {
          row => boundCondition(joinedRow(current, row))
        })
      }
    }
  }

  protected def join(
      streamedIter: Iterator[InternalRow],
      hashed: HashedRelation,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {

    val joinedIter = joinType match {
      case _: InnerLike =>
        innerJoin(streamedIter, hashed)
      case LeftOuter | RightOuter =>
        outerJoin(streamedIter, hashed)
      case LeftSemi =>
        semiJoin(streamedIter, hashed)
      case LeftAnti =>
        antiJoin(streamedIter, hashed)
      case _: ExistenceJoin =>
        existenceJoin(streamedIter, hashed)
      case x =>
        throw new IllegalArgumentException(
          s"HashJoin should not take $x as the JoinType")
    }

    val resultProj = createResultProjection
    joinedIter.map { r =>
      numOutputRows += 1
      resultProj(r)
    }
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    joinType match {
      case _: InnerLike => codegenInner(ctx, input)
      case LeftOuter | RightOuter => codegenOuter(ctx, input)
      case LeftSemi => codegenSemi(ctx, input)
      case LeftAnti => codegenAnti(ctx, input)
      case _: ExistenceJoin => codegenExistence(ctx, input)
      case x =>
        throw new IllegalArgumentException(
          s"HashJoin should not take $x as the JoinType")
    }
  }

  /**
   * Returns the code for generating join key for stream side, and expression of whether the key
   * has any null in it or not.
   */
  protected def genStreamSideJoinKey(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (ExprCode, String) = {
    ctx.currentVars = input
    if (streamedBoundKeys.length == 1 && streamedBoundKeys.head.dataType == LongType) {
      // generate the join key as Long
      val ev = streamedBoundKeys.head.genCode(ctx)
      (ev, ev.isNull)
    } else {
      // generate the join key as UnsafeRow
      val ev = GenerateUnsafeProjection.createCode(ctx, streamedBoundKeys)
      (ev, s"${ev.value}.anyNull()")
    }
  }

  /**
   * Generates the code for Inner join.
   */
  protected def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, isEmptyHashedRelation) = prepareRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, buildVars) = getJoinCondition(ctx, input, streamedPlan, buildPlan)
    val numOutput = metricTerm(ctx, "numOutputRows")

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }

    if (isEmptyHashedRelation) {
      """
        |// If HashedRelation is empty, hash inner join simply returns nothing.
      """.stripMargin
    } else if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |if ($matched != null) {
         |  $checkCondition {
         |    $numOutput.add(1);
         |    ${consume(ctx, resultVars)}
         |  }
         |}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ?
         |  null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |if ($matches != null) {
         |  while ($matches.hasNext()) {
         |    UnsafeRow $matched = (UnsafeRow) $matches.next();
         |    $checkCondition {
         |      $numOutput.add(1);
         |      ${consume(ctx, resultVars)}
         |    }
         |  }
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for left or right outer join.
   */
  protected def codegenOuter(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, _) = prepareRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val matched = ctx.freshName("matched")
    val buildVars = genOneSideJoinVars(ctx, matched, buildPlan, setDefaultValue = true)
    val numOutput = metricTerm(ctx, "numOutputRows")

    // filter the output via condition
    val conditionPassed = ctx.freshName("conditionPassed")
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      s"""
         |boolean $conditionPassed = true;
         |${eval.trim}
         |if ($matched != null) {
         |  ${ev.code}
         |  $conditionPassed = !${ev.isNull} && ${ev.value};
         |}
       """.stripMargin
    } else {
      s"final boolean $conditionPassed = true;"
    }

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }

    if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |${checkCondition.trim}
         |if (!$conditionPassed) {
         |  $matched = null;
         |  // reset the variables those are already evaluated.
         |  ${buildVars.filter(_.code.isEmpty).map(v => s"${v.isNull} = true;").mkString("\n")}
         |}
         |$numOutput.add(1);
         |${consume(ctx, resultVars)}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      val found = ctx.freshName("found")

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |boolean $found = false;
         |// the last iteration of this loop is to emit an empty row if there is no matched rows.
         |while ($matches != null && $matches.hasNext() || !$found) {
         |  UnsafeRow $matched = $matches != null && $matches.hasNext() ?
         |    (UnsafeRow) $matches.next() : null;
         |  ${checkCondition.trim}
         |  if ($conditionPassed) {
         |    $found = true;
         |    $numOutput.add(1);
         |    ${consume(ctx, resultVars)}
         |  }
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for left semi join.
   */
  protected def codegenSemi(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, isEmptyHashedRelation) = prepareRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _) = getJoinCondition(ctx, input, streamedPlan, buildPlan)
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (isEmptyHashedRelation) {
      """
        |// If HashedRelation is empty, hash semi join simply returns nothing.
      """.stripMargin
    } else if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |if ($matched != null) {
         |  $checkCondition {
         |    $numOutput.add(1);
         |    ${consume(ctx, input)}
         |  }
         |}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      val found = ctx.freshName("found")

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |if ($matches != null) {
         |  boolean $found = false;
         |  while (!$found && $matches.hasNext()) {
         |    UnsafeRow $matched = (UnsafeRow) $matches.next();
         |    $checkCondition {
         |      $found = true;
         |    }
         |  }
         |  if ($found) {
         |    $numOutput.add(1);
         |    ${consume(ctx, input)}
         |  }
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for anti join.
   */
  protected def codegenAnti(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, isEmptyHashedRelation) = prepareRelation(ctx)
    val numOutput = metricTerm(ctx, "numOutputRows")
    if (isEmptyHashedRelation) {
      return s"""
                |// If HashedRelation is empty, hash anti join simply returns the stream side.
                |$numOutput.add(1);
                |${consume(ctx, input)}
              """.stripMargin
    }

    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _) = getJoinCondition(ctx, input, streamedPlan, buildPlan)

    if (keyIsUnique) {
      val found = ctx.freshName("found")
      s"""
         |boolean $found = false;
         |// generate join key for stream side
         |${keyEv.code}
         |// Check if the key has nulls.
         |if (!($anyNull)) {
         |  // Check if the HashedRelation exists.
         |  UnsafeRow $matched = (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |  if ($matched != null) {
         |    // Evaluate the condition.
         |    $checkCondition {
         |      $found = true;
         |    }
         |  }
         |}
         |if (!$found) {
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      val found = ctx.freshName("found")
      s"""
         |boolean $found = false;
         |// generate join key for stream side
         |${keyEv.code}
         |// Check if the key has nulls.
         |if (!($anyNull)) {
         |  // Check if the HashedRelation exists.
         |  $iteratorCls $matches = ($iteratorCls)$relationTerm.get(${keyEv.value});
         |  if ($matches != null) {
         |    // Evaluate the condition.
         |    while (!$found && $matches.hasNext()) {
         |      UnsafeRow $matched = (UnsafeRow) $matches.next();
         |      $checkCondition {
         |        $found = true;
         |      }
         |    }
         |  }
         |}
         |if (!$found) {
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for existence join.
   */
  protected def codegenExistence(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val HashedRelationInfo(relationTerm, keyIsUnique, _) = prepareRelation(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")
    val existsVar = ctx.freshName("exists")

    val matched = ctx.freshName("matched")
    val buildVars = genOneSideJoinVars(ctx, matched, buildPlan, setDefaultValue = false)
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      s"""
         |$eval
         |${ev.code}
         |$existsVar = !${ev.isNull} && ${ev.value};
       """.stripMargin
    } else {
      s"$existsVar = true;"
    }

    val resultVar = input ++ Seq(ExprCode.forNonNullValue(
      JavaCode.variable(existsVar, BooleanType)))

    if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |boolean $existsVar = false;
         |if ($matched != null) {
         |  $checkCondition
         |}
         |$numOutput.add(1);
         |${consume(ctx, resultVar)}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |boolean $existsVar = false;
         |if ($matches != null) {
         |  while (!$existsVar && $matches.hasNext()) {
         |    UnsafeRow $matched = (UnsafeRow) $matches.next();
         |    $checkCondition
         |  }
         |}
         |$numOutput.add(1);
         |${consume(ctx, resultVar)}
       """.stripMargin
    }
  }

  protected def prepareRelation(ctx: CodegenContext): HashedRelationInfo
}

object HashJoin extends CastSupport with SQLConfHelper {
  //判断给定的连接键 keys 是否能够被重写为 LongType
  private def canRewriteAsLongType(keys: Seq[Expression]): Boolean = {
    // TODO: support BooleanType, DateType and TimestampType
    keys.forall(_.dataType.isInstanceOf[IntegralType]) &&
      keys.map(_.dataType.defaultSize).sum <= 8
    //检查所有键的 defaultSize（即每个键的字节大小）之和是否小于等于 8 字节（64 位）。
    // 这是因为 LongType 是 8 字节的，所有连接键的总大小必须小于或等于 8 字节才能将它们打包成一个 LongType
  }

  /**
   * Try to rewrite the key as LongType so we can use getLong(), if they key can fit with a long.
   * 尝试将连接键重写为 LongType，以便通过 getLong() 高效访问。它的作用是将多个连接键打包成一个 LongType，如果这些键的大小符合条件
   * If not, returns the original expressions.
   */
  def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    assert(keys.nonEmpty)
    if (!canRewriteAsLongType(keys)) {
      return keys
    }
    //如果连接键的大小符合条件，方法将通过以下步骤将多个键合并为一个 LongType
    var keyExpr: Expression = if (keys.head.dataType != LongType) {
      cast(keys.head, LongType)
    } else {
      keys.head
    } //通过位运算（左移、按位与、按位或）将其余的键与第一个键合并。每个键会根据它的数据类型大小进行适当的左移
    keys.tail.foreach { e =>
      val bits = e.dataType.defaultSize * 8
      keyExpr = BitwiseOr(ShiftLeft(keyExpr, Literal(bits)),
        BitwiseAnd(cast(e, LongType), Literal((1L << bits) - 1)))
    }
    keyExpr :: Nil
  }

  /** 从已经被打包为 LongType 的连接键中提取特定的键。它是从一个 LongType 中解包出指定的连接键
   * Extract a given key which was previously packed in a long value using its index to
   * determine the number of bits to shift
   */
  def extractKeyExprAt(keys: Seq[Expression], index: Int): Expression = {
    assert(canRewriteAsLongType(keys))
    // jump over keys that have a higher index value than the required key
    if (keys.size == 1) {
      assert(index == 0)
      Cast(
        child = BoundReference(0, LongType, nullable = false),
        dataType = keys(index).dataType,
        timeZoneId = Option(conf.sessionLocalTimeZone),
        ansiEnabled = false)
    } else {
      val shiftedBits =
        keys.slice(index + 1, keys.size).map(_.dataType.defaultSize * 8).sum
      val mask = (1L << (keys(index).dataType.defaultSize * 8)) - 1
      // build the schema for unpacking the required key
      val castChild = BitwiseAnd(
        ShiftRightUnsigned(BoundReference(0, LongType, nullable = false), Literal(shiftedBits)),
        Literal(mask))
      Cast(
        child = castChild,
        dataType = keys(index).dataType,
        timeZoneId = Option(conf.sessionLocalTimeZone),
        ansiEnabled = false)
    }
  }
}
