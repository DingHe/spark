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

package org.apache.spark.sql.execution

import java.util.Locale
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * An interface for those physical operators that support codegen.
 */
//用于支持生成代码的物理执行计划节点
trait CodegenSupport extends SparkPlan {

  /** Prefix used in the current operator's variable names. */
  //该属性根据执行计划节点的类型返回不同的变量名前缀。用于生成代码时，帮助区分不同的操作符或执行计划节点
  private def variablePrefix: String = this match {
    case _: HashAggregateExec => "hashAgg"
    case _: SortAggregateExec => "sortAgg"
    case _: BroadcastHashJoinExec => "bhj"
    case _: ShuffledHashJoinExec => "shj"
    case _: SortMergeJoinExec => "smj"
    case _: BroadcastNestedLoopJoinExec => "bnlj"
    case _: RDDScanExec => "rdd"
    case _: DataSourceScanExec => "scan"
    case _: InMemoryTableScanExec => "memoryScan"
    case _: WholeStageCodegenExec => "wholestagecodegen"
    case _ => nodeName.toLowerCase(Locale.ROOT)  //默认的实现为simpleClassName.replaceAll("Exec$", "")
  }

  /**
   * Creates a metric using the specified name.
   *
   * @return name of the variable representing the metric
   */
    //生成度量相关的代码，并把度量项注册到代码上下文中
  def metricTerm(ctx: CodegenContext, name: String): String = {
    ctx.addReferenceObj(name, longMetric(name))
  }

  /**
   * Whether this SparkPlan supports whole stage codegen or not.
   */
    //表明当前 SparkPlan 是否支持代码生成
  def supportCodegen: Boolean = true

  /**
   * Which SparkPlan is calling produce() of this one. It's itself for the first SparkPlan.
   */
    //在代码生成时，父节点决定当前节点的生成逻辑，确保代码流的正确性
  protected var parent: CodegenSupport = null

  /**
   * Returns all the RDDs of InternalRow which generates the input rows.
   *
   * @note Right now we support up to two RDDs
   */
  //返回所有的输入 RDD，生成输入数据
  def inputRDDs(): Seq[RDD[InternalRow]]

  /**
   * Returns Java source code to process the rows from input RDD.
   */
    //生成 Java 源代码，用来处理输入 RDD 中的每一行数据。它调用 doProduce() 方法来实现实际的代码生成逻辑
  final def produce(ctx: CodegenContext, parent: CodegenSupport): String = executeQuery {
    this.parent = parent  //记录父节点
    ctx.freshNamePrefix = variablePrefix  //变量前缀
    s"""
       |${ctx.registerComment(s"PRODUCE: ${this.simpleString(conf.maxToStringFields)}")}
       |${doProduce(ctx)}
     """.stripMargin
  }

  /**
   * Generate the Java source code to process, should be overridden by subclass to support codegen.
   *
   * doProduce() usually generate the framework, for example, aggregation could generate this:
   *
   *   if (!initialized) {
   *     # create a hash map, then build the aggregation hash map
   *     # call child.produce()
   *     initialized = true;
   *   }
   *   while (hashmap.hasNext()) {
   *     row = hashmap.next();
   *     # build the aggregation results
   *     # create variables for results
   *     # call consume(), which will call parent.doConsume()
   *      if (shouldStop()) return;
   *   }
   */
  //子类需要实现它来生成 Java 代码。通常这个方法会生成操作框架，比如聚合操作会生成一个哈希表的创建和处理逻辑
  protected def doProduce(ctx: CodegenContext): String
  //根据需要的列生成 UnsafeRow，并为每个列创建相应的代码表示
  private def prepareRowVar(ctx: CodegenContext, row: String, colVars: Seq[ExprCode]): ExprCode = {
    //row: String：表示输入的 UnsafeRow 变量名，可能为空（null）
    //colVars: Seq[ExprCode]：列变量的序列，这些列可能是表达式或其他数据的中间表示
    if (row != null) { //如果传入的 row 变量不为空（即已有一个 UnsafeRow 变量），则直接返回该变量
      ExprCode.forNonNullValue(JavaCode.variable(row, classOf[UnsafeRow]))
    } else {
      if (colVars.nonEmpty) {
        val colExprs = output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable)
        }
        val evaluateInputs = evaluateVariables(colVars)
        // generate the code to create a UnsafeRow
        ctx.INPUT_ROW = row
        ctx.currentVars = colVars
        val ev = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
        val code = code"""
          |$evaluateInputs
          |${ev.code}
         """.stripMargin
        ExprCode(code, FalseLiteral, ev.value)
      } else {
        // There are no columns
        ExprCode.forNonNullValue(JavaCode.variable("unsafeRow", classOf[UnsafeRow]))
      }
    }
  }

  /**
   * Consume the generated columns or row from current SparkPlan, call its parent's `doConsume()`.
   *
   * Note that `outputVars` and `row` can't both be null.
   */
    //作用是处理当前 SparkPlan 生成的列或行数据，并将这些数据传递给父级 SparkPlan 的 doConsume 方法。
  // 它主要用于生成代码，在代码生成过程中将计算结果传递给上游的处理逻辑，outputVars为上一个节点的输出变量    row为上一个节点的行遍历变量
  final def consume(ctx: CodegenContext, outputVars: Seq[ExprCode], row: String = null): String = {
    // outputVars 输出变量的序列，每个变量对应一列的生成代码
    // row: String：表示一个 UnsafeRow 的变量名，默认值为 null，表示没有显式提供行数据
    val inputVarsCandidate =
      if (outputVars != null) {  //不为空，代表上一个节点直接返回来输出变量
        assert(outputVars.length == output.length)
        // outputVars will be used to generate the code for UnsafeRow, so we should copy them
        outputVars.map(_.copy())
      } else { //上一个节点没有输出变量，则按行来遍历代码
        assert(row != null, "outputVars and row cannot both be null.")
        ctx.currentVars = null
        ctx.INPUT_ROW = row   //row为当前行的输入变量
        output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable).genCode(ctx)  //生成输出的代码
        }
      }
    //row按位置取值后变成输入变量，Stream 中的元素不是立即计算的，而是在需要时才计算
    val inputVars = inputVarsCandidate match {
      case stream: Stream[ExprCode] => stream.force //orce 方法用于强制求值整个流中的所有元素，生成一个完全求值的流（即所有惰性计算都会被执行）
      case other => other
    }
    //用来生成一个 UnsafeRow 的变量，表示当前操作的输入行，如果row不为空，变量名跟row一样
    val rowVar = prepareRowVar(ctx, row, outputVars)

    // Set up the `currentVars` in the codegen context, as we generate the code of `inputVars`
    // before calling `parent.doConsume`. We can't set up `INPUT_ROW`, because parent needs to
    // generate code of `rowVar` manually.
    ctx.currentVars = inputVars   //当前操作列引用变量变成currentVars
    ctx.INPUT_ROW = null
    ctx.freshNamePrefix = parent.variablePrefix  //获取父节点的变量前缀
    val evaluated = evaluateRequiredVariables(output, inputVars, parent.usedInputs) //计算父节点引用的输入变量的代码

    // Under certain conditions, we can put the logic to consume the rows of this operator into
    // another function. So we can prevent a generated function too long to be optimized by JIT.
    // The conditions:
    // 1. The config "spark.sql.codegen.splitConsumeFuncByOperator" is enabled.
    // 2. `inputVars` are all materialized. That is guaranteed to be true if the parent plan uses
    //    all variables in output (see `requireAllOutput`).
    // 3. The number of output variables must less than maximum number of parameters in Java method
    //    declaration.
    val confEnabled = conf.wholeStageSplitConsumeFuncByOperator
    val requireAllOutput = output.forall(parent.usedInputs.contains(_))
    val paramLength = CodeGenerator.calculateParamLength(output) + (if (row != null) 1 else 0)  //计算每个物理节点代码生成的方法需要多少个参数
    val consumeFunc = if (confEnabled && requireAllOutput
        && CodeGenerator.isValidParamLength(paramLength)) {  //如果父节点有引用本节点的输出，则生成函数
      constructDoConsumeFunction(ctx, inputVars, row)
    } else {
      parent.doConsume(ctx, inputVars, rowVar)
    }
    s"""
       |${ctx.registerComment(s"CONSUME: ${parent.simpleString(conf.maxToStringFields)}")}
       |$evaluated
       |$consumeFunc
     """.stripMargin
  }

  /**
   * To prevent concatenated function growing too long to be optimized by JIT. We can separate the
   * parent's `doConsume` codes of a `CodegenSupport` operator into a function to call.
   */
  private def constructDoConsumeFunction(
      ctx: CodegenContext,
      inputVars: Seq[ExprCode],
      row: String): String = {
    val (args, params, inputVarsInFunc) = constructConsumeParameters(ctx, output, inputVars, row)
    val rowVar = prepareRowVar(ctx, row, inputVarsInFunc)

    val doConsume = ctx.freshName("doConsume")
    ctx.currentVars = inputVarsInFunc
    ctx.INPUT_ROW = null

    val doConsumeFuncName = ctx.addNewFunction(doConsume,
      s"""
         | private void $doConsume(${params.mkString(", ")}) throws java.io.IOException {
         |   ${parent.doConsume(ctx, inputVarsInFunc, rowVar)}
         | }
       """.stripMargin)

    s"""
       | $doConsumeFuncName(${args.mkString(", ")});
     """.stripMargin
  }

  /**
   * Returns arguments for calling method and method definition parameters of the consume function.
   * And also returns the list of `ExprCode` for the parameters.
   */
  private def constructConsumeParameters(
      ctx: CodegenContext,
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      row: String): (Seq[String], Seq[String], Seq[ExprCode]) = {
    val arguments = mutable.ArrayBuffer[String]()
    val parameters = mutable.ArrayBuffer[String]()
    val paramVars = mutable.ArrayBuffer[ExprCode]()

    if (row != null) {
      arguments += row
      parameters += s"InternalRow $row"
    }

    variables.zipWithIndex.foreach { case (ev, i) =>
      val paramName = ctx.freshName(s"expr_$i")
      val paramType = CodeGenerator.javaType(attributes(i).dataType)

      arguments += ev.value
      parameters += s"$paramType $paramName"
      val paramIsNull = if (!attributes(i).nullable) {
        // Use constant `false` without passing `isNull` for non-nullable variable.
        FalseLiteral
      } else {
        val isNull = ctx.freshName(s"exprIsNull_$i")
        arguments += ev.isNull
        parameters += s"boolean $isNull"
        JavaCode.isNullVariable(isNull)
      }

      paramVars += ExprCode(paramIsNull, JavaCode.variable(paramName, attributes(i).dataType))
    }
    (arguments.toSeq, parameters.toSeq, paramVars.toSeq)
  }

  /**
   * Returns source code to evaluate all the variables, and clear the code of them, to prevent
   * them to be evaluated twice.
   */
  protected def evaluateVariables(variables: Seq[ExprCode]): String = {
    val evaluate = variables.filter(_.code.nonEmpty).map(_.code.toString).mkString("\n")
    variables.foreach(_.code = EmptyBlock)
    evaluate
  }

  /** 主要用于生成 计算所需属性（required attributes）的代码，并清除已生成的代码，避免重复计算
   * Returns source code to evaluate the variables for required attributes, and clear the code
   * of evaluated variables, to prevent them to be evaluated twice.
   *///生成的计算代码，以字符串形式返回，最终会嵌入到 Codegen 的主逻辑中
  protected def evaluateRequiredVariables(
      attributes: Seq[Attribute], // 属性列表，对应当前节点输出的字段
      variables: Seq[ExprCode], // 生成的表达式代码，表示各个属性的计算代码
      required: AttributeSet): String = {// 需要计算的属性集合
    val evaluateVars = new StringBuilder
    variables.zipWithIndex.foreach { case (ev, i) =>
      if (ev.code.nonEmpty && required.contains(attributes(i))) { //属性是必需的：required.contains(attributes(i))
        evaluateVars.append(ev.code.toString + "\n")
        ev.code = EmptyBlock //清空已生成的代码，避免重复计算
      }
    }
    evaluateVars.toString()
  }

  /**
   * Returns source code to evaluate the variables for non-deterministic expressions, and clear the
   * code of evaluated variables, to prevent them to be evaluated twice.
   */
  protected def evaluateNondeterministicVariables(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      expressions: Seq[NamedExpression]): String = {
    val nondeterministicAttrs = expressions.filterNot(_.deterministic).map(_.toAttribute)
    evaluateRequiredVariables(attributes, variables, AttributeSet(nondeterministicAttrs))
  }

  /**
   * The subset of inputSet those should be evaluated before this plan.
   *
   * We will use this to insert some code to access those columns that are actually used by current
   * plan before calling doConsume().
   */
  def usedInputs: AttributeSet = references

  /**
   * Generate the Java source code to process the rows from child SparkPlan. This should only be
   * called from `consume`.
   *
   * This should be override by subclass to support codegen.
   *
   * Note: The operator should not assume the existence of an outer processing loop,
   *       which it can jump from with "continue;"!
   *
   * For example, filter could generate this:
   *   # code to evaluate the predicate expression, result is isNull1 and value2
   *   if (!isNull1 && value2) {
   *     # call consume(), which will call parent.doConsume()
   *   }
   *
   * Note: A plan can either consume the rows as UnsafeRow (row), or a list of variables (input).
   *       When consuming as a listing of variables, the code to produce the input is already
   *       generated and `CodegenContext.currentVars` is already set. When consuming as UnsafeRow,
   *       implementations need to put `row.code` in the generated code and set
   *       `CodegenContext.INPUT_ROW` manually. Some plans may need more tweaks as they have
   *       different inputs(join build side, aggregate buffer, etc.), or other special cases.
   */
  def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    throw new UnsupportedOperationException
  }

  /**
   * Whether or not the result rows of this operator should be copied before putting into a buffer.
   *
   * If any operator inside WholeStageCodegen generate multiple rows from a single row (for
   * example, Join), this should be true.
   *
   * If an operator starts a new pipeline, this should be false.
   */
  def needCopyResult: Boolean = {
    if (children.isEmpty) {
      false
    } else if (children.length == 1) {
      children.head.asInstanceOf[CodegenSupport].needCopyResult
    } else {
      throw new UnsupportedOperationException
    }
  }

  /**
   * Whether or not the children of this operator should generate a stop check when consuming input
   * rows. This is used to suppress shouldStop() in a loop of WholeStageCodegen.
   *
   * This should be false if an operator starts a new pipeline, which means it consumes all rows
   * produced by children but doesn't output row to buffer by calling append(),  so the children
   * don't require shouldStop() in the loop of producing rows.
   */
  def needStopCheck: Boolean = parent.needStopCheck

  /**
   * Helper default should stop check code.
   */
  def shouldStopCheckCode: String = if (needStopCheck) {
    "if (shouldStop()) return;"
  } else {
    "// shouldStop check is eliminated"
  }

  /**  主要用于 生成检查是否达到 LIMIT 条件的逻辑,每个 LIMIT 对应一个检查条件，形如 "count < limit"
   * A sequence of checks which evaluate to true if the downstream Limit operators have not received
   * enough records and reached the limit. If current node is a data producing node, it can leverage
   * this information to stop producing data and complete the data flow earlier. Common data
   * producing nodes are leaf nodes like Range and Scan, and blocking nodes like Sort and Aggregate.
   * These checks should be put into the loop condition of the data producing loop.
   */
  def limitNotReachedChecks: Seq[String] = parent.limitNotReachedChecks

  /** 因为只有叶子节点（如 Scan、LocalTableScan）直接输出数据，能实时追踪生成的行数 ， 中间节点（如 Filter、Project）处理的是来自子节点的数据，不能精确控制行数
   * Check if the node is supposed to produce limit not reached checks.
   */
  protected def canCheckLimitNotReached: Boolean = children.isEmpty  //只有叶子节点可以检查输出行数是否达到限制

  /** final 表示子节点不能重写此类
   * A helper method to generate the data producing loop condition according to the
   * limit-not-reached checks.
   */
  final def limitNotReachedCond: String = {
    if (!canCheckLimitNotReached) { //只有叶子节点或阻塞节点（如 Sort、Aggregate）可以执行该检查，若不满足条件，则抛出异常或记录警告
      val errMsg = "Only leaf nodes and blocking nodes need to call 'limitNotReachedCond' " +
        "in its data producing loop."
      if (Utils.isTesting) {
        throw new IllegalStateException(errMsg)
      } else {
        logWarning(s"[BUG] $errMsg Please open a JIRA ticket to report it.")
      }
    }
    if (parent.limitNotReachedChecks.isEmpty) {
      ""
    } else {
      parent.limitNotReachedChecks.mkString("", " && ", " &&")
    }
  }
}

/**
 * A special kind of operators which support whole stage codegen. Blocking means these operators
 * will consume all the inputs first, before producing output. Typical blocking operators are
 * sort and aggregate.
 */
trait BlockingOperatorWithCodegen extends CodegenSupport {

  // Blocking operators usually have some kind of buffer to keep the data before producing them, so
  // then don't to copy its result even if its child does.
  override def needCopyResult: Boolean = false

  // Blocking operators always consume all the input first, so its upstream operators don't need a
  // stop check.
  override def needStopCheck: Boolean = false

  // Blocking operators need to consume all the inputs before producing any output. This means,
  // Limit operator after this blocking operator will never reach its limit during the execution of
  // this blocking operator's upstream operators. Here we override this method to return Nil, so
  // that upstream operators will not generate useless conditions (which are always evaluated to
  // false) for the Limit operators after this blocking operator.
  override def limitNotReachedChecks: Seq[String] = Nil

  // This is a blocking node so the node can produce these checks
  override protected def canCheckLimitNotReached: Boolean = true
}

/**  用于生成代码以读取来自一个单一 RDD 的数据。它主要用于 Spark 的代码生成优化，特别是在执行计划的 WholeStageCodegen 中，通过内联生成字节码来提高性能。
 * Leaf codegen node reading from a single RDD.
 */
trait InputRDDCodegen extends CodegenSupport {

  def inputRDD: RDD[InternalRow]  //存储要处理的输入数据，后续操作会从这个 RDD 中读取每一行数据

  // If the input can be InternalRows, an UnsafeProjection needs to be created.
  protected val createUnsafeProjection: Boolean  //指示是否需要创建一个 UnsafeProjection

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }
  //用于为InputAdapter生成输入代码
  override def doProduce(ctx: CodegenContext): String = {
    // Inline mutable state since an InputRDDCodegen is used once in a task for WholeStageCodegen
    //在ctx中添加变量 scala.collection.Iterator input = inputs[0],返回input的变量名
    val input = ctx.addMutableState("scala.collection.Iterator", "input", v => s"$v = inputs[0];",
      forceInline = true)
    val row = ctx.freshName("row")

    val outputVars = if (createUnsafeProjection) {  //是否需要把输入转为UnsafeProject
      // creating the vars will make the parent consume add an unsafe projection.
      ctx.INPUT_ROW = row
      ctx.currentVars = null
      output.zipWithIndex.map { case (a, i) =>
        BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      }
    } else {
      null
    }

    val updateNumOutputRowsMetrics = if (metrics.contains("numOutputRows")) {
      val numOutputRows = metricTerm(ctx, "numOutputRows")
      s"$numOutputRows.add(1);"
    } else {
      ""
    }
    s"""
       | while ($limitNotReachedCond $input.hasNext()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   ${updateNumOutputRowsMetrics}
       |   ${consume(ctx, outputVars, if (createUnsafeProjection) null else row).trim}
       |   ${shouldStopCheckCode}
       | }
     """.stripMargin  //如果createUnsafeProjection为true,意味着已经创建了输出变量outputVars，否则需要把row传入
  }
}

/**
 * InputAdapter is used to hide a SparkPlan from a subtree that supports codegen.
 *
 * This is the leaf node of a tree with WholeStageCodegen that is used to generate code
 * that consumes an RDD iterator of InternalRow.
 */
//简单封装子节点child，原来不支持代码生成的节点，把执行结果传递给inputRDD，然后利用InputRDDCodegen支持代码生成
case class InputAdapter(child: SparkPlan) extends UnaryExecNode with InputRDDCodegen {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def vectorTypes: Option[Seq[String]] = child.vectorTypes

  // This is not strictly needed because the codegen transformation happens after the columnar
  // transformation but just for consistency
  override def supportsColumnar: Boolean = child.supportsColumnar

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.doExecuteBroadcast()
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar()
  }

  // `InputAdapter` can only generate code to process the rows from its child. If the child produces
  // columnar batches, there must be a `ColumnarToRowExec` above `InputAdapter` to handle it by
  // overriding `inputRDDs` and calling `InputAdapter#executeColumnar` directly.
  override def inputRDD: RDD[InternalRow] = child.execute()  //从子节点获取读取数据的RDD

  // This is a leaf node so the node can produce limit not reached checks.
  override protected def canCheckLimitNotReached: Boolean = true
  //默认不用UnsafeProjection
  // InputAdapter does not need UnsafeProjection.
  protected val createUnsafeProjection: Boolean = false

  override def generateTreeString(
      depth: Int,
      lastChildren: java.util.ArrayList[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix = "",
      addSuffix = false,
      maxFields,
      printNodeId,
      indent)
  }

  override def needCopyResult: Boolean = false

  override protected def withNewChildInternal(newChild: SparkPlan): InputAdapter =
    copy(child = newChild)
}

object WholeStageCodegenExec {
  val PIPELINE_DURATION_METRIC = "duration"

  private def numOfNestedFields(dataType: DataType): Int = dataType match {
    case dt: StructType => dt.fields.map(f => numOfNestedFields(f.dataType)).sum
    case m: MapType => numOfNestedFields(m.keyType) + numOfNestedFields(m.valueType)
    case a: ArrayType => numOfNestedFields(a.elementType)
    case u: UserDefinedType[_] => numOfNestedFields(u.sqlType)
    case _ => 1
  }

  def isTooManyFields(conf: SQLConf, dataType: DataType): Boolean = {
    numOfNestedFields(dataType) > conf.wholeStageMaxNumFields
  }

  // The whole-stage codegen generates Java code on the driver side and sends it to the Executors
  // for compilation and execution. The whole-stage codegen can bring significant performance
  // improvements with large dataset in distributed environments. However, in the test environment,
  // due to the small amount of data, the time to generate Java code takes up a major part of the
  // entire runtime. So we summarize the total code generation time and output it to the execution
  // log for easy analysis and view.
  private val _codeGenTime = new AtomicLong

  // Increase the total generation time of Java source code in nanoseconds.
  // Visible for testing
  def increaseCodeGenTime(time: Long): Unit = _codeGenTime.addAndGet(time)

  // Returns the total generation time of Java source code in nanoseconds.
  // Visible for testing
  def codeGenTime: Long = _codeGenTime.get

  // Reset generation time of Java source code.
  // Visible for testing
  def resetCodeGenTime(): Unit = _codeGenTime.set(0L)
}

/**
 * WholeStageCodegen compiles a subtree of plans that support codegen together into single Java
 * function.
 *
 * Here is the call graph of to generate Java source (plan A supports codegen, but plan B does not):
 *
 *   WholeStageCodegen       Plan A               FakeInput        Plan B
 * =========================================================================
 *
 * -> execute()
 *     |
 *  doExecute() --------->   inputRDDs() -------> inputRDDs() ------> execute()
 *     |
 *     +----------------->   produce()
 *                             |
 *                          doProduce()  -------> produce()
 *                                                   |
 *                                                doProduce()
 *                                                   |
 *                         doConsume() <--------- consume()
 *                             |
 *  doConsume()  <--------  consume()
 *
 * SparkPlan A should override `doProduce()` and `doConsume()`.
 *
 * `doCodeGen()` will create a `CodeGenContext`, which will hold a list of variables for input,
 * used to generated code for [[BoundReference]].
 */
//通过将支持代码生成的查询子树编译成单一的 Java 函数来提高性能
//child: SparkPlan 执行的是一个子查询，通常是某个操作的输入
//codegenStageId: Int 唯一标识代码生成阶段的 ID，用于调试和跟踪生成的代码
case class WholeStageCodegenExec(child: SparkPlan)(val codegenStageId: Int)
    extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output  //获取该执行节点的输出列（属性），即它从 child 继承过来的输出

  override def outputPartitioning: Partitioning = child.outputPartitioning  //获取输出的分区方式，这里同样继承自子节点

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering  //获取输出数据的排序方式，继承自子节点

  // This is not strictly needed because the codegen transformation happens after the columnar
  // transformation but just for consistency
  override def supportsColumnar: Boolean = child.supportsColumnar  //表示当前节点是否支持列式存储（Columnar）
  //属性创建了一个映射，其中包含了一个计时器指标，用来记录代码生成和执行阶段的时间
  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext,
      WholeStageCodegenExec.PIPELINE_DURATION_METRIC))
  //获取节点的名称，格式为 WholeStageCodegen (codegenStageId)，便于日志和调试时查看
  override def nodeName: String = s"WholeStageCodegen (${codegenStageId})"
  //生成代码生成的 Java 类的名称
  def generatedClassName(): String = if (conf.wholeStageUseIdInClassName) {
    s"GeneratedIteratorForCodegenStage$codegenStageId"
  } else {
    "GeneratedIterator"
  }

  /**
   * Generates code for this subtree.
   * 负责生成整个执行计划子树的代码。该方法通过调用子节点的代码生成接口，生成计算的 Java 代码并进行一些必要的初始化操作，最终返回生成的代码和代码生成的上下文
   * @return the tuple of the codegen context and the actual generated source.
   */
  def doCodeGen(): (CodegenContext, CodeAndComment) = {
    val startTime = System.nanoTime()
    val ctx = new CodegenContext
    val code = child.asInstanceOf[CodegenSupport].produce(ctx, this) //产生子节点的代码

    // main next function.
    ctx.addNewFunction("processNext",
      s"""
        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
       """, inlineToOuterClass = true)

    val className = generatedClassName() //生成当前代码生成类的类名

    val source = s"""
      public Object generate(Object[] references) {
        return new $className(references);
      }

      ${ctx.registerComment(
        s"""Codegened pipeline for stage (id=$codegenStageId)
           |${this.treeString.trim}""".stripMargin,
         "wsc_codegenPipeline")}
      ${ctx.registerComment(s"codegenStageId=$codegenStageId", "wsc_codegenStageId", true)}
      final class $className extends ${classOf[BufferedRowIterator].getName} {

        private Object[] references;
        private scala.collection.Iterator[] inputs;
        ${ctx.declareMutableStates()}

        public $className(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator[] inputs) {
          partitionIndex = index;
          this.inputs = inputs;
          ${ctx.initMutableStates()}
          ${ctx.initPartition()}
        }

        ${ctx.emitExtraCode()}

        ${ctx.declareAddedFunctions()}
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(source), ctx.getPlaceHolderToComments()))

    val duration = System.nanoTime() - startTime
    WholeStageCodegenExec.increaseCodeGenTime(duration)

    logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    (ctx, cleanedSource)
  }
  //用于执行列式存储的数据（Columnar），目前 Spark 对列式输出的代码生成不完全支持，因此这个方法通常会回退到解释执行路径，直接调用子节点的 executeColumnar() 方法
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Code generation is not currently supported for columnar output, so just fall back to
    // the interpreted path
    child.executeColumnar()
  }

  override def doExecute(): RDD[InternalRow] = {
    val (ctx, cleanedSource) = doCodeGen()  //生成代码
    // try to compile and fallback if it failed
    val (_, compiledCodeStats) = try {   //尝试编译生成的代码
      CodeGenerator.compile(cleanedSource)
    } catch {
      case NonFatal(_) if !Utils.isTesting && conf.codegenFallback =>
        // We should already saw the error message
        logWarning(s"Whole-stage codegen disabled for plan (id=$codegenStageId):\n $treeString")
        return child.execute()
    }
    //如果编译后的方法过大（超过了 conf.hugeMethodLimit 的配置），则会禁用代码生成，并回退到原始执行路径（child.execute()）
    // Check if compiled code has a too large function
    if (compiledCodeStats.maxMethodCodeSize > conf.hugeMethodLimit) {
      logInfo(s"Found too long generated codes and JIT optimization might not work: " +
        s"the bytecode size (${compiledCodeStats.maxMethodCodeSize}) is above the limit " +
        s"${conf.hugeMethodLimit}, and the whole-stage codegen was disabled " +
        s"for this plan (id=$codegenStageId). To avoid this, you can raise the limit " +
        s"`${SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key}`:\n$treeString")
      return child.execute()
    }
    //获取所有生成代码中使用的引用（即变量或常量），并将其转换为数组形式，以便后续使用
    val references = ctx.references.toArray
    //记录了代码生成后执行阶段的时间，用于性能监控
    val durationMs = longMetric("pipelineTime")

    // Even though rdds is an RDD[InternalRow] it may actually be an RDD[ColumnarBatch] with
    // type erasure hiding that. This allows for the input to a code gen stage to be columnar,
    // but the output must be rows.
    val rdds = child.asInstanceOf[CodegenSupport].inputRDDs()  //获取子节点输入的RDD，转换节点不断调用子节点的inputRDDs,直到数据源节点才有RDD
    assert(rdds.size <= 2, "Up to two input RDDs can be supported")  //支持最多两个输入 RDD（这限制了在代码生成过程中可以并行处理的数据源数量
    val evaluatorFactory = new WholeStageCodegenEvaluatorFactory(
      cleanedSource, durationMs, references)  //生成处理每个分区的代码
    if (rdds.length == 1) {  //按照生成的分区代码处理RDD的逻辑
      if (conf.usePartitionEvaluator) {
        rdds.head.mapPartitionsWithEvaluator(evaluatorFactory)
      } else {
        rdds.head.mapPartitionsWithIndex { (index, iter) =>
          val evaluator = evaluatorFactory.createEvaluator()
          evaluator.eval(index, iter)
        }
      }
    } else {
      // Right now, we support up to two input RDDs.
      if (conf.usePartitionEvaluator) {
        rdds.head.zipPartitionsWithEvaluator(rdds(1), evaluatorFactory)
      } else {
        rdds.head.zipPartitions(rdds(1)) { (leftIter, rightIter) =>
          Iterator((leftIter, rightIter))
          // a small hack to obtain the correct partition index
        }.mapPartitionsWithIndex { (index, zippedIter) =>
          val (leftIter, rightIter) = zippedIter.next()
          val evaluator = evaluatorFactory.createEvaluator()
          evaluator.eval(index, leftIter, rightIter)
        }
      }
    }
  }
  //输入 RDD 的获取是通过代码生成的，不直接通过此方法
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    throw new UnsupportedOperationException
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }
  //生成代码，用于消费输入的数据行
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val doCopy = if (needCopyResult) {
      ".copy()"
    } else {
      ""
    }
    s"""
      |${row.code}
      |append(${row.value}$doCopy);
     """.stripMargin.trim
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: java.util.ArrayList[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      if (printNodeId) "* " else s"*($codegenStageId) ",
      false,
      maxFields,
      printNodeId,
      indent)
  }

  override def needStopCheck: Boolean = true

  override def limitNotReachedChecks: Seq[String] = Nil

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(codegenStageId.asInstanceOf[Integer])

  override protected def withNewChildInternal(newChild: SparkPlan): WholeStageCodegenExec =
    copy(child = newChild)(codegenStageId)
}


/**
 * Find the chained plans that support codegen, collapse them together as WholeStageCodegen.
 *
 * The `codegenStageCounter` generates ID for codegen stages within a query plan.
 * It does not affect equality, nor does it participate in destructuring pattern matching
 * of WholeStageCodegenExec.
 *
 * This ID is used to help differentiate between codegen stages. It is included as a part
 * of the explain output for physical plans, e.g.
 *
 * == Physical Plan ==
 * *(5) SortMergeJoin [x#3L], [y#9L], Inner
 * :- *(2) Sort [x#3L ASC NULLS FIRST], false, 0
 * :  +- Exchange hashpartitioning(x#3L, 200)
 * :     +- *(1) Project [(id#0L % 2) AS x#3L]
 * :        +- *(1) Filter isnotnull((id#0L % 2))
 * :           +- *(1) Range (0, 5, step=1, splits=8)
 * +- *(4) Sort [y#9L ASC NULLS FIRST], false, 0
 *    +- Exchange hashpartitioning(y#9L, 200)
 *       +- *(3) Project [(id#6L % 2) AS y#9L]
 *          +- *(3) Filter isnotnull((id#6L % 2))
 *             +- *(3) Range (0, 5, step=1, splits=8)
 *
 * where the ID makes it obvious that not all adjacent codegen'd plan operators are of the
 * same codegen stage.
 *
 * The codegen stage ID is also optionally included in the name of the generated classes as
 * a suffix, so that it's easier to associate a generated class back to the physical operator.
 * This is controlled by SQLConf: spark.sql.codegen.useIdInClassName
 *
 * The ID is also included in various log messages.
 *
 * Within a query, a codegen stage in a plan starts counting from 1, in "insertion order".
 * WholeStageCodegenExec operators are inserted into a plan in depth-first post-order.
 * See CollapseCodegenStages.insertWholeStageCodegen for the definition of insertion order.
 *
 * 0 is reserved as a special ID value to indicate a temporary WholeStageCodegenExec object
 * is created, e.g. for special fallback handling when an existing WholeStageCodegenExec
 * failed to generate/compile code.
 */
//作用是将支持代码生成（codegen）的查询执行计划（SparkPlan）进行合并，形成一个包含代码生成的整体执行节点（WholeStageCodegenExec）。
//这个优化的目标是通过代码生成来提高执行效率，减少运行时的解析和计算开销
//codegenStageCounter 是一个 AtomicInteger，用来生成代码生成阶段的唯一标识符（ID），每次调用时会自增，用于区分不同的代码生成阶段
case class CollapseCodegenStages(
    codegenStageCounter: AtomicInteger = new AtomicInteger(0))
  extends Rule[SparkPlan] {
  //检查一个表达式是否支持代码生成。支持代码生成的表达式包括叶子表达式（LeafExpression）以及其他非 CodegenFallback 类型的表达式
  private def supportCodegen(e: Expression): Boolean = e match {
    case e: LeafExpression => true
    // CodegenFallback requires the input to be an InternalRow
    case e: CodegenFallback => false //不支持代码生成的表达式类型
    case _ => true
  }
  //检查一个 SparkPlan 是否支持代码生成
  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport if plan.supportCodegen =>
      val willFallback = plan.expressions.exists(_.exists(e => !supportCodegen(e)))  //如果该节点的表达式中存在不支持代码生成的子表达式，则不支持代码生成
      // the generated code will be huge if there are too many columns
      val hasTooManyOutputFields =
        WholeStageCodegenExec.isTooManyFields(conf, plan.schema)  //如果输出或输入字段过多，则认为不适合进行代码生成
      val hasTooManyInputFields =
        plan.children.exists(p => WholeStageCodegenExec.isTooManyFields(conf, p.schema))
      !willFallback && !hasTooManyOutputFields && !hasTooManyInputFields
    case _ => false
  }

  /** 在不支持代码生成的计划节点上方插入一个 InputAdapter，将不支持代码生成的部分转换为支持代码生成的部分
   * Inserts an InputAdapter on top of those that do not support codegen.
   */
  private def insertInputAdapter(plan: SparkPlan): SparkPlan = {
    plan match {
      case p if !supportCodegen(p) =>
        // collapse them recursively
        InputAdapter(insertWholeStageCodegen(p))
      case j: SortMergeJoinExec =>
        // The children of SortMergeJoin should do codegen separately.
        j.withNewChildren(j.children.map(
          child => InputAdapter(insertWholeStageCodegen(child))))
      case j: ShuffledHashJoinExec =>
        // The children of ShuffledHashJoin should do codegen separately.
        j.withNewChildren(j.children.map(
          child => InputAdapter(insertWholeStageCodegen(child))))
      case p => p.withNewChildren(p.children.map(insertInputAdapter))
    }
  }

  /** 支持代码生成的节点上方插入 WholeStageCodegenExec，将整个子树转换为使用代码生成的形式
   * Inserts a WholeStageCodegen on top of those that support codegen.
   */
  private def insertWholeStageCodegen(plan: SparkPlan): SparkPlan = {
    plan match {
      // For operators that will output domain object, do not insert WholeStageCodegen for it as
      // domain object can not be written into unsafe row.
      //如果节点的输出字段是 ObjectType，则不进行代码生成，因为对象类型无法直接写入 UnsafeRow
      case plan if plan.output.length == 1 && plan.output.head.dataType.isInstanceOf[ObjectType] =>
        plan.withNewChildren(plan.children.map(insertWholeStageCodegen))
      case plan: LocalTableScanExec =>
        // Do not make LogicalTableScanExec the root of WholeStageCodegen
        // to support the fast driver-local collect/take paths.
        plan   //不支持代码生成
      case plan: CommandResultExec =>
        // Do not make CommandResultExec the root of WholeStageCodegen
        // to support the fast driver-local collect/take paths.
        plan   //不支持代码生成
      case plan: CodegenSupport if supportCodegen(plan) =>
        // The whole-stage-codegen framework is row-based. If a plan supports columnar execution,
        // it can't support whole-stage-codegen at the same time.
        assert(!plan.supportsColumnar)  //没有启用列式处理
        WholeStageCodegenExec(insertInputAdapter(plan))(codegenStageCounter.incrementAndGet())  //将其包装在 WholeStageCodegenExec 中
      case other =>
        other.withNewChildren(other.children.map(insertWholeStageCodegen))
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.wholeStageEnabled && CodegenObjectFactoryMode.withName(conf.codegenFactoryMode)
      != CodegenObjectFactoryMode.NO_CODEGEN) { //codegenFactoryMode的值为FALLBACK, CODEGEN_ONLY, NO_CODEGEN，默认为FALLBACK
      insertWholeStageCodegen(plan)
    } else {
      plan
    }
  }
}
