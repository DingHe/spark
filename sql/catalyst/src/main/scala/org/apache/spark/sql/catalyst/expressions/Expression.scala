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

package org.apache.spark.sql.catalyst.expressions

import java.util.Locale

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, CurrentOrigin, LeafLike, QuaternaryLike, SQLQueryContext, TernaryLike, TreeNode, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.errors.{QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.MULTI_COMMUTATIVE_OP_OPT_THRESHOLD
import org.apache.spark.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the basic expression abstract classes in Catalyst.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * An expression in Catalyst.
 *
 * If an expression wants to be exposed in the function registry (so users can call it with
 * "name(arguments...)", the concrete implementation must be a case class whose constructor
 * arguments are all Expressions types. See [[Substring]] for an example.
 *
 * There are a few important traits or abstract classes:
 *
 * - [[Nondeterministic]]: an expression that is not deterministic.
 * - [[Unevaluable]]: an expression that is not supposed to be evaluated.
 * - [[CodegenFallback]]: an expression that does not have code gen implemented and falls back to
 *                        interpreted mode.
 * - [[NullIntolerant]]: an expression that is null intolerant (i.e. any null input will result in
 *                       null output).
 * - [[NonSQLExpression]]: a common base trait for the expressions that do not have SQL
 *                         expressions like representation. For example, `ScalaUDF`, `ScalaUDAF`,
 *                         and object `MapObjects` and `Invoke`.
 * - [[UserDefinedExpression]]: a common base trait for user-defined functions, including
 *                              UDF/UDAF/UDTF.
 * - [[HigherOrderFunction]]: a common base trait for higher order functions that take one or more
 *                            (lambda) functions and applies these to some objects. The function
 *                            produces a number of variables which can be consumed by some lambda
 *                            functions.
 * - [[NamedExpression]]: An [[Expression]] that is named.
 * - [[TimeZoneAwareExpression]]: A common base trait for time zone aware expressions.
 * - [[SubqueryExpression]]: A base interface for expressions that contain a
 *                           [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]].
 *
 * - [[LeafExpression]]: an expression that has no child.
 * - [[UnaryExpression]]: an expression that has one child.
 * - [[BinaryExpression]]: an expression that has two children.
 * - [[TernaryExpression]]: an expression that has three children.
 * - [[QuaternaryExpression]]: an expression that has four children.
 * - [[BinaryOperator]]: a special case of [[BinaryExpression]] that requires two children to have
 *                       the same output data type.
 *
 * A few important traits used for type coercion rules:
 * - [[ExpectsInputTypes]]: an expression that has the expected input types. This trait is typically
 *                          used by operator expressions (e.g. [[Add]], [[Subtract]]) to define
 *                          expected input types without any implicit casting.
 * - [[ImplicitCastInputTypes]]: an expression that has the expected input types, which can be
 *                               implicitly castable using [[TypeCoercion.ImplicitTypeCasts]].
 * - [[ComplexTypeMergingExpression]]: to resolve output types of the complex expressions
 *                                     (e.g., [[CaseWhen]]).
 */
abstract class Expression extends TreeNode[Expression] {

  /**
   * Returns true when an expression is a candidate for static evaluation before the query is
   * executed. A typical use case: [[org.apache.spark.sql.catalyst.optimizer.ConstantFolding]]
   *
   * The following conditions are used to determine suitability for constant folding:
   *  - A [[Coalesce]] is foldable if all of its children are foldable
   *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
   *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
   *  - A [[Literal]] is foldable
   *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
   */
    //表达式是否可以在查询执行前进行常量折叠。即，是否可以提前计算出表达式的结果
  def foldable: Boolean = false

  /**
   * Returns true when the current expression always return the same result for fixed inputs from
   * children. The non-deterministic expressions should not change in number and order. They should
   * not be evaluated during the query planning.
   *
   * Note that this means that an expression should be considered as non-deterministic if:
   * - it relies on some mutable internal state, or
   * - it relies on some implicit input that is not part of the children expression list.
   * - it has non-deterministic child or children.
   * - it assumes the input satisfies some certain condition via the child operator.
   *
   * An example would be `SparkPartitionID` that relies on the partition id returned by TaskContext.
   * By default leaf expressions are deterministic as Nil.forall(_.deterministic) returns true.
   */
    //表示表达式是否是确定性的。即，在固定输入下，每次执行结果是否相同
  lazy val deterministic: Boolean = children.forall(_.deterministic)
  //表示表达式的输出是否可能为null
  def nullable: Boolean

  /**
   * Workaround scala compiler so that we can call super on lazy vals
   */
  @transient
  private lazy val _references: AttributeSet =
    AttributeSet.fromAttributeSets(children.map(_.references))
  //返回表达式引用的所有字段集合
  def references: AttributeSet = _references

  /**
   * Returns true if the expression contains mutable state.
   *
   * A stateful expression should never be evaluated multiple times for a single row. This should
   * only be a problem for interpreted execution. This can be prevented by creating fresh copies
   * of the stateful expression before execution. A common example to trigger this issue:
   * {{{
   *   val rand = functions.rand()
   *   df.select(rand, rand) // These 2 rand should not share a state.
   * }}}
   */
    //返回表达式是否包含可变状态
  def stateful: Boolean = false

  /**
   * Returns a copy of this expression where all stateful expressions are replaced with fresh
   * uninitialized copies. If the expression contains no stateful expressions then the original
   * expression is returned.
   */
    //是返回当前表达式的副本，其中所有包含可变状态的子表达式都被替换为新的未初始化的副本。
  // 如果表达式中没有状态依赖的子表达式，则直接返回原始表达式
  def freshCopyIfContainsStatefulExpression(): Expression = {
    val childrenIndexedSeq: IndexedSeq[Expression] = children match {
      case types: IndexedSeq[Expression] => types
      case other => other.toIndexedSeq
    }
    val newChildren = childrenIndexedSeq.map(_.freshCopyIfContainsStatefulExpression())
    // A more efficient version of `children.zip(newChildren).exists(_ ne _)`
    val anyChildChanged = {
      val size = newChildren.length
      var i = 0
      var res: Boolean = false
      while (!res && i < size) {
        res |= (childrenIndexedSeq(i) ne newChildren(i))
        i += 1
      }
      res
    }
    // If the children contain stateful expressions and get copied, or this expression is stateful,
    // copy this expression with the new children.
    //检查子表达式是否发生了变化
    if (anyChildChanged || stateful) {
      CurrentOrigin.withOrigin(origin) {
        val res = withNewChildrenInternal(newChildren)
        res.copyTagsFrom(this)
        res
      }
    } else {
      this
    }
  }

  /** Returns the result of evaluating this expression on a given input Row */
  //评估表达式并返回结果，接受一个InternalRow作为输入，并返回计算结果。
  def eval(input: InternalRow = null): Any

  /**
   * Returns an [[ExprCode]], that contains the Java source code to generate the result of
   * evaluating the expression on an input row.
   *
   * @param ctx a [[CodegenContext]]
   * @return [[ExprCode]]
   */
  def genCode(ctx: CodegenContext): ExprCode = {
    //检查当前表达式是否与之前的子表达式重复。如果表达式是重复的（已经生成过代码），则可以复用之前的代码，从而避免不必要的重复计算
    ctx.subExprEliminationExprs.get(ExpressionEquals(this)).map { subExprState =>
      // This expression is repeated which means that the code to evaluate it has already been added
      // as a function before. In that case, we just re-use it.
      ExprCode(
        ctx.registerComment(this.toString),
        subExprState.eval.isNull,
        subExprState.eval.value)
    }.getOrElse { //如果没有重复表达式
      val isNull = ctx.freshName("isNull")   //空值检测变量
      val value = ctx.freshName("value")     //实际值的变量
      val eval = doGenCode(ctx, ExprCode(
        JavaCode.isNullVariable(isNull),
        JavaCode.variable(value, dataType)))
      reduceCodeSize(ctx, eval)   //如果代码过长，就拆分为子函数
      if (eval.code.toString.nonEmpty) {
        // Add `this` in the comment.
        eval.copy(code = ctx.registerComment(this.toString) + eval.code)
      } else {
        eval
      }
    }
  }
  //这段代码主要是在生成代码过大时，将其拆分成更小的函数，以减少冗余和提高代码的可管理性
  private def reduceCodeSize(ctx: CodegenContext, eval: ExprCode): Unit = {
    // TODO: support whole stage codegen too
    //获取配置的阈值，用来决定是否需要将代码拆分成多个函数
    val splitThreshold = SQLConf.get.methodSplitThreshold
    if (eval.code.length > splitThreshold && ctx.INPUT_ROW != null && ctx.currentVars == null) {
      val setIsNull = if (!eval.isNull.isInstanceOf[LiteralValue]) {
        //判断当前的isNull是否为LiteralValue
        val globalIsNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "globalIsNull")
        val localIsNull = eval.isNull
        eval.isNull = JavaCode.isNullGlobal(globalIsNull)
        s"$globalIsNull = $localIsNull;"
      } else {
        ""
      }

      val javaType = CodeGenerator.javaType(dataType)
      val newValue = ctx.freshName("value")

      val funcName = ctx.freshName(nodeName)
      val funcFullName = ctx.addNewFunction(funcName,
        s"""
           |private $javaType $funcName(InternalRow ${ctx.INPUT_ROW}) {
           |  ${eval.code}
           |  $setIsNull
           |  return ${eval.value};
           |}
           """.stripMargin)

      eval.value = JavaCode.variable(newValue, dataType)
      eval.code = code"$javaType $newValue = $funcFullName(${ctx.INPUT_ROW});"
    }
  }

  /**
   * Returns Java source code that can be compiled to evaluate this expression.
   * The default behavior is to call the eval method of the expression. Concrete expression
   * implementations should override this to do actual code generation.
   *
   * @param ctx a [[CodegenContext]]
   * @param ev an [[ExprCode]] with unique terms.
   * @return an [[ExprCode]] containing the Java source code to generate the given expression
   */
  //具体的Expression子类会实现该方法来生成针对该表达式的Java代码
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode

  /**
   * Returns `true` if this expression and all its children have been resolved to a specific schema
   * and input data types checking passed, and `false` if it still contains any unresolved
   * placeholders or has data types mismatch.
   * Implementations of expressions should override this if the resolution of this type of
   * expression involves more than just the resolution of its children and type checking.
   */
    //表示表达式及其所有子表达式是否已经解析并检查了数据类型。如果表达式仍未解析，resolved 将为 false
  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  def dataType: DataType

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if any still contains any unresolved placeholders.
   */
    //表示表达式的所有子表达式是否已解析。如果所有子表达式都解析了，则返回 true
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Returns an expression where a best effort attempt has been made to transform `this` in a way
   * that preserves the result but removes cosmetic variations (case sensitivity, ordering for
   * commutative operations, etc.).
   *
   * `deterministic` expressions where `this.canonicalized == other.canonicalized` will always
   * evaluate to the same result.
   *
   * The process of canonicalization is a one pass, bottum-up expression tree computation based on
   * canonicalizing children before canonicalizing the current node. There is one exception though,
   * as adjacent, same class [[CommutativeExpression]]s canonicalazion happens in a way that calling
   * `canonicalized` on the root:
   *   1. Gathers and canonicalizes the non-commutative (or commutative but not same class) child
   *      expressions of the adjacent expressions.
   *   2. Reorder the canonicalized child expressions by their hashcode.
   * This means that the lazy `cannonicalized` is called and computed only on the root of the
   * adjacent expressions.
   */
    //返回一个标准化的表达式，即去除一些外部差异（如大小写、运算顺序等）后，表达式的规范化版本
  lazy val canonicalized: Expression = withCanonicalizedChildren

  /**
   * The default process of canonicalization. It is a one pass, bottum-up expression tree
   * computation based oncanonicalizing children before canonicalizing the current node.
   */
  final protected def withCanonicalizedChildren: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    withNewChildren(canonicalizedChildren)
  }

  /**
   * Returns true when two expressions will always compute the same result, even if they differ
   * cosmetically (i.e. capitalization of names in attributes may be different).
   *
   * See [[Canonicalize]] for more details.
   */
    //判断两个表达式是否语义上相等，即使它们的表示方式可能不同（如大小写不同，但结果相同）
  final def semanticEquals(other: Expression): Boolean =
    deterministic && other.deterministic && canonicalized == other.canonicalized

  /**
   * Returns a `hashCode` for the calculation performed by this expression. Unlike the standard
   * `hashCode`, an attempt has been made to eliminate cosmetic differences.
   *
   * See [[Canonicalize]] for more details.
   */
  def semanticHash(): Int = canonicalized.hashCode()

  /**
   * Checks the input data types, returns `TypeCheckResult.success` if it's valid,
   * or returns a `TypeCheckResult` with an error message if invalid.
   * Note: it's not valid to call this method until `childrenResolved == true`.
   */
    //检查表达式的输入数据类型是否有效。如果输入数据类型不匹配，会返回一个错误信息
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /**
   * Returns a user-facing string representation of this expression's name.
   * This should usually match the name of the function in SQL.
   */
    //返回表达式的用户友好的名字，通常与 SQL 函数名匹配
  def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(nodeName.toLowerCase(Locale.ROOT))
 //返回表达式的所有参数的平铺形式，主要用于将复杂的参数结构展平成简单的结构
  protected def flatArguments: Iterator[Any] = stringArgs.flatMap {
    case t: Iterable[_] => t
    case single => single :: Nil
  }

  // Marks this as final, Expression.verboseString should never be called, and thus shouldn't be
  // overridden by concrete classes.
  //返回表达式的详细字符串表示。此方法被标记为 final，并且不应被具体类重写
  final override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleString(maxFields: Int): String = toString

  override def toString: String = prettyName + truncatedString(
    flatArguments.toSeq, "(", ", ", ")", SQLConf.get.maxToStringFields)

  /**
   * Returns SQL representation of this expression.  For expressions extending [[NonSQLExpression]],
   * this method may return an arbitrary user facing string.
   */
    //返回表达式的 SQL 表达式表示。例如，Add(1, 2) 会转换为 SQL 表达式 1 + 2
  def sql: String = {
    val childrenSQL = children.map(_.sql).mkString(", ")
    s"$prettyName($childrenSQL)"
  }

  override def simpleStringWithNodeId(): String = {
    throw SparkException.internalError(s"$nodeName does not implement simpleStringWithNodeId")
  }
  //数据类型前缀
  protected def typeSuffix =
    if (resolved) {
      dataType match {
        case LongType => "L"
        case _ => ""
      }
    } else {
      ""
    }
}


/**
 * An expression that cannot be evaluated. These expressions don't live past analysis or
 * optimization time (e.g. Star) and should not be evaluated during query planning and
 * execution.
 */
//表示在查询执行过程中无法被评估的表达式
trait Unevaluable extends Expression {

  /** Unevaluable is not foldable because we don't have an eval for it. */
    //表示该表达式是否可以在查询执行前被折叠（即在编译时或优化阶段进行简化）。在这里，它的值是 false，意味着该表达式无法在执行前被计算或简化
  final override def foldable: Boolean = false
  //该方法用于评估表达式，并返回计算结果。但由于这个表达式不能被评估，它会抛出一个错误
  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}


/**
 * An expression that gets replaced at runtime (currently by the optimizer) into a different
 * expression for evaluation. This is mainly used to provide compatibility with other databases.
 * For example, we use this to support "nvl" by replacing it with "coalesce".
 */
//表示在运行时会被替换为其他表达式的表达式，通常是在查询优化阶段进行替换
trait RuntimeReplaceable extends Expression {
  //该表达式在运行时会被替换为的实际表达式。替换后的表达式会被评估
  def replacement: Expression

  override val nodePatterns: Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)
  override def nullable: Boolean = replacement.nullable
  override def dataType: DataType = replacement.dataType
  // As this expression gets replaced at optimization with its `child" expression,
  // two `RuntimeReplaceable` are considered to be semantically equal if their "child" expressions
  // are semantically equal.
  override lazy val canonicalized: Expression = replacement.canonicalized

  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)
  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

/**
 * An add-on of [[RuntimeReplaceable]]. It makes `replacement` the child of the expression, to
 * inherit the analysis rules for it, such as type coercion. The implementation should put
 * `replacement` in the case class constructor, and define a normal constructor that accepts only
 * the original parameters. For an example, see [[TryAdd]]. To make sure the explain plan and
 * expression SQL works correctly, the implementation should also implement the `parameters` method.
 */
////增强版的 RuntimeReplaceable，其作用是使得 replacement 成为当前表达式的子表达式，
// 从而继承其分析规则，比如类型强制转换。这允许替换表达式的规则（如类型推断、优化等）能够自动应用到包含它的表达式上
trait InheritAnalysisRules extends UnaryLike[Expression] { self: RuntimeReplaceable =>
  override def child: Expression = replacement //当前表达式的子表达式是 replacement
  def parameters: Seq[Expression] //用于获取当前表达式的参数序列
  override def flatArguments: Iterator[Any] = parameters.iterator
  // This method is used to generate a SQL string with transformed inputs. This is necessary as
  // the actual inputs are not the children of this expression.
  //用于生成 SQL 字符串表示形式。由于实际的输入（即子表达式）并不是当前表达式的直接子表达式，
  // 因此需要对 replacement 的 SQL 字符串进行转换，并将其格式化为适当的 SQL 语法。
  def makeSQLString(childrenSQL: Seq[String]): String = {
    prettyName + childrenSQL.mkString("(", ", ", ")")
  }
  final override def sql: String = makeSQLString(parameters.map(_.sql))
}

/**
 * An add-on of [[AggregateFunction]]. This gets rewritten (currently by the optimizer) into a
 * different aggregate expression for evaluation. This is mainly used to provide compatibility
 * with other databases. For example, we use this to support every, any/some aggregates by rewriting
 * them with Min and Max respectively.
 */
//是 RuntimeReplaceable 的一个扩展，用于处理聚合函数的运行时替换。
// 它主要通过优化器在运行时将特定的聚合表达式替换为其他聚合表达式，以提供与其他数据库的兼容性
//RuntimeReplaceableAggregate 继承了 RuntimeReplaceable，同时还要求继承自 AggregateFunction。
// 这意味着它既具备表达式替换的功能，又具备聚合函数的特性
trait RuntimeReplaceableAggregate extends RuntimeReplaceable { self: AggregateFunction =>
  override def aggBufferSchema: StructType = {
    throw SparkException.internalError(
      "RuntimeReplaceableAggregate.aggBufferSchema should not be called")
  }
  override def aggBufferAttributes: Seq[AttributeReference] = {
    throw SparkException.internalError(
      "RuntimeReplaceableAggregate.aggBufferAttributes should not be called")
  }
  override def inputAggBufferAttributes: Seq[AttributeReference] = {
    throw SparkException.internalError(
      "RuntimeReplaceableAggregate.inputAggBufferAttributes should not be called")
  }
}

/**
 * Expressions that don't have SQL representation should extend this trait.  Examples are
 * `ScalaUDF`, `ScalaUDAF`, and object expressions like `MapObjects` and `Invoke`.
 */
//用于表示没有 SQL 表达式表示的表达式。这些表达式通常无法直接转换为 SQL 语法，但它们仍然是 Spark 查询执行的一部分。典型的例子包括 ScalaUDF（用户定义的函数）、
// ScalaUDAF（用户定义的聚合函数）以及像 MapObjects 和 Invoke 这样的对象表达式
trait NonSQLExpression extends Expression {
  final override def sql: String = {
    transform {
      case a: Attribute => new PrettyAttribute(a)
      case a: Alias => PrettyAttribute(a.sql, a.dataType)
      case p: PythonFuncExpression => PrettyPythonUDF(p.name, p.dataType, p.children)
    }.toString
  }
}


/**
 * An expression that is nondeterministic.
 */
//非确定性表达式在不同的计算中可能产生不同的结果，这通常是因为它们依赖于外部因素，如随机数生成器、系统时间等
trait Nondeterministic extends Expression {
  final override lazy val deterministic: Boolean = false
  final override def foldable: Boolean = false
 //用于跟踪表达式是否已被初始化
  @transient
  private[this] var initialized = false

  /**
   * Initializes internal states given the current partition index and mark this as initialized.
   * Subclasses should override [[initializeInternal()]].
   */
  final def initialize(partitionIndex: Int): Unit = {
    initializeInternal(partitionIndex)
    initialized = true
  }

  protected def initializeInternal(partitionIndex: Int): Unit

  /**
   * @inheritdoc
   * Throws an exception if [[initialize()]] is not called yet.
   * Subclasses should override [[evalInternal()]].
   */
  final override def eval(input: InternalRow = null): Any = {
    require(initialized,
      s"Nondeterministic expression ${this.getClass.getName} should be initialized before eval.")
    evalInternal(input)
  }
  //子类需要实现它来执行表达式的计算
  protected def evalInternal(input: InternalRow): Any
}

/**
 * An expression that contains conditional expression branches, so not all branches will be hit.
 * All optimization should be careful with the evaluation order.
 */
//表示包含条件分支的表达式的特质。此特质主要用于那些在执行时根据条件分支的表达式，在某些情况下某些分支可能不会被执行。
// 一个经典的例子是 if-else 表达式、case 表达式或者其他形式的条件判断语句
trait ConditionalExpression extends Expression {
  //如果所有子表达式都是可折叠的（即常量表达式），那么这个表达式也是可折叠的
  final override def foldable: Boolean = children.forall(_.foldable)

  /**
   * Return the children expressions which can always be hit at runtime.
   */
  //一个包含条件分支的表达式来说，有些分支会在运行时必然被执行，而另一些则根据条件决定是否执行。这个方法帮助确定哪些子表达式总是会被评估
  def alwaysEvaluatedInputs: Seq[Expression]

  /**
   * Return groups of branches. For each group, at least one branch will be hit at runtime,
   * so that we can eagerly evaluate the common expressions of a group.
   */
  //每个分支组包含一组条件分支，其中至少一个分支会在运行时被执行。这个方法允许在优化时，针对每个分支组进行“提前计算”优化，将常见的子表达式提前计算，从而避免重复计算
  def branchGroups: Seq[Seq[Expression]]
}

/**
 * A leaf expression, i.e. one without any child expressions.
 */
//叶子表达式
abstract class LeafExpression extends Expression with LeafLike[Expression]


/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 */
 //主要用于处理只有一个子表达式的运算，例如一元操作符（如取反、取绝对值等）
abstract class UnaryExpression extends Expression with UnaryLike[Expression] {
  //如果子表达式是可折叠的（即它是常量表达式），那么该表达式也会被认为是可折叠的
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  /**
   * Default behavior of evaluation according to the default nullability of UnaryExpression.
   * If subclass of UnaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of UnaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("UnaryExpressions",
      "eval", "nullSafeEval")

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * As an example, the following does a boolean inversion (i.e. NOT).
   * {{{
   *   defineCodeGen(ctx, ev, c => s"!($c)")
   * }}}
   *
   * @param f function that accepts a variable name and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.value} = ${f(eval)};"
    })
  }

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not null, use `f` to generate the expression.
   *
   * @param f function that accepts the non-null evaluation result name of child and returns Java
   *          code to compute the output.
   */
    //生成 Java 代码，确保表达式在处理 null 时能够正确返回 null，并在子表达式值非 null 时执行实际的计算
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String): ExprCode = {
    //生成子表达式的代码
    val childGen = child.genCode(ctx)
    //生成实际的计算代码
    val resultCode = f(childGen.value)

    if (nullable) {
      val nullSafeEval = ctx.nullSafeExec(child.nullable, childGen.isNull)(resultCode)
      ev.copy(code = code"""
        ${childGen.code}
        boolean ${ev.isNull} = ${childGen.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${childGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with SQL query context. The context string can be serialized from the Driver
 * to executors. It will also be kept after rule transforms.
 */
//为表达式提供 SQL 查询上下文，在分布式 SQL 执行引擎中非常有用。这个上下文包含了关于 SQL 查询的元数据等信息，
// 可以从 Driver 端序列化到 Executor 端，并且在规则转换（rule transforms）过程中保持不变
trait SupportQueryContext extends Expression with Serializable {
  //保存了一个 SQLQueryContext 类型的可选值
  protected var queryContext: Option[SQLQueryContext] = initQueryContext()

  def initQueryContext(): Option[SQLQueryContext]

  def getContextOrNull(): SQLQueryContext = queryContext.orNull
  //方法生成代码，在生成的代码中获取 SQL 查询上下文
  def getContextOrNullCode(ctx: CodegenContext, withErrorContext: Boolean = true): String = {
    if (withErrorContext && queryContext.isDefined) {
      ctx.addReferenceObj("errCtx", queryContext.get)
    } else {
      "null"
    }
  }

  // Note: Even though query contexts are serialized to executors, it will be regenerated from an
  //       empty "Origin" during rule transforms since "Origin"s are not serialized to executors
  //       for better performance. Thus, we need to copy the original query context during
  //       transforms. The query context string is considered as a "tag" on the expression here.
  override def copyTagsFrom(other: Expression): Unit = {
    other match {
      case s: SupportQueryContext =>
        queryContext = s.queryContext
      case _ =>
    }
    super.copyTagsFrom(other)
  }
}

object UnaryExpression {
  def unapply(e: UnaryExpression): Option[Expression] = Some(e.child)
}


/**
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class BinaryExpression extends Expression with BinaryLike[Expression] {

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /**
   * Default behavior of evaluation according to the default nullability of BinaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("BinaryExpressions",
      "eval", "nullSafeEval")

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"${ev.value} = ${f(eval1, eval2)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val resultCode = f(leftGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(left.nullable, leftGen.isNull) {
          rightGen.code + ctx.nullSafeExec(right.nullable, rightGen.isNull) {
            s"""
              ${ev.isNull} = false; // resultCode could change nullability.
              $resultCode
            """
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}


object BinaryExpression {
  def unapply(e: BinaryExpression): Option[(Expression, Expression)] = Some((e.left, e.right))
}


/**
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 * 2. Two inputs are expected to be of the same type. If the two inputs have different types,
 *    the analyzer will find the tightest common type and do the proper type casting.
 */
abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes with QueryErrorsBase {

  /**
   * Expected input type from both left/right child expressions, similar to the
   * [[ImplicitCastInputTypes]] trait.
   */
  def inputType: AbstractDataType

  def symbol: String

  def sqlOperator: String = symbol

  override def toString: String = s"($left $sqlOperator $right)"

  override def inputTypes: Seq[AbstractDataType] = Seq(inputType, inputType)

  override def checkInputDataTypes(): TypeCheckResult = {
    // First check whether left and right have the same type, then check if the type is acceptable.
    if (!DataTypeUtils.sameType(left.dataType, right.dataType)) {
      DataTypeMismatch(
        errorSubClass = "BINARY_OP_DIFF_TYPES",
        messageParameters = Map(
          "left" -> toSQLType(left.dataType),
          "right" -> toSQLType(right.dataType)))
    } else if (!inputType.acceptsType(left.dataType)) {
      DataTypeMismatch(
        errorSubClass = "BINARY_OP_WRONG_TYPE",
        messageParameters = Map(
          "inputType" -> toSQLType(inputType),
          "actualDataType" -> toSQLType(left.dataType)))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def sql: String = s"(${left.sql} $sqlOperator ${right.sql})"
}


object BinaryOperator {
  def unapply(e: BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
}

/**
 * An expression with three inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class TernaryExpression extends Expression with TernaryLike[Expression] {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of TernaryExpression.
   * If subclass of TernaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = first.eval(input)
    if (value1 != null) {
      val value2 = second.eval(input)
      if (value2 != null) {
        val value3 = third.eval(input)
        if (value3 != null) {
          return nullSafeEval(value1, value2, value3)
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of TernaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("TernaryExpressions",
      "eval", "nullSafeEval")

  /**
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts three variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3) => {
      s"${ev.value} = ${f(eval1, eval2, eval3)};"
    })
  }

  /**
   * Short hand for generating ternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 3 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String) => String): ExprCode = {
    val leftGen = children(0).genCode(ctx)
    val midGen = children(1).genCode(ctx)
    val rightGen = children(2).genCode(ctx)
    val resultCode = f(leftGen.value, midGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(children(0).nullable, leftGen.isNull) {
          midGen.code + ctx.nullSafeExec(children(1).nullable, midGen.isNull) {
            rightGen.code + ctx.nullSafeExec(children(2).nullable, rightGen.isNull) {
              s"""
                ${ev.isNull} = false; // resultCode could change nullability.
                $resultCode
              """
            }
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${leftGen.code}
        ${midGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with four inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
abstract class QuaternaryExpression extends Expression with QuaternaryLike[Expression] {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of QuaternaryExpression.
   * If subclass of QuaternaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value1 = first.eval(input)
    if (value1 != null) {
      val value2 = second.eval(input)
      if (value2 != null) {
        val value3 = third.eval(input)
        if (value3 != null) {
          val value4 = fourth.eval(input)
          if (value4 != null) {
            return nullSafeEval(value1, value2, value3, value4)
          }
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of QuaternaryExpression keep the
   *  default nullability, they can override this method to save null-check code.  If we need
   *  full control of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any, input4: Any): Any =
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("QuaternaryExpressions",
      "eval", "nullSafeEval")

  /**
   * Short hand for generating quaternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts four variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String) => String): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3, eval4) => {
      s"${ev.value} = ${f(eval1, eval2, eval3, eval4)};"
    })
  }

  /**
   * Short hand for generating quaternary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 4 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodegenContext,
    ev: ExprCode,
    f: (String, String, String, String) => String): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thridGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val resultCode = f(firstGen.value, secondGen.value, thridGen.value, fourthGen.value)

    if (nullable) {
      val nullSafeEval =
        firstGen.code + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thridGen.code + ctx.nullSafeExec(children(2).nullable, thridGen.isNull) {
              fourthGen.code + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                s"""
                  ${ev.isNull} = false; // resultCode could change nullability.
                  $resultCode
                """
              }
            }
          }
      }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thridGen.code}
        ${fourthGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with five inputs and one output. The output is by default evaluated to null if
 * any input is evaluated to null.
 */
abstract class QuinaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of QuinaryExpression. If
   * subclass of QuinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val v1 = exprs(0).eval(input)
    if (v1 != null) {
      val v2 = exprs(1).eval(input)
      if (v2 != null) {
        val v3 = exprs(2).eval(input)
        if (v3 != null) {
          val v4 = exprs(3).eval(input)
          if (v4 != null) {
            val v5 = exprs(4).eval(input)
            if (v5 != null) {
              return nullSafeEval(v1, v2, v3, v4, v5)
            }
          }
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation. If subclass of QuinaryExpression keep the default
   * nullability, they can override this method to save null-check code. If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(
      input1: Any,
      input2: Any,
      input3: Any,
      input4: Any,
      input5: Any): Any = {
    throw QueryExecutionErrors.notOverrideExpectedMethodsError(
      "QuinaryExpression",
      "eval",
      "nullSafeEval")
  }

  /**
   * Short hand for generating quinary evaluation code. If either of the sub-expressions is null,
   * the result of this computation is assumed to be null.
   *
   * @param f
   *   accepts seven variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String) => String): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (eval1, eval2, eval3, eval4, eval5) => {
        s"${ev.value} = ${f(eval1, eval2, eval3, eval4, eval5)};"
      })
  }

  /**
   * Short hand for generating quinary evaluation code. If either of the sub-expressions is null,
   * the result of this computation is assumed to be null.
   *
   * @param f
   *   function that accepts the 5 non-null evaluation result names of children and returns Java
   *   code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String) => String): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = children(4).genCode(ctx)
    val resultCode =
      f(firstGen.value, secondGen.value, thirdGen.value, fourthGen.value, fifthGen.value)

    if (nullable) {
      val nullSafeEval =
        firstGen.code + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thirdGen.code + ctx.nullSafeExec(children(2).nullable, thirdGen.isNull) {
              fourthGen.code + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                fifthGen.code + ctx.nullSafeExec(children(4).nullable, fifthGen.isNull) {
                  s"""
                      ${ev.isNull} = false; // resultCode could change nullability.
                      $resultCode
                      """
                }
              }
            }
          }
        }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(
        code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""",
        isNull = FalseLiteral)
    }
  }
}

/**
 * An expression with six inputs + 7th optional input and one output.
 * The output is by default evaluated to null if any input is evaluated to null.
 */
abstract class SeptenaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of SeptenaryExpression.
   * If subclass of SeptenaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val v1 = exprs(0).eval(input)
    if (v1 != null) {
      val v2 = exprs(1).eval(input)
      if (v2 != null) {
        val v3 = exprs(2).eval(input)
        if (v3 != null) {
          val v4 = exprs(3).eval(input)
          if (v4 != null) {
            val v5 = exprs(4).eval(input)
            if (v5 != null) {
              val v6 = exprs(5).eval(input)
              if (v6 != null) {
                if (exprs.length > 6) {
                  val v7 = exprs(6).eval(input)
                  if (v7 != null) {
                    return nullSafeEval(v1, v2, v3, v4, v5, v6, Some(v7))
                  }
                } else {
                  return nullSafeEval(v1, v2, v3, v4, v5, v6, None)
                }
              }
            }
          }
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of SeptenaryExpression keep the
   * default nullability, they can override this method to save null-check code.  If we need
   * full control of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(
      input1: Any,
      input2: Any,
      input3: Any,
      input4: Any,
      input5: Any,
      input6: Any,
      input7: Option[Any]): Any = {
    throw QueryExecutionErrors.notOverrideExpectedMethodsError("SeptenaryExpression",
      "eval", "nullSafeEval")
  }

  /**
   * Short hand for generating septenary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts seven variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String, String, Option[String]) => String
    ): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3, eval4, eval5, eval6, eval7) => {
      s"${ev.value} = ${f(eval1, eval2, eval3, eval4, eval5, eval6, eval7)};"
    })
  }

  /**
   * Short hand for generating septenary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 7 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, String, String, Option[String]) => String
    ): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = children(4).genCode(ctx)
    val sixthGen = children(5).genCode(ctx)
    val seventhGen = if (children.length > 6) Some(children(6).genCode(ctx)) else None
    val resultCode = f(
      firstGen.value,
      secondGen.value,
      thirdGen.value,
      fourthGen.value,
      fifthGen.value,
      sixthGen.value,
      seventhGen.map(_.value))

    if (nullable) {
      val nullSafeEval =
        firstGen.code + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thirdGen.code + ctx.nullSafeExec(children(2).nullable, thirdGen.isNull) {
              fourthGen.code + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                fifthGen.code + ctx.nullSafeExec(children(4).nullable, fifthGen.isNull) {
                  sixthGen.code + ctx.nullSafeExec(children(5).nullable, sixthGen.isNull) {
                    val nullSafeResultCode =
                      s"""
                      ${ev.isNull} = false; // resultCode could change nullability.
                      $resultCode
                      """
                    seventhGen.map { gen =>
                      gen.code + ctx.nullSafeExec(children(6).nullable, gen.isNull) {
                        nullSafeResultCode
                      }
                    }.getOrElse(nullSafeResultCode)
                  }
                }
              }
            }
          }
        }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
    } else {
      ev.copy(code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${fifthGen.code}
        ${sixthGen.code}
        ${seventhGen.map(_.code).getOrElse("")}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

/**
 * A trait used for resolving nullable flags, including `nullable`, `containsNull` of [[ArrayType]]
 * and `valueContainsNull` of [[MapType]], containsNull, valueContainsNull flags of the output date
 * type. This is usually utilized by the expressions (e.g. [[CaseWhen]]) that combine data from
 * multiple child expressions of non-primitive types.
 */
trait ComplexTypeMergingExpression extends Expression {

  /**
   * A collection of data types used for resolution the output type of the expression. By default,
   * data types of all child expressions. The collection must not be empty.
   */
  @transient
  lazy val inputTypesForMerging: Seq[DataType] = children.map(_.dataType)

  def dataTypeCheck: Unit = {
    require(
      inputTypesForMerging.nonEmpty,
      "The collection of input data types must not be empty.")
    require(
      TypeCoercion.haveSameType(inputTypesForMerging),
      "All input types must be the same except nullable, containsNull, valueContainsNull flags. " +
        s"The expression is: $this. " +
        s"The input types found are\n\t${inputTypesForMerging.mkString("\n\t")}.")
  }

  private lazy val internalDataType: DataType = {
    dataTypeCheck
    inputTypesForMerging.reduceLeft(TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(_, _).get)
  }

  override def dataType: DataType = internalDataType
}

/**
 * Common base trait for user-defined functions, including UDF/UDAF/UDTF of different languages
 * and Hive function wrappers.
 */
trait UserDefinedExpression {
  def name: String
}

trait CommutativeExpression extends Expression {
  /** Collects adjacent commutative operations. */
  private def gatherCommutative(
      e: Expression,
      f: PartialFunction[CommutativeExpression, Seq[Expression]]): Seq[Expression] = e match {
    case c: CommutativeExpression if f.isDefinedAt(c) => f(c).flatMap(gatherCommutative(_, f))
    case other => other.canonicalized :: Nil
  }

  /**
   * Reorders adjacent commutative operators such as [[And]] in the expression tree, according to
   * the `hashCode` of non-commutative nodes, to remove cosmetic variations.
   */
  protected def orderCommutative(
      f: PartialFunction[CommutativeExpression, Seq[Expression]]): Seq[Expression] =
    gatherCommutative(this, f).sortBy(_.hashCode())

  /**
   * Helper method to generated a canonicalized plan. If the number of operands are
   * greater than the MULTI_COMMUTATIVE_OP_OPT_THRESHOLD, this method creates a
   * [[MultiCommutativeOp]] as the canonicalized plan.
   */
  protected def buildCanonicalizedPlan(
      collectOperands: PartialFunction[Expression, Seq[Expression]],
      buildBinaryOp: (Expression, Expression) => Expression,
      evalMode: Option[EvalMode.Value] = None): Expression = {
    val operands = orderCommutative(collectOperands)
    val reorderResult =
      if (operands.length < SQLConf.get.getConf(MULTI_COMMUTATIVE_OP_OPT_THRESHOLD)) {
        operands.reduce(buildBinaryOp)
      } else {
        MultiCommutativeOp(operands, this.getClass, evalMode)(this)
      }
    reorderResult
  }
}

/**
 * A helper class used by the Commutative expressions during canonicalization. During
 * canonicalization, when we have a long tree of commutative operations, we use the MultiCommutative
 * expression to represent that tree instead of creating new commutative objects.
 * This class is added as a memory optimization for processing large commutative operation trees
 * without creating a large number of new intermediate objects.
 * The MultiCommutativeOp memory optimization is applied to the following commutative
 * expressions:
 *      Add, Multiply, And, Or, BitwiseAnd, BitwiseOr, BitwiseXor.
 * @param operands A sequence of operands that produces a commutative expression tree.
 * @param opCls The class of the root operator of the expression tree.
 * @param evalMode The optional expression evaluation mode.
 * @param originalRoot Root operator of the commutative expression tree before canonicalization.
 *                     This object reference is used to deduce the return dataType of Add and
 *                     Multiply operations when the input datatype is decimal.
 */
case class MultiCommutativeOp(
    operands: Seq[Expression],
    opCls: Class[_],
    evalMode: Option[EvalMode.Value])(originalRoot: Expression) extends Unevaluable {
  // Helper method to deduce the data type of a single operation.
  private def singleOpDataType(lType: DataType, rType: DataType): DataType = {
    originalRoot match {
      case add: Add =>
        (lType, rType) match {
          case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
            add.resultDecimalType(p1, s1, p2, s2)
          case _ => lType
        }
      case multiply: Multiply =>
        (lType, rType) match {
          case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
            multiply.resultDecimalType(p1, s1, p2, s2)
          case _ => lType
        }
    }
  }

  override def dataType: DataType = {
    originalRoot match {
      case _: Add | _: Multiply =>
        operands.map(_.dataType).reduce((l, r) => singleOpDataType(l, r))
      case other => other.dataType
    }
  }

  override def nullable: Boolean = operands.exists(_.nullable)

  override def children: Seq[Expression] = operands

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    this.copy(operands = newChildren)(originalRoot)

  override protected final def otherCopyArgs: Seq[AnyRef] = originalRoot :: Nil
}
