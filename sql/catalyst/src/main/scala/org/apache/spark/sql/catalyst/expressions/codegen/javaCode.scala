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

package org.apache.spark.sql.catalyst.expressions.codegen

import java.lang.{Boolean => JBool}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.trees.{LeafLike, TreeNode}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * Trait representing an opaque fragments of java code.
 */
//表示 Java 代码片段
trait JavaCode {
  def code: String  //返回 Java 代码的字符串表示
  override def toString: String = code
}

/**
 * Utility functions for creating [[JavaCode]] fragments.
 */
object JavaCode {
  /**
   * Create a java literal.
   */
    //创建一个 Java 字面量，基于输入的值和数据类型
  def literal(v: String, dataType: DataType): LiteralValue = dataType match {
    case BooleanType if v == "true" => TrueLiteral
    case BooleanType if v == "false" => FalseLiteral
    case _ => new LiteralValue(v, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a default literal. This is null for reference types, false for boolean types and
   * -1 for other primitive types.
   */
    //创建一个默认的字面量，对于引用类型为 null，对于布尔类型为 false，对于其他原始类型为 -1
  def defaultLiteral(dataType: DataType): LiteralValue = {
    new LiteralValue(
      CodeGenerator.defaultValue(dataType, typedNull = true),
      CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a local java variable.
   */
    //创建一个局部变量
  def variable(name: String, dataType: DataType): VariableValue = {
    variable(name, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a local java variable.
   */
    //创建一个局部变量，使用指定的 Java 类
  def variable(name: String, javaClass: Class[_]): VariableValue = {
    VariableValue(name, javaClass)
  }

  /**
   * Create a local isNull variable.
   */
    //创建一个 isNull 局部变量，布尔类型
  def isNullVariable(name: String): VariableValue = variable(name, BooleanType)

  /**
   * Create a global java variable.
   */
  def global(name: String, dataType: DataType): GlobalValue = {
    global(name, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create a global java variable.
   */
    //创建一个全局变量，使用指定的 Java 类
  def global(name: String, javaClass: Class[_]): GlobalValue = {
    GlobalValue(name, javaClass)
  }

  /**
   * Create a global isNull variable.
   */
    //创建一个 isNull 全局变量，布尔类型
  def isNullGlobal(name: String): GlobalValue = global(name, BooleanType)

  /**
   * Create an expression fragment.
   */
    //创建一个表达式片段
  def expression(code: String, dataType: DataType): SimpleExprValue = {
    expression(code, CodeGenerator.javaClass(dataType))
  }

  /**
   * Create an expression fragment.
   */
    //创建一个表达式片段，使用指定的 Java 类
  def expression(code: String, javaClass: Class[_]): SimpleExprValue = {
    SimpleExprValue(code, javaClass)
  }

  /**
   * Create a isNull expression fragment.
   */
    //创建一个 isNull 表达式片段，布尔类型
  def isNullExpression(code: String): SimpleExprValue = {
    expression(code, BooleanType)
  }

  /**
   * Create an `Inline` for Java Class name.
   */
    //创建一个包含 Java 类名称的 Inline
  def javaType(javaClass: Class[_]): Inline = Inline(javaClass.getName)

  /**
   * Create an `Inline` for Java Type name.
   */
    //创建一个包含数据类型对应的 Java 类型名称的 Inline
  def javaType(dataType: DataType): Inline = Inline(CodeGenerator.javaType(dataType))

  /**
   * Create an `Inline` for boxed Java Type name.
   */
  def boxedType(dataType: DataType): Inline = Inline(CodeGenerator.boxedType(dataType))
}

/**
 * A trait representing a block of java code.
 */
//表示 Java 代码块的 trait
trait Block extends TreeNode[Block] with JavaCode {
  import Block._

  // Returns java code string for this code block.
  override def toString: String = _marginChar match {
    case Some(c) => code.stripMargin(c).trim
    case _ => code.trim
  }

  // We could remove comments, extra whitespaces and newlines when calculating length as it is used
  // only for codegen method splitting, but SPARK-30564 showed that this is a performance critical
  // function so we decided not to do so.
  //计算当前代码块的字符串长度（去除多余的空格和换行符）
  def length: Int = toString.length
  //判断代码块是否为空
  def isEmpty: Boolean = toString.isEmpty

  def nonEmpty: Boolean = !isEmpty

  // The leading prefix that should be stripped from each line.
  // By default we strip blanks or control characters followed by '|' from the line.
  //存储代码行首需要去除的字符。默认值是 '|'，用于 stripMargin 方法去除每行开头的某些字符
  var _marginChar: Option[Char] = Some('|')
  //设置行首去除的字符为 c，并返回当前 Block 对象
  def stripMargin(c: Char): this.type = {
    _marginChar = Some(c)
    this
  }
  //使用默认的｜
  def stripMargin: this.type = {
    _marginChar = Some('|')
    this
  }

  /**
   * Apply a map function to each java expression codes present in this java code, and return a new
   * java code based on the mapped java expression codes.
   */
    //作用是对当前 Java 代码中的表达式进行映射转换
  def transformExprValues(f: PartialFunction[ExprValue, ExprValue]): this.type = {
    var changed = false

    @inline def transform(e: ExprValue): ExprValue = {
      val newE = f lift e   //把f函数应用到e表达式上
      if (!newE.isDefined || newE.get.equals(e)) {  //如果结果是None或者跟原来一样，直接返回
        e
      } else {  //范泽返回新表达式的结果
        changed = true
        newE.get
      }
    }

    def doTransform(arg: Any): AnyRef = arg match {
      case e: ExprValue => transform(e)   //如果是单个表达式，直接应用transform
      case Some(value) => Some(doTransform(value))   //如果是Option
      case seq: Iterable[_] => seq.map(doTransform)   //如果是迭代器
      case other: AnyRef => other   //其他直接返回
    }
    //把doTransform函数应用到所有元素上，如果有改变，则用返回值构造新的表达式
    val newArgs = mapProductIterator(doTransform)
    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  // Concatenates this block with other block.
  //将当前代码块与另一个代码块拼接，形成一个新的代码块
  def + (other: Block): Block = other match {
    case EmptyBlock => this
    case _ => code"$this\n$other"
  }

  override def verboseString(maxFields: Int): String = toString
  override def simpleStringWithNodeId(): String = {
    throw new IllegalStateException(s"$nodeName does not implement simpleStringWithNodeId")
  }
}

object Block {
  //用于代码块缓存区的长度（512）
  val CODE_BLOCK_BUFFER_LENGTH: Int = 512

  /**
   * A custom string interpolator which inlines a string into code block.
   */
  //StringContext类用于处理插值字符串（interpolated strings）。
  // 当你使用字符串插值（如s"..."）时，StringContext类帮助构建最终的字符串
  //提供了一个关键方法parts，它返回所有插值字符串的部分，即未插入任何变量的静态部分
  //val name = "Alice"
  //val age = 30
  //val greeting = s"Hello, my name is $name and I am $age years old."
  implicit class InlineHelper(val sc: StringContext) extends AnyVal {
    def inline(args: Any*): Inline = {
      //接收可变参数 args（类型为 Any），表示插入到字符串中的参数
      val inlineString = sc.raw(args: _*)
      //raw 方法不会对参数进行任何转义（即不处理特殊字符的转义，如换行符等），直接将参数作为原始字符串插入到最终字符串中
      //可以这么使用val code = inline"val result = $x + $y"
      Inline(inlineString)
    }
  }
  //隐式地把所有代码块拼接在一起
  implicit def blocksToBlock(blocks: Seq[Block]): Block = blocks.reduceLeft(_ + _)
  //定义了一个 BlockHelper 隐式类，它提供了一个 code 方法，
  // 用于扩展 StringContext 的功能，允许通过自定义字符串插值生成 Block 类型的代码块
  implicit class BlockHelper(val sc: StringContext) extends AnyVal {
    /**
     * A string interpolator that retains references to the `JavaCode` inputs, and behaves like
     * the Scala builtin StringContext.s() interpolator otherwise, i.e. it will treat escapes in
     * the code parts, and will not treat escapes in the input arguments.
     */
    def code(args: Any*): Block = {
      sc.checkLengths(args)
      if (sc.parts.length == 0) {
        EmptyBlock
      } else {
        //遍历所有输入参数 args，并检查它们的类型
        //允许的类型包括下面除了other
        args.foreach {
          case _: ExprValue | _: Inline | _: Block =>
          case _: Boolean | _: Byte | _: Int | _: Long | _: Float | _: Double | _: String =>
          case other => throw QueryExecutionErrors.cannotInterpolateClassIntoCodeBlockError(other)
        }

        val (codeParts, blockInputs) = foldLiteralArgs(sc.parts, args)
        CodeBlock(codeParts, blockInputs)
      }
    }
  }

  // Folds eagerly the literal args into the code parts.
  //用于将字符串插值中的字面量（literal）参数与代码片段结合起来，并返回一个包含代码部分和输入的元组。
  // 这个方法是代码生成过程中对插值字符串与参数的处理
  private def foldLiteralArgs(parts: Seq[String], args: Seq[Any]): (Seq[String], Seq[JavaCode]) = {
    //parts: Seq[String]：一个字符串序列，表示由 StringContext 处理后的字符串插值的不同部分
    //args: Seq[Any]：一个包含插值参数的序列，它们将插入到 parts 对应的位置中。
    val codeParts = ArrayBuffer.empty[String]
    val blockInputs = ArrayBuffer.empty[JavaCode]

    val strings = parts.iterator
    val inputs = args.iterator
    val buf = new StringBuilder(Block.CODE_BLOCK_BUFFER_LENGTH)
    //处理 parts 中的第一个字符串部分，并将其添加到 buf 中。
    // StringContext.treatEscapes 方法用于处理字符串中的转义字符（例如 \n）
    buf.append(StringContext.treatEscapes(strings.next))
    while (strings.hasNext) {
      val input = inputs.next
      input match {
        //如果 input 是 ExprValue 或 CodeBlock 类型
        case _: ExprValue | _: CodeBlock =>
          codeParts += buf.toString
          buf.clear
          blockInputs += input.asInstanceOf[JavaCode]
        case EmptyBlock =>
        case _ =>  //其他继续追加到buf里面
          buf.append(input)
      }
      buf.append(StringContext.treatEscapes(strings.next))
    }
    codeParts += buf.toString

    (codeParts.toSeq, blockInputs.toSeq)
  }
}

/**
 * A block of java code. Including a sequence of code parts and some inputs to this block.
 * The actual java code is generated by embedding the inputs into the code parts. Here we keep
 * inputs of `JavaCode` instead of simply folding them as a string of code, because we need to
 * track expressions (`ExprValue`) in this code block. We need to be able to manipulate the
 * expressions later without changing the behavior of this code block in some applications, e.g.,
 * method splitting.
 */
case class CodeBlock(codeParts: Seq[String], blockInputs: Seq[JavaCode]) extends Block {
  //codeParts：是代码的字面部分，包含一系列字符串
  //blockInputs：是 JavaCode 类型的元素，表示嵌入到字面部分中的表达式或代码片段，这些片段会被插入到代码中

  //会过滤 blockInputs 中所有是 Block 类型的元素，并将它们作为 Seq[Block] 返回
  override def children: Seq[Block] =
    blockInputs.filter(_.isInstanceOf[Block]).asInstanceOf[Seq[Block]]

  override lazy val code: String = {
    val strings = codeParts.iterator
    val inputs = blockInputs.iterator
    val buf = new StringBuilder(Block.CODE_BLOCK_BUFFER_LENGTH)
    buf.append(strings.next)
    while (strings.hasNext) {
      buf.append(inputs.next)
      buf.append(strings.next)
    }
    buf.toString
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Block]): Block =
    super.legacyWithNewChildren(newChildren)
}

case object EmptyBlock extends Block with Serializable with LeafLike[Block] {
  override val code: String = ""
}

/**
 * A piece of java code snippet inlines all types of input arguments into a string without
 * tracking any reference of `JavaCode` instances.
 */
//简单的 Java 代码片段类，包含原始的字符串代码，不会追踪任何 JavaCode 实例
case class Inline(codeString: String) extends JavaCode {
  override val code: String = codeString
}

/**
 * A typed java fragment that must be a valid java expression.
 */
//表示有效 Java 表达式的 trait，包含 Java 类型信息
trait ExprValue extends JavaCode {
  def javaType: Class[_]   //java的类型
  def isPrimitive: Boolean = javaType.isPrimitive  //判断表达式的 Java 类型是否为原始类型
}

object ExprValue {
  implicit def exprValueToString(exprValue: ExprValue): String = exprValue.code
}

/**
 * A java expression fragment.
 */
//表示一个简单的 Java 表达式
case class SimpleExprValue(expr: String, javaType: Class[_]) extends ExprValue {
  override def code: String = s"($expr)"
}

/**
 * A local variable java expression.
 */
//表示一个局部变量的 Java 表达式
case class VariableValue(variableName: String, javaType: Class[_]) extends ExprValue {
  override def code: String = variableName
}

/**
 * A global variable java expression.
 */
//表示一个全局变量的 Java 表达式
case class GlobalValue(value: String, javaType: Class[_]) extends ExprValue {
  override def code: String = value
}

/**
 * A literal java expression.
 */
//表示一个字面量的 Java 表达式
class LiteralValue(val value: String, val javaType: Class[_]) extends ExprValue with Serializable {
  override def code: String = value

  override def equals(arg: Any): Boolean = arg match {
    case l: LiteralValue => l.javaType == javaType && l.value == value
    case _ => false
  }

  override def hashCode(): Int = value.hashCode() * 31 + javaType.hashCode()
}

case object TrueLiteral extends LiteralValue("true", JBool.TYPE)
case object FalseLiteral extends LiteralValue("false", JBool.TYPE)
