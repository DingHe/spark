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
package org.apache.spark.sql.catalyst.parser

import scala.collection.JavaConverters._

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree.TerminalNodeImpl

import org.apache.spark.{QueryContext, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, WithOrigin}
import org.apache.spark.sql.catalyst.util.SparkParserUtils
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Base SQL parsing infrastructure.
 */
//涉及到 SQL 语句的解析、错误处理、语法检查等多个方面
abstract class AbstractParser extends DataTypeParserInterface with Logging {
  /** Creates/Resolves DataType for a given SQL string. */
    //解析 SQL 字符串并生成 DataType 对象。DataType 是 Spark SQL 中表示数据类型的类
  override def parseDataType(sqlText: String): DataType = parse(sqlText) { parser =>
    astBuilder.visitSingleDataType(parser.singleDataType())
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
    //解析 SQL 字符串并生成 StructType 对象。StructType 是 Spark SQL 中表示表结构的类，通常用来描述一个表的列和类型
  override def parseTableSchema(sqlText: String): StructType = parse(sqlText) { parser =>
    astBuilder.visitSingleTableSchema(parser.singleTableSchema())
  }

  /** Get the builder (visitor) which converts a ParseTree into an AST. */
  protected def astBuilder: DataTypeAstBuilder

  protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    //command: String：待解析的 SQL 命令
    //toResult: SqlBaseParser => T：一个高阶函数，接受 SqlBaseParser 类型的解析器并返回一个结果 T，即解析完成后的结果
    logDebug(s"Parsing command: $command")
    //SqlBaseLexer 是通过 ANTLR 自动生成的词法分析器，用于将 SQL 命令转换为一个个的词法单元（tokens）
    //通过 UpperCaseCharStream 将 SQL 命令转换为大写流，确保词法分析不区分大小写
    //移除默认的错误监听器，并添加自定义的 ParseErrorListener 来捕捉解析错误
    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    //词法分析器 lexer 生成的词法单元（tokens）会被 CommonTokenStream 包装，这样可以传递给语法解析器使用
    val tokenStream = new CommonTokenStream(lexer)

    //创建一个语法解析器 SqlBaseParser，它基于词法单元流（tokenStream）进行语法分析
    val parser = new SqlBaseParser(tokenStream)
    //将一个 ParseTreeListener 添加到解析器中，以便在解析过程中进行回调操作。
    // 这个方法允许你在语法树的构建过程中插入自定义的监听器，监听特定的规则匹配事件，并在相应的事件发生时执行特定的操作
    parser.addParseListener(PostProcessor)
    parser.addParseListener(UnclosedCommentProcessor(command, tokenStream))

    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser.legacy_setops_precedence_enabled = conf.setOpsPrecedenceEnforced
    parser.legacy_exponent_literal_as_decimal_enabled = conf.exponentLiteralAsDecimalEnabled
    parser.SQL_standard_keyword_behavior = conf.enforceReservedKeywords
    parser.double_quoted_identifiers = conf.doubleQuotedIdentifiers

    // https://github.com/antlr/antlr4/issues/192#issuecomment-15238595
    // Save a great deal of time on correct inputs by using a two-stage parsing strategy.
    try {
      try {
        // first, try parsing with potentially faster SLL mode w/ SparkParserBailErrorStrategy
        //设置 SparkParserBailErrorStrategy 作为错误处理器，如果发生解析错误，则立即退出并抛出异常
        parser.setErrorHandler(new SparkParserBailErrorStrategy())
        //使用 SLL（Singly Lookahead） 模式进行解析。这个模式通常比 LL（Lookahead） 模式快
        //Singly Lookahead 模式（也叫 1-Token Lookahead）是一种语法分析策略，指的是在分析某个输入符号时，
        // 分析器只查看下一个输入符号（即一个 token）来决定如何进行解析
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode w/ SparkParserErrorStrategy
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          //LL(k) 语法分析是指一种通过从左到右扫描输入符号（Left-to-right），
          // 并进行最左推导（Leftmost derivation）来进行解析的技术，k 表示 Lookahead 的符号数，
          // 即在做决策时，解析器查看的输入符号的个数。
          //
          //LL(1): 这是最常见的形式，解析器在每次决策时查看一个符号。
          //LL(k): 解析器在每次决策时查看 k 个符号
          parser.setErrorHandler(new SparkParserErrorStrategy())
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: SparkThrowable with WithOrigin =>
        throw new ParseException(
          command = Option(command),
          message = e.getMessage,
          start = e.origin,
          stop = e.origin,
          errorClass = Option(e.getErrorClass),
          messageParameters = e.getMessageParameters.asScala.toMap,
          queryContext = e.getQueryContext)
    }
  }

  private def conf: SqlApiConf = SqlApiConf.get
}

/**
 * This string stream provides the lexer with upper case characters only. This greatly simplifies
 * lexing the stream, while we can maintain the original command.
 *
 * This is based on Hive's org.apache.hadoop.hive.ql.parse.ParseDriver.ANTLRNoCaseStringStream
 *
 * The comment below (taken from the original class) describes the rationale for doing this:
 *
 * This class provides and implementation for a case insensitive token checker for the lexical
 * analysis part of antlr. By converting the token stream into upper case at the time when lexical
 * rules are checked, this class ensures that the lexical rules need to just match the token with
 * upper case letters as opposed to combination of upper case and lower case characters. This is
 * purely used for matching lexical rules. The actual token text is stored in the same way as the
 * user input without actually converting it into an upper case. The token values are generated by
 * the consume() function of the super class ANTLRStringStream. The LA() function is the lookahead
 * function and is purely used for matching lexical rules. This also means that the grammar will
 * only accept capitalized tokens in case it is run from other tools like antlrworks which do not
 * have the UpperCaseCharStream implementation.
 */
//CodePointCharStream 是一个实现了 CharStream 接口的类，通常用于处理 Unicode 字符流，
// 它比传统的 ANTLRInputStream 更加适用于处理多字节字符集，如 UTF-8 或 UTF-16 编码。
// CodePointCharStream 允许你处理 Unicode 代码点而不是字节流，这对于支持多语言的解析器非常重要
//CodePointCharStream 可以通过 CharStreams.fromString() 或 CharStreams.fromFileName() 等方法来初始化
private[parser] class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  //消费当前字符并将流指针移至下一个字符
  override def consume(): Unit = wrapped.consume
  //获取被包装流的源名称
  override def getSourceName(): String = wrapped.getSourceName
  //获取当前流的位置（索引）
  override def index(): Int = wrapped.index
  //标记当前流的位置
  override def mark(): Int = wrapped.mark
  //释放先前标记的位置
  override def release(marker: Int): Unit = wrapped.release(marker)
  //移动流指针到指定位置
  override def seek(where: Int): Unit = wrapped.seek(where)
  //返回流的大小
  override def size(): Int = wrapped.size
  //返回指定区间内的文本
  override def getText(interval: Interval): String = wrapped.getText(interval)

  override def LA(i: Int): Int = {
    ////查看指定位置的字符（Unicode 代码点）
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)   //转为大写
  }
}

/**
 * The ParseErrorListener converts parse errors into ParseExceptions.
 */
//BaseErrorListener主要用于在解析过程中捕获错误，并根据需要执行自定义操作。它并不会自动处理错误，而是为你提供了一个钩子，
// 使你能够根据自己的需求处理错误（例如打印错误消息、记录日志、抛出异常等）

case object ParseErrorListener extends BaseErrorListener {
  //这是最常用的错误监听方法，用来处理语法错误
  override def syntaxError(
      recognizer: Recognizer[_, _], //触发错误的解析器实例（Parser 或 Lexer）
      offendingSymbol: scala.Any, //引起错误的符号（通常是某个 token）
      line: Int, //发生错误的行号
      charPositionInLine: Int,  //发生错误的字符位置
      msg: String, //错误消息
      e: RecognitionException): Unit = { //RecognitionException 异常，包含关于错误的更多信息
    val (start, stop) = offendingSymbol match {
      case token: CommonToken =>
        val start = Origin(Some(line), Some(token.getCharPositionInLine))
        val length = token.getStopIndex - token.getStartIndex + 1
        val stop = Origin(Some(line), Some(token.getCharPositionInLine + length))
        (start, stop)
      case _ =>
        val start = Origin(Some(line), Some(charPositionInLine))
        (start, start)
    }
    e match {
      case sre: SparkRecognitionException if sre.errorClass.isDefined =>
        throw new ParseException(None, start, stop, sre.errorClass.get, sre.messageParameters)
      case _ =>
        throw new ParseException(None, msg, start, stop)
    }
  }
}

/**
 * A [[ParseException]] is an [[SparkException]] that is thrown during the parse process. It
 * contains fields and an extended error message that make reporting and diagnosing errors easier.
 */
class ParseException(
    val command: Option[String],
    message: String,
    val start: Origin,
    val stop: Origin,
    errorClass: Option[String] = None,
    messageParameters: Map[String, String] = Map.empty,
    queryContext: Array[QueryContext] = ParseException.getQueryContext())
  extends AnalysisException(
    message,
    start.line,
    start.startPosition,
    None,
    errorClass,
    messageParameters,
    queryContext) {

  def this(errorClass: String, messageParameters: Map[String, String], ctx: ParserRuleContext) =
    this(Option(SparkParserUtils.command(ctx)),
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      SparkParserUtils.position(ctx.getStart),
      SparkParserUtils.position(ctx.getStop),
      Some(errorClass),
      messageParameters)

  def this(errorClass: String, ctx: ParserRuleContext) = this(errorClass, Map.empty, ctx)

  /** Compose the message through SparkThrowableHelper given errorClass and messageParameters. */
  def this(
      command: Option[String],
      start: Origin,
      stop: Origin,
      errorClass: String,
      messageParameters: Map[String, String]) =
    this(
      command,
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      start,
      stop,
      Some(errorClass),
      messageParameters)

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(Some(l), Some(p), _, _, _, _, _) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach { cmd =>
          val (above, below) = cmd.split("\n").splitAt(l)
          builder ++= "\n== SQL ==\n"
          above.foreach(builder ++= _ += '\n')
          builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
          below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach { cmd =>
          builder ++= "\n== SQL ==\n" ++= cmd
        }
    }
    builder.toString
  }

  def withCommand(cmd: String): ParseException = {
    val (cls, params) =
      if (errorClass == Some("PARSE_SYNTAX_ERROR") && cmd.trim().isEmpty) {
        // PARSE_EMPTY_STATEMENT error class overrides the PARSE_SYNTAX_ERROR when cmd is empty
        (Some("PARSE_EMPTY_STATEMENT"), Map.empty[String, String])
      } else {
        (errorClass, messageParameters)
      }
    new ParseException(Option(cmd), message, start, stop, cls, params, queryContext)
  }

  override def getQueryContext: Array[QueryContext] = queryContext
}

object ParseException {
  def getQueryContext(): Array[QueryContext] = {
    val context = CurrentOrigin.get.context
    if (context.isValid) Array(context) else Array.empty
  }
}

/**
 * The post-processor validates & cleans-up the parse tree during the parse process.
 */
case object PostProcessor extends SqlBaseParserBaseListener {

  /** Throws error message when exiting a explicitly captured wrong identifier rule */
  override def exitErrorIdent(ctx: SqlBaseParser.ErrorIdentContext): Unit = {
    val ident = ctx.getParent.getText

    throw QueryParsingErrors.invalidIdentifierError(ident, ctx)
  }

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext): Unit = {
    if (ctx.BACKQUOTED_IDENTIFIER() != null) {
      replaceTokenByIdentifier(ctx, 1) { token =>
        // Remove the double back ticks in the string.
        token.setText(token.getText.replace("``", "`"))
        token
      }
    } else if (ctx.DOUBLEQUOTED_STRING() != null) {
      replaceTokenByIdentifier(ctx, 1) { token =>
        // Remove the double quotes in the string.
        token.setText(token.getText.replace("\"\"", "\""))
        token
      }
    }
  }

  /** Remove the back ticks from an Identifier. */
  override def exitBackQuotedIdentifier(ctx: SqlBaseParser.BackQuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: SqlBaseParser.NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(
      ctx: ParserRuleContext,
      stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      SqlBaseParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}

/**
 * The post-processor checks the unclosed bracketed comment.
 */
case class UnclosedCommentProcessor(
    command: String, tokenStream: CommonTokenStream) extends SqlBaseParserBaseListener {

  override def exitSingleDataType(ctx: SqlBaseParser.SingleDataTypeContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleExpression(ctx: SqlBaseParser.SingleExpressionContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleTableIdentifier(ctx: SqlBaseParser.SingleTableIdentifierContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleFunctionIdentifier(
      ctx: SqlBaseParser.SingleFunctionIdentifierContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleMultipartIdentifier(
      ctx: SqlBaseParser.SingleMultipartIdentifierContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleTableSchema(ctx: SqlBaseParser.SingleTableSchemaContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitQuery(ctx: SqlBaseParser.QueryContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleStatement(ctx: SqlBaseParser.SingleStatementContext): Unit = {
    // SET command uses a wildcard to match anything, and we shouldn't parse the comments, e.g.
    // `SET myPath =/a/*`.
    if (!ctx.statement().isInstanceOf[SqlBaseParser.SetConfigurationContext]) {
      checkUnclosedComment(tokenStream, command)
    }
  }

  /** check `has_unclosed_bracketed_comment` to find out the unclosed bracketed comment. */
  private def checkUnclosedComment(tokenStream: CommonTokenStream, command: String) = {
    assert(tokenStream.getTokenSource.isInstanceOf[SqlBaseLexer])
    val lexer = tokenStream.getTokenSource.asInstanceOf[SqlBaseLexer]
    if (lexer.has_unclosed_bracketed_comment) {
      // The last token is 'EOF' and the penultimate is unclosed bracketed comment
      val failedToken = tokenStream.get(tokenStream.size() - 2)
      assert(failedToken.getType() == SqlBaseParser.BRACKETED_COMMENT)
      val position = Origin(Option(failedToken.getLine), Option(failedToken.getCharPositionInLine))
      throw QueryParsingErrors.unclosedBracketedCommentError(
        command = command,
        start = Origin(Option(failedToken.getStartIndex)),
        stop = Origin(Option(failedToken.getStopIndex)))
    }
  }
}

object DataTypeParser extends AbstractParser {
  override protected def astBuilder: DataTypeAstBuilder = new DataTypeAstBuilder
}
