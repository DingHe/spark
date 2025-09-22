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

import org.apache.spark.sql.catalyst.expressions.{BindReferences, BoundReference}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}

/**
 * An interface for those join physical operators that support codegen.
 */
trait JoinCodegenSupport extends CodegenSupport with BaseJoinExec {

  /**
   * Generate the (non-equi) condition used to filter joined rows.
   * This is used in Inner, Left Semi, Left Anti and Full Outer joins.
   *
   * @return Tuple of variable name for row of build side, generated code for condition,
   *         and generated code for variables of build side.
   */
    //用于生成非等值条件（non-equi condition），这些条件用于过滤连接的行。
  // 它特别适用于内连接（Inner Join）、左半连接（Left Semi Join）、左反连接（Left Anti Join）和全外连接（Full Outer Join）
  protected def getJoinCondition(
      ctx: CodegenContext,
      streamVars: Seq[ExprCode], //流（stream）侧的变量
      streamPlan: SparkPlan, //流侧的执行计划
      buildPlan: SparkPlan,  //构建（build）侧的执行计划
      buildRow: Option[String] = None): (String, String, Seq[ExprCode]) = { //buildRow: Option[String] = None：可选参数，表示构建侧的行变量名
    val buildSideRow = buildRow.getOrElse(ctx.freshName("buildRow"))
    val buildVars = genOneSideJoinVars(ctx, buildSideRow, buildPlan, setDefaultValue = false)
    // We want to evaluate the passed streamVars. However, evaluation modifies the contained
    // ExprCode instances, which may surprise the caller to this method (in particular,
    // full outer join will want to evaluate streamVars in a different scope than the
    // condition check). Because of this, we first make a copy.
    val streamVars2 = streamVars.map(_.copy())
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables that are used by the condition
      val eval = evaluateRequiredVariables(streamPlan.output ++ buildPlan.output,
        streamVars2 ++ buildVars, expr.references)

      // filter the output via condition
      ctx.currentVars = streamVars2 ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamPlan.output ++ buildPlan.output).genCode(ctx)
      val skipRow = s"${ev.isNull} || !${ev.value}"
      s"""
         |$eval
         |${ev.code}
         |if (!($skipRow))
       """.stripMargin
    } else {
      ""
    }
    (buildSideRow, checkCondition, buildVars)
  }

  /**
   * Generates the code for variables of one child side of join.
   */
  protected def genOneSideJoinVars(
      ctx: CodegenContext,
      row: String,
      plan: SparkPlan,
      setDefaultValue: Boolean): Seq[ExprCode] = {
    ctx.currentVars = null
    ctx.INPUT_ROW = row
    plan.output.toIndexedSeq.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      if (setDefaultValue) {
        // the variables are needed even there is no matched rows
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val javaType = CodeGenerator.javaType(a.dataType)
        val code = code"""
            |boolean $isNull = true;
            |$javaType $value = ${CodeGenerator.defaultValue(a.dataType)};
            |if ($row != null) {
            |  ${ev.code}
            |  $isNull = ${ev.isNull};
            |  $value = ${ev.value};
            |}
          """.stripMargin
        ExprCode(code, JavaCode.isNullVariable(isNull), JavaCode.variable(value, a.dataType))
      } else {
        ev
      }
    }
  }
}
