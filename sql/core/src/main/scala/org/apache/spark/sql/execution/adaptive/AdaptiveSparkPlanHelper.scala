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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.execution.SparkPlan

/**
 * This class provides utility methods related to tree traversal of an [[AdaptiveSparkPlanExec]]
 * plan. Unlike their counterparts in [[org.apache.spark.sql.catalyst.trees.TreeNode]] or
 * [[org.apache.spark.sql.catalyst.plans.QueryPlan]], these methods traverse down leaf nodes of
 * adaptive plans, i.e., [[AdaptiveSparkPlanExec]] and [[QueryStageExec]].
 */
//主要用于处理 Spark 执行计划的树形结构，特别是在自适应查询执行（AQE）中，帮助操作和查找执行计划节点
trait AdaptiveSparkPlanHelper {

  /**
   * Find the first [[SparkPlan]] that satisfies the condition specified by `f`.
   * The condition is recursively applied to this node and all of its children (pre-order).
   */
    //该方法递归遍历执行计划树，查找第一个满足条件 f 的 SparkPlan 节点，并返回一个 Option[SparkPlan]
  def find(p: SparkPlan)(f: SparkPlan => Boolean): Option[SparkPlan] = if (f(p)) {
    Some(p)
  } else {
    //Scala 集合库中的一个高阶函数，用于通过一个二元操作函数（函数接受两个参数）对集合中的元素进行累积计算。
    // 它通常用于从左到右地遍历集合，并对每个元素执行某种操作
    //foldRight 是从右到左遍历集合
    allChildren(p).foldLeft(Option.empty[SparkPlan]) { (l, r) => l.orElse(find(r)(f)) }
  }

  /**
   * Runs the given function on this node and then recursively on children.
   * @param f the function to be applied to each node in the tree.
   */
    //该方法对执行计划树中的每个节点应用函数 f，先对当前节点应用，再递归应用到所有子节点
  def foreach(p: SparkPlan)(f: SparkPlan => Unit): Unit = {
    f(p)
    allChildren(p).foreach(foreach(_)(f))
  }

  /**
   * Runs the given function recursively on children then on this node.
   * @param f the function to be applied to each node in the tree.
   */
    //与 foreach 相似，但函数 f 的应用顺序不同。该方法先递归对子节点应用 f，然后再应用到当前节点
  def foreachUp(p: SparkPlan)(f: SparkPlan => Unit): Unit = {
    allChildren(p).foreach(foreachUp(_)(f))
    f(p)
  }

  /**
   * Returns a Seq containing the result of applying the given function to each
   * node in this tree in a preorder traversal.
   * @param f the function to be applied.
   */
  def mapPlans[A](p: SparkPlan)(f: SparkPlan => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(p)(ret += f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
   * resulting collections.
   */
    //该方法对执行计划树中的每个节点应用函数 f，并返回一个包含应用结果的序列（pre-order 遍历）
  def flatMap[A](p: SparkPlan)(f: SparkPlan => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(p)(ret ++= f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq containing the result of applying a partial function to all elements in this
   * tree on which the function is defined.
   */
    //PartialFunction 是一种特殊类型的函数，它只对部分输入值定义有效，并且可以在输入不符合要求时抛出异常
    //部分定义：与普通的函数不同，PartialFunction 只对某些输入值定义有效，而对其他输入值未定义
    //isDefinedAt 方法：PartialFunction 提供了 isDefinedAt 方法来检查某个值是否在其定义域内
    //apply 方法：如果输入值在定义域内，则可以调用 apply 方法计算结果，否则抛出异常。
    //lift 方法将一个普通的函数提升为一个 PartialFunction，返回一个 Option 类型的结果。如果函数的输入值不在定义域内，它会返回 None
    //该方法对执行计划树中的每个节点应用部分函数 pf，并返回一个包含符合条件的节点的序列
  def collect[B](p: SparkPlan)(pf: PartialFunction[SparkPlan, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(p)(node => lifted(node).foreach(ret.+=))
    ret.toSeq
  }

  /**
   * Returns a Seq containing the leaves in this tree.
   */
    //该方法返回执行计划树中所有叶子节点的序列。叶子节点是没有子节点的节点。
  def collectLeaves(p: SparkPlan): Seq[SparkPlan] = {
    collect(p) { case plan if allChildren(plan).isEmpty => plan }
  }

  /**
   * Finds and returns the first [[SparkPlan]] of the tree for which the given partial function
   * is defined (pre-order), and applies the partial function to it.
   */
    //该方法递归地在树中查找第一个满足部分函数 pf 的节点，并应用该函数
  def collectFirst[B](p: SparkPlan)(pf: PartialFunction[SparkPlan, B]): Option[B] = {
    val lifted = pf.lift
    lifted(p).orElse {
      allChildren(p).foldLeft(Option.empty[B]) { (l, r) => l.orElse(collectFirst(r)(pf)) }
    }
  }

  /**
   * Returns a sequence containing the result of applying a partial function to all elements in this
   * plan, also considering all the plans in its (nested) subqueries
   */
    //该方法返回一个序列，包含执行计划树中所有节点及其子查询的结果，并应用部分函数 f
  def collectWithSubqueries[B](p: SparkPlan)(f: PartialFunction[SparkPlan, B]): Seq[B] = {
    (p +: subqueriesAll(p)).flatMap(collect(_)(f))
  }

  /**
   * Returns a sequence containing the subqueries in this plan, also including the (nested)
   * subqueries in its children
   */
    //该方法返回一个包含当前执行计划及其所有子查询节点的序列
  def subqueriesAll(p: SparkPlan): Seq[SparkPlan] = {
    val subqueries = flatMap(p)(_.subqueries)
    subqueries ++ subqueries.flatMap(subqueriesAll)
  }
  //根据不同的节点类型，返回该节点的子节点。
  // AdaptiveSparkPlanExec 类型的节点返回 executedPlan，QueryStageExec 类型的节点返回 plan，其他类型的节点返回 children
  protected def allChildren(p: SparkPlan): Seq[SparkPlan] = p match {
    case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
    case s: QueryStageExec => Seq(s.plan)
    case _ => p.children
  }

  /**
   * Strip the executePlan of AdaptiveSparkPlanExec leaf node.
   */
    //该方法用于去除 AdaptiveSparkPlanExec 节点的执行计划（即 executedPlan），返回去除后的节点。
  // 如果节点不是 AdaptiveSparkPlanExec 类型，则返回原节点
  def stripAQEPlan(p: SparkPlan): SparkPlan = p match {
    case a: AdaptiveSparkPlanExec => a.executedPlan
    case other => other
  }
}
