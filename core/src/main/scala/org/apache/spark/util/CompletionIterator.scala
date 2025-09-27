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

package org.apache.spark.util

/**
 * Wrapper around an iterator which calls a completion method after it successfully iterates
 * through all the elements.
 */
// 装饰器（Wrapper）模式的迭代器，主要用于确保在底层迭代器耗尽（即成功遍历完所有元素）时，能够执行一个清理或完成操作
// 核心作用是在正常迭代结束时触发一个回调函数（completion() 方法），从而执行必要的收尾工作
// +A (元素类型)
// +I (底层迭代器类型，必须是 Iterator[A] 的子类型)
private[spark]
abstract class CompletionIterator[ +A, +I <: Iterator[A]](sub: I) extends Iterator[A] {
  //用于记录 completion() 方法是否已被调用。它确保完成逻辑只执行一次
  private[this] var completed = false
  //存储实际数据来源的底层迭代器
  private[this] var iter = sub
  //直接将调用转发给底层的 iter.next() 方法。它负责实际的数据读取
  def next(): A = iter.next()
  def hasNext: Boolean = {
    //调用底层迭代器的 hasNext 方法，将结果存储在 r 中
    val r = iter.hasNext
    //只有当底层迭代器返回 false (!r)，并且完成回调尚未执行 (!completed) 时，才进入完成逻辑
    if (!r && !completed) {
      completed = true
      // reassign to release resources of highly resource consuming iterators early
      iter = Iterator.empty.asInstanceOf[I]
      completion()
    }
    r
  }

  def completion(): Unit
}

private[spark] object CompletionIterator {
  def apply[A, I <: Iterator[A]](sub: I, completionFunction: => Unit) : CompletionIterator[A, I] = {
    new CompletionIterator[A, I](sub) {
      def completion(): Unit = completionFunction
    }
  }
}
