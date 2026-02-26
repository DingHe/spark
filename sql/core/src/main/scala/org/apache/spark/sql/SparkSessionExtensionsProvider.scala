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

package org.apache.spark.sql

import org.apache.spark.annotation.{DeveloperApi, Since, Unstable}

// scalastyle:off line.size.limit
/**
 * :: Unstable ::
 *
 * Base trait for implementations used by [[SparkSessionExtensions]]
 *
 *
 * For example, now we have an external function named `Age` to register as an extension for SparkSession:
 *
 *
 * {{{
 *   package org.apache.spark.examples.extensions
 *
 *   import org.apache.spark.sql.catalyst.expressions.{CurrentDate, Expression, RuntimeReplaceable, SubtractDates}
 *
 *   case class Age(birthday: Expression, child: Expression) extends RuntimeReplaceable {
 *
 *     def this(birthday: Expression) = this(birthday, SubtractDates(CurrentDate(), birthday))
 *     override def exprsReplaced: Seq[Expression] = Seq(birthday)
 *     override protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
 *   }
 * }}}
 *
 * We need to create our extension which inherits [[SparkSessionExtensionsProvider]]
 * Example:
 *
 * {{{
 *   package org.apache.spark.examples.extensions
 *
 *   import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
 *   import org.apache.spark.sql.catalyst.FunctionIdentifier
 *   import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
 *
 *   class MyExtensions extends SparkSessionExtensionsProvider {
 *     override def apply(v1: SparkSessionExtensions): Unit = {
 *       v1.injectFunction(
 *         (new FunctionIdentifier("age"),
 *           new ExpressionInfo(classOf[Age].getName, "age"),
 *           (children: Seq[Expression]) => new Age(children.head)))
 *     }
 *   }
 * }}}
 *
 * Then, we can inject `MyExtensions` in three ways,
 * <ul>
 *   <li>withExtensions of [[SparkSession.Builder]]  可以在 SparkSession.Builder 中调用这个方法</li>
 *   <li>Config - spark.sql.extensions  配置项 spark.sql.extensions：通过 Spark 配置文件来加载扩展</li>
 *   <li>[[java.util.ServiceLoader]] - Add to src/main/resources/META-INF/services/org.apache.spark.sql.SparkSessionExtensionsProvider</li>
 * </ul>
 *
 * @see [[SparkSessionExtensions]]
 * @see [[SparkSession.Builder]]
 * @see [[java.util.ServiceLoader]]
 *
 * @since 3.2.0
 */
// 在 Apache Spark SQL 项目中，SparkSessionExtensionsProvider 是一个非常精简但功能强大的接口（Trait）。它是开发者接入 Spark SQL 内部机制的标准化入口。
// 这个类的核心作用是定义一个规范化的扩展加载接口。
// 解耦扩展与核心：它允许第三方插件（如 Gluten, Iceberg, Delta Lake）定义自己的扩展逻辑，而不需要修改 Spark 的源代码。
// 支持多种加载方式：通过继承这个接口，你的扩展类可以被 Spark 通过反射或 Java 的 ServiceLoader 机制自动发现并加载。
// 配置标准：它继承自 Function1[SparkSessionExtensions, Unit]。这意味着它的唯一任务就是：“接受一个扩展容器（SparkSessionExtensions），然后往里面塞入自定义规则”。
// 三种加载机制（重点）
// SparkSessionExtensionsProvider 的设计精髓在于它支持的加载方式，这决定了它如何被 Spark 识别：
// 手动注入 (withExtensions)：
// 开发者在创建 SparkSession 时，直接实例化 Provider 类并传入。
// 配置注入 (spark.sql.extensions)：
// 在 spark-defaults.conf 中设置。Spark 会读取类名，检查它是否是 SparkSessionExtensionsProvider 的子类，然后调用其 apply 方法。
//自动发现 (ServiceLoader)：
//这是最“插件化”的方式。你在 Jar 包的 META-INF/services/org.apache.spark.sql.SparkSessionExtensionsProvider 文件中写入你的类全名。Spark 启动时会自动扫描类路径下的所有该文件，并自动运行所有找到的扩展。
@DeveloperApi
@Unstable
@Since("3.2.0")
trait SparkSessionExtensionsProvider extends Function1[SparkSessionExtensions, Unit]
// scalastyle:on line.size.limit
