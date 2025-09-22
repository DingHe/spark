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

package org.apache.spark.sql.types

import org.apache.spark.sql.errors.DataTypeErrors
//用于表示 JVM 对象的数据类型，常用于 Spark SQL 表达式求值时传递自定义的 JVM 对象
object ObjectType extends AbstractDataType {
  //ObjectType 是一个特殊的数据类型，不能直接进行默认的类型转换
  override private[sql] def defaultConcreteType: DataType =
    throw DataTypeErrors.nullLiteralsCannotBeCastedError(ObjectType.simpleString)

  override private[sql] def acceptsType(other: DataType): Boolean = other match {
    case ObjectType(_) => true
    case _ => false
  }

  override private[sql] def simpleString: String = "Object"
}

/**
 * Represents a JVM object that is passing through Spark SQL expression evaluation.
 */
//cls 是一个类类型的成员变量，它表示 ObjectType 对应的 Java 类类型
case class ObjectType(cls: Class[_]) extends DataType {
  override def defaultSize: Int = 4096

  def asNullable: DataType = this

  override def simpleString: String = cls.getName

  override def acceptsType(other: DataType): Boolean = other match {
    //则会检查 cls 是否可以被 otherCls 所赋值
    case ObjectType(otherCls) => cls.isAssignableFrom(otherCls)
    case _ => false
  }
}
