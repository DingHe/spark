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

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.errors.DataTypeErrors

/**
 * A non-concrete data type, reserved for internal uses.
 */
//抽象类，用于表示一个抽象的数据类型，它并不是具体的某种数据类型，而是内部使用的一个基类
private[sql] abstract class AbstractDataType {
  /**
   * The default concrete type to use if we want to cast a null literal into this type.
   */
  //返回该抽象数据类型对应的默认具体类型。如果传入的是 null 字面量并需要转换成这个类型时，返回的就是这个默认的具体类型
  private[sql] def defaultConcreteType: DataType

  /**
   * Returns true if `other` is an acceptable input type for a function that expects this,
   * possibly abstract DataType.
   *
   * {{{
   *   // this should return true
   *   DecimalType.acceptsType(DecimalType(10, 2))
   *
   *   // this should return true as well
   *   NumericType.acceptsType(DecimalType(10, 2))
   * }}}
   */
  //用于检查 other 类型是否是一个可以接受的输入类型。比如，在进行类型转换时，某个操作可能要求输入的类型符合某个抽象数据类型
  private[sql] def acceptsType(other: DataType): Boolean

  /** Readable string representation for the type. */
  //该数据类型的简单字符串表示，通常是该数据类型的名字
  private[sql] def simpleString: String
}


/**
 * A collection of types that can be used to specify type constraints. The sequence also specifies
 * precedence: an earlier type takes precedence over a latter type.
 *
 * {{{
 *   TypeCollection(StringType, BinaryType)
 * }}}
 *
 * This means that we prefer StringType over BinaryType if it is possible to cast to StringType.
 */
//包含多个 AbstractDataType 的集合类型，用于表示可以互相转换的类型集合
private[sql] class TypeCollection(private val types: Seq[AbstractDataType])
  extends AbstractDataType {
  //types 存储 AbstractDataType 类型的序列。通过这个序列，可以表示一个类型集合，这些类型可以互相转换

  require(types.nonEmpty, s"TypeCollection ($types) cannot be empty")
  //返回集合中的第一个类型的默认具体类型
  override private[sql] def defaultConcreteType: DataType = types.head.defaultConcreteType
  //判断 other 类型是否能被集合中的任何一个类型接受
  override private[sql] def acceptsType(other: DataType): Boolean =
    types.exists(_.acceptsType(other))

  override private[sql] def simpleString: String = {
    types.map(_.simpleString).mkString("(", " or ", ")")
  }
}


private[sql] object TypeCollection {

  /**
   * Types that include numeric types and ANSI interval types.
   */
    //包含了数值类型和 ANSI 标准的间隔类型
  val NumericAndAnsiInterval = TypeCollection(
    NumericType,
    DayTimeIntervalType,
    YearMonthIntervalType)

  /**
   * Types that include numeric and ANSI interval types, and additionally the legacy interval type.
   * They are only used in unary_minus, unary_positive, add and subtract operations.
   */
    //包含了数值类型和 ANSI 标准的间隔类型，并且还包括了遗留的间隔类型
  val NumericAndInterval = new TypeCollection(NumericAndAnsiInterval.types :+ CalendarIntervalType)

  def apply(types: AbstractDataType*): TypeCollection = new TypeCollection(types)

  def unapply(typ: AbstractDataType): Option[Seq[AbstractDataType]] = typ match {
    case typ: TypeCollection => Some(typ.types)
    case _ => None
  }
}


/**
 * An `AbstractDataType` that matches any concrete data types.
 */
//表示任意的数据类型
protected[sql] object AnyDataType extends AbstractDataType with Serializable {

  // Note that since AnyDataType matches any concrete types, defaultConcreteType should never
  // be invoked.
  override private[sql] def defaultConcreteType: DataType =
    throw DataTypeErrors.unsupportedOperationExceptionError()

  override private[sql] def simpleString: String = "any"

  override private[sql] def acceptsType(other: DataType): Boolean = true
}


/**
 * An internal type used to represent everything that is not null, UDTs, arrays, structs, and maps.
 */
//AtomicType 是一个抽象类，用于表示基础类型（例如数值、日期时间等）
protected[sql] abstract class AtomicType extends DataType

object AtomicType


/**
 * Numeric data types.
 *
 * @since 1.3.0
 */
@Stable
abstract class NumericType extends AtomicType


private[spark] object NumericType extends AbstractDataType {
  override private[spark] def defaultConcreteType: DataType = DoubleType

  override private[spark] def simpleString: String = "numeric"

  override private[spark] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[NumericType]   //接受任意的子类
}

//IntegralType 是 NumericType 的一个子类，表示整数类型
private[sql] object IntegralType extends AbstractDataType {
  override private[sql] def defaultConcreteType: DataType = IntegerType

  override private[sql] def simpleString: String = "integral"

  override private[sql] def acceptsType(other: DataType): Boolean = other.isInstanceOf[IntegralType]
}


private[sql] abstract class IntegralType extends NumericType

//FractionalType 是 NumericType 的另一个子类，表示带小数的数值类型（如 Float 或 Double）
private[sql] object FractionalType


private[sql] abstract class FractionalType extends NumericType

//表示所有时间戳类型（包括带时区和不带时区的时间戳）
private[sql] object AnyTimestampType extends AbstractDataType with Serializable {
  override private[sql] def defaultConcreteType: DataType = TimestampType

  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[TimestampType] || other.isInstanceOf[TimestampNTZType]

  override private[sql] def simpleString = "(timestamp or timestamp without time zone)"
}

private[sql] abstract class DatetimeType extends AtomicType

/**
 * The interval type which conforms to the ANSI SQL standard.
 */
 //表示符合 ANSI SQL 标准的时间间隔类型
private[sql] abstract class AnsiIntervalType extends AtomicType

private[spark] object AnsiIntervalType extends AbstractDataType {
  override private[sql] def simpleString: String = "ANSI interval"

  override private[sql] def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[AnsiIntervalType]

  override private[sql] def defaultConcreteType: DataType = DayTimeIntervalType()
}
