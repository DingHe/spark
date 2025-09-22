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

package org.apache.spark.serializer

import java.io._
import java.nio.ByteBuffer
import javax.annotation.concurrent.NotThreadSafe

import scala.reflect.ClassTag

import org.apache.spark.annotation.{DeveloperApi, Private}
import org.apache.spark.util.NextIterator

/**
 * :: DeveloperApi ::
 * A serializer. Because some serialization libraries are not thread safe, this class is used to
 * create [[org.apache.spark.serializer.SerializerInstance]] objects that do the actual
 * serialization and are guaranteed to only be called from one thread at a time.
 *
 * Implementations of this trait should implement:
 *
 * 1. a zero-arg constructor or a constructor that accepts a [[org.apache.spark.SparkConf]]
 * as parameter. If both constructors are defined, the latter takes precedence.
 *
 * 2. Java serialization interface.
 *
 * @note Serializers are not required to be wire-compatible across different versions of Spark.
 * They are intended to be used to serialize/de-serialize data within a single Spark application.
 */
@DeveloperApi
abstract class Serializer {

  /**
   * Default ClassLoader to use in deserialization. Implementations of [[Serializer]] should
   * make sure it is using this when set.
   */
  @volatile protected var defaultClassLoader: Option[ClassLoader] = None  //用于存储反序列化时使用的默认类加载器

  /**
   * Sets a class loader for the serializer to use in deserialization.
   *
   * @return this Serializer object
   */
  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  /** Creates a new [[SerializerInstance]]. 实际执行序列化和反序列化操作的对象*/
  def newInstance(): SerializerInstance

  /**
   * :: Private ::
   * Returns true if this serializer supports relocation of its serialized objects and false
   * otherwise. This should return true if and only if reordering the bytes of serialized objects
   * in serialization stream output is equivalent to having re-ordered those elements prior to
   * serializing them. More specifically, the following should hold if a serializer supports
   * relocation:
   *
   * {{{
   * serOut.open()
   * position = 0
   * serOut.write(obj1)
   * serOut.flush()
   * position = # of bytes written to stream so far
   * obj1Bytes = output[0:position-1]
   * serOut.write(obj2)
   * serOut.flush()
   * position2 = # of bytes written to stream so far
   * obj2Bytes = output[position:position2-1]
   * serIn.open([obj2bytes] concatenate [obj1bytes]) should return (obj2, obj1)
   * }}}
   *
   * In general, this property should hold for serializers that are stateless and that do not
   * write special metadata at the beginning or end of the serialization stream.
   *
   * This API is private to Spark; this method should not be overridden in third-party subclasses
   * or called in user code and is subject to removal in future Spark releases.
   *
   * See SPARK-7311 for more details.
   */
    //重排序 = 序列化后的二进制数据仍然保留了逻辑顺序，可以直接比较和排序
  @Private
  private[spark] def supportsRelocationOfSerializedObjects: Boolean = false  //支持重定位的序列化器允许重新排序序列化流中的对象并且能够正确地反序列化
}


/**
 * :: DeveloperApi ::
 * An instance of a serializer, for use by one thread at a time.
 *
 * It is legal to create multiple serialization / deserialization streams from the same
 * SerializerInstance as long as those streams are all used within the same thread.
 */
@DeveloperApi
@NotThreadSafe
abstract class SerializerInstance { //序列化器实例，每个实例可以用于一个线程中的序列化和反序列化操作
  def serialize[T: ClassTag](t: T): ByteBuffer  //将对象 t 序列化成一个 ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T  //从 ByteBuffer 中反序列化出一个对象，返回类型 T

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T

  def serializeStream(s: OutputStream): SerializationStream  //用于将对象序列化到输出流中

  def deserializeStream(s: InputStream): DeserializationStream //用于从输入流中反序列化对象
}

/**
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
 */
@DeveloperApi
abstract class SerializationStream extends Closeable {
  /** The most general-purpose method to write an object. */
  def writeObject[T: ClassTag](t: T): SerializationStream
  /** Writes the object representing the key of a key-value pair. */
  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  /** Writes the object representing the value of a key-value pair. */
  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)
  def flush(): Unit
  override def close(): Unit

  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}


/**
 * :: DeveloperApi ::
 * A stream for reading serialized objects.
 */
@DeveloperApi
abstract class DeserializationStream extends Closeable {
  /** The most general-purpose method to read an object. */
  def readObject[T: ClassTag](): T
  /** Reads the object representing the key of a key-value pair. */
  def readKey[T: ClassTag](): T = readObject[T]()
  /** Reads the object representing the value of a key-value pair. */
  def readValue[T: ClassTag](): T = readObject[T]()
  override def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
   */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close(): Unit = {
      DeserializationStream.this.close()
    }
  }

  /**
   * Read the elements of this stream through an iterator over key-value pairs. This can only be
   * called once, as reading each element will consume data from the input source.
   */
  def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
    override protected def getNext() = {
      try {
        (readKey[Any](), readValue[Any]())
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close(): Unit = {
      DeserializationStream.this.close()
    }
  }
}
