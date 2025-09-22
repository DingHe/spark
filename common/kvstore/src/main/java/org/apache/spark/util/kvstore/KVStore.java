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

package org.apache.spark.util.kvstore;

import java.io.Closeable;
import java.util.Collection;

import org.apache.spark.annotation.Private;

/**
 * Abstraction for a local key/value store for storing app data.
 *
 * <p>
 * There are two main features provided by the implementations of this interface:
 * </p>
 *
 * <h3>Serialization</h3>
 *
 * <p>
 * If the underlying data store requires serialization, data will be serialized to and deserialized
 * using a {@link KVStoreSerializer}, which can be customized by the application. The serializer is
 * based on Jackson, so it supports all the Jackson annotations for controlling the serialization of
 * app-defined types.
 * </p>
 *
 * <p>
 * Data is also automatically compressed to save disk space.
 * </p>
 *
 * <h3>Automatic Key Management</h3>
 *
 * <p>
 * When using the built-in key management, the implementation will automatically create unique
 * keys for each type written to the store. Keys are based on the type name, and always start
 * with the "+" prefix character (so that it's easy to use both manual and automatic key
 * management APIs without conflicts).
 * </p>
 *
 * <p>
 * Another feature of automatic key management is indexing; by annotating fields or methods of
 * objects written to the store with {@link KVIndex}, indices are created to sort the data
 * by the values of those properties. This makes it possible to provide sorting without having
 * to load all instances of those types from the store.
 * </p>
 *
 * <p>
 * KVStore instances are thread-safe for both reads and writes.
 * </p>
 */
// 用于本地键/值存储的抽象接口
// 主要目的是为 Spark 提供一个高效且可靠的方式来持久化应用程序数据
//Spark 内部被广泛用于历史服务器（History Server）和事件日志（Event Log）功能，将事件日志中的事件数据（如作业、阶段、任务信息等）高效地存储在本地文件系统中，以便后续查询和分析
@Private
public interface KVStore extends Closeable {

  /**
   * Returns app-specific metadata from the store, or null if it's not currently set.
   *
   * <p>
   * The metadata type is application-specific. This is a convenience method so that applications
   * don't need to define their own keys for this information.
   * </p>
   */
  //从存储中读取应用程序特定的元数据
  <T> T getMetadata(Class<T> klass) throws Exception;

  /**
   * Writes the given value in the store metadata key.
   */
  //将一个对象写入存储的元数据键中。这是设置应用程序元数据的便捷方法，避免了应用自己管理元数据键
  void setMetadata(Object value) throws Exception;

  /**
   * Read a specific instance of an object.
   *
   * @param naturalKey The object's "natural key", which uniquely identifies it. Null keys
   *                   are not allowed.
   * @throws java.util.NoSuchElementException If an element with the given key does not exist.
   */

  // 根据对象的自然键（naturalKey）读取一个特定的对象实例
  <T> T read(Class<T> klass, Object naturalKey) throws Exception;

  /**
   * Writes the given object to the store, including indexed fields. Indices are updated based
   * on the annotated fields of the object's class.
   *
   * <p>
   * Writes may be slower when the object already exists in the store, since it will involve
   * updating existing indices.
   * </p>
   *
   * @param value The object to write.
   */
  // 将给定的对象写入存储，并自动处理其索引。如果对象已存在，会更新现有索引。
  void write(Object value) throws Exception;

  /**
   * Removes an object and all data related to it, like index entries, from the store.
   *
   * @param type The object's type.
   * @param naturalKey The object's "natural key", which uniquely identifies it. Null keys
   *                   are not allowed.
   * @throws java.util.NoSuchElementException If an element with the given key does not exist.
   */
  // 从存储中移除一个对象及其所有相关数据（包括索引）
  void delete(Class<?> type, Object naturalKey) throws Exception;

  /**
   * Returns a configurable view for iterating over entities of the given type.
   */
  //返回一个可配置的视图对象（KVStoreView），用于迭代给定类型的所有实体
  <T> KVStoreView<T> view(Class<T> type) throws Exception;

  /**
   * Returns the number of items of the given type currently in the store.
   */
  //返回存储中给定类型的所有项目的数量
  long count(Class<?> type) throws Exception;

  /**
   * Returns the number of items of the given type which match the given indexed value.
   */
  // 返回存储中，其特定索引（index）的值（indexedValue）匹配给定值的项目数量。
  // 这使得可以快速查询满足特定索引条件的对象数量
  long count(Class<?> type, String index, Object indexedValue) throws Exception;

  /**
   * A cheaper way to remove multiple items from the KVStore
   */
  <T> boolean removeAllByIndexValues(Class<T> klass, String index, Collection<?> indexValues)
      throws Exception;
}
