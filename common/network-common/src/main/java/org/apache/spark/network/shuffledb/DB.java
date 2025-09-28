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

package org.apache.spark.network.shuffledb;

import java.io.Closeable;

import org.apache.spark.annotation.Private;

/**
 * The local KV storage used to persist the shuffle state,
 * the implementations may include LevelDB, RocksDB, etc.
 */
//主要作用是为 Spark 应用程序（特别是 External Shuffle Service 或其他需要持久化状态的组件）提供一个抽象的本地键值（Key-Value, KV）存储系统的契约
// 允许 Spark 将其服务状态（例如，External Shuffle Service 中已注册执行器的信息、应用程序的安全密钥等）持久化到本地磁盘，即使服务重启也能恢复
//接口的具体实现通常是 LevelDB 或 RocksDB，这些都是在本地磁盘上运行的、高性能的嵌入式键值数据库
@Private
public interface DB extends Closeable {
    /**
     * Set the DB entry for "key" to "value".
     */
    void put(byte[] key, byte[] value);

    /**
     * Get which returns a new byte array storing the value associated
     * with the specified input key if any.
     */
    byte[] get(byte[] key);

    /**
     * Delete the DB entry (if any) for "key".
     */
    void delete(byte[] key);

    /**
     * Return an iterator over the contents of the DB.
     */
    DBIterator iterator();
}
