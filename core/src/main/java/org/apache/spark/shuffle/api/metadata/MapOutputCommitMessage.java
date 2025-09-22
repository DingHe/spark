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

package org.apache.spark.shuffle.api.metadata;

import java.util.Optional;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * Represents the result of writing map outputs for a shuffle map task.
 * <p>
 * Partition lengths represents the length of each block written in the map task. This can
 * be used for downstream readers to allocate resources, such as in-memory buffers.
 * <p>
 * Map output writers can choose to attach arbitrary metadata tags to register with a
 * shuffle output tracker (a module that is currently yet to be built in a future
 * iteration of the shuffle storage APIs).
 */
// 表示一次 shuffle map task 写出（commit）结果的元数据消息：
// 它把该 map 任务针对每个下游 partition 的输出字节长度汇总起来（以及可选的 map-output 附加元数据），
// 供下游读者/调度器在后续读取/分配资源、注册 shuffle 输出时使用
@Private
public final class MapOutputCommitMessage {
  //数组的每个元素表示该 map 任务对应的 某个下游 partition（通常是 reduce 分区） 所写出的数据长度（单位通常是字节）
  // 调度器或 shuffle 管理组件用它来判断哪些 partition 有数据、哪些是空分区（长度为 0），以及做 I/O 优化（比如预读/流控）
  private final long[] partitionLengths;
  // 表示是否存在可选的、由 map 输出编写器附加的任意元数据标签
  private final Optional<MapOutputMetadata> mapOutputMetadata;

  private MapOutputCommitMessage(
      long[] partitionLengths, Optional<MapOutputMetadata> mapOutputMetadata) {
    this.partitionLengths = partitionLengths;
    this.mapOutputMetadata = mapOutputMetadata;
  }

  public static MapOutputCommitMessage of(long[] partitionLengths) {
    return new MapOutputCommitMessage(partitionLengths, Optional.empty());
  }

  public static MapOutputCommitMessage of(
      long[] partitionLengths, MapOutputMetadata mapOutputMetadata) {
    return new MapOutputCommitMessage(partitionLengths, Optional.of(mapOutputMetadata));
  }

  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  public Optional<MapOutputMetadata> getMapOutputMetadata() {
    return mapOutputMetadata;
  }
}
