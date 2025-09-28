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

package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Request to find the meta information for the specified merged block. The meta information
 * contains the number of chunks in the merged blocks and the maps ids in each chunk.
 *
 * @since 3.2.0
 */
// 用于实现 Push-Based Shuffle（推送式 Shuffle） 的新功能
// 请求元数据：它是一个客户端请求消息（实现了 RequestMessage 接口），用于向远程的 Shuffle Server（通常是 External Shuffle Service）请求已合并 Shuffle 数据块的元信息
// 支持分块拉取：在 Push-Based Shuffle 中，多个 Map 任务的输出会被合并成一个大的合并块（Merged Block）。
// 为了高效地传输，这个合并块又会被分割成多个逻辑块（Chunk）。Reduce 任务需要先获取这些元数据，才能知道合并块被分成了多少个 Chunk，以及每个 Chunk 包含了哪些 Map 任务的输出
public class MergedBlockMetaRequest extends AbstractMessage implements RequestMessage {
  //唯一的请求标识符。用于客户端在收到响应时，将响应与特定的请求进行匹配
  public final long requestId;
  //应用程序的唯一标识符（application ID）。用于在 Shuffle Server 上定位该应用的数据，并进行认证
  public final String appId;
  //当前 Shuffle 操作的 ID
  public final int shuffleId;
  //标识特定的 Shuffle 合并操作。Push-Based Shuffle 中特有的标识符，用于区分不同的合并活动
  public final int shuffleMergeId;
  //目标 Reduce 任务的 ID。在 Push-Based Shuffle 中，一个合并块是为特定的 Reduce 任务创建的，这个 ID 用于定位该 Reduce 任务所需的合并块。
  public final int reduceId;

  public MergedBlockMetaRequest(
      long requestId,
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId) {
    super(null, false);
    this.requestId = requestId;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.shuffleMergeId = shuffleMergeId;
    this.reduceId = reduceId;
  }

  @Override
  public Type type() {
    return Type.MergedBlockMetaRequest;
  }

  @Override
  public int encodedLength() {
    return 8 + Encoders.Strings.encodedLength(appId) + 4 + 4 + 4;
  }
  //编码消息
  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    Encoders.Strings.encode(buf, appId);
    buf.writeInt(shuffleId);
    buf.writeInt(shuffleMergeId);
    buf.writeInt(reduceId);
  }
  //解码消息
  public static MergedBlockMetaRequest decode(ByteBuf buf) {
    long requestId = buf.readLong();
    String appId = Encoders.Strings.decode(buf);
    int shuffleId = buf.readInt();
    int shuffleMergeId = buf.readInt();
    int reduceId = buf.readInt();
    return new MergedBlockMetaRequest(requestId, appId, shuffleId, shuffleMergeId, reduceId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, appId, shuffleId, shuffleMergeId, reduceId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof MergedBlockMetaRequest) {
      MergedBlockMetaRequest o = (MergedBlockMetaRequest) other;
      return requestId == o.requestId && shuffleId == o.shuffleId &&
        shuffleMergeId == o.shuffleMergeId && reduceId == o.reduceId &&
        Objects.equal(appId, o.appId);
    }
    return false;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
      .append("requestId", requestId)
      .append("appId", appId)
      .append("shuffleId", shuffleId)
      .append("shuffleMergeId", shuffleMergeId)
      .append("reduceId", reduceId)
      .toString();
  }
}
