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

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.buffer.ManagedBuffer;

/** An on-the-wire transmittable message. 要求实现该接口的类能够将消息编码为字节流，并定义消息的类型、主体以及是否将消息主体包含在消息的同一帧中*/
public interface Message extends Encodable {
  /** Used to identify this request type. */
  Type type(); //消息类型由Type枚举定义，表示消息的不同类别（如请求、响应、失败等）

  /** An optional body for the message. */
  ManagedBuffer body(); //返回消息的主体部分
  //指示消息主体是否应该与消息类型一起包含在同一帧中发送。如果返回 true，则消息主体将与消息头（消息类型）一起发送，否则消息主体会单独发送或以其他方式处理
  /** Whether to include the body of the message in the same frame as the message. */
  boolean isBodyInFrame();

  /** Preceding every serialized Message is its type, which allows us to deserialize it. */
  enum Type implements Encodable {
    ChunkFetchRequest(0), ChunkFetchSuccess(1), ChunkFetchFailure(2),
    RpcRequest(3), RpcResponse(4), RpcFailure(5),
    StreamRequest(6), StreamResponse(7), StreamFailure(8),
    OneWayMessage(9), UploadStream(10), MergedBlockMetaRequest(11), MergedBlockMetaSuccess(12),
    User(-1);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() { return id; }

    @Override public int encodedLength() { return 1; }

    @Override public void encode(ByteBuf buf) { buf.writeByte(id); }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      switch (id) {
        case 0: return ChunkFetchRequest;
        case 1: return ChunkFetchSuccess;
        case 2: return ChunkFetchFailure;
        case 3: return RpcRequest;
        case 4: return RpcResponse;
        case 5: return RpcFailure;
        case 6: return StreamRequest;
        case 7: return StreamResponse;
        case 8: return StreamFailure;
        case 9: return OneWayMessage;
        case 10: return UploadStream;
        case 11: return MergedBlockMetaRequest;
        case 12: return MergedBlockMetaSuccess;
        case -1: throw new IllegalArgumentException("User type messages cannot be decoded.");
        default: throw new IllegalArgumentException("Unknown message type: " + id);
      }
    }
  }
}
