/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.rpc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;

import io.netty.buffer.ByteBuf;

public class InboundRpcMessage extends RpcMessage{
  public byte[] pBody;
  public ByteBuf dBody;

  public InboundRpcMessage(RpcMode mode, int rpcType, int coordinationId, byte[] pBody, ByteBuf dBody) {
    super(mode, rpcType, coordinationId);
    this.pBody = pBody;
    this.dBody = dBody;
  }

  @Override
  public int getBodySize() {
    int len = pBody.length;
    if (dBody != null) {
      len += dBody.capacity();
    }
    return len;
  }

  @Override
  void release() {
    if (dBody != null) {
      dBody.release();
    }
  }

  @SuppressWarnings("ArrayToString") // do not need pBody content to be printed out...
  @Override
  public String toString() {
    return "InboundRpcMessage [pBody=" + pBody + ", mode=" + mode + ", rpcType=" + rpcType + ", coordinationId="
        + coordinationId + ", dBody=" + dBody + "]";
  }

  public InputStream getProtobufBodyAsIS() {
    return new ByteArrayInputStream(pBody);
  }

}
