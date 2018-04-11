/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.rpc.user;

import java.util.List;

import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.rpc.OutboundRpcMessage;
import com.dremio.sabot.rpc.user.BaseBackwardsCompatibilityHandler.QueryBatch;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * Encoder step for adding backward compatibility conversion step
 */
class BackwardsCompatibilityEncoder extends MessageToMessageEncoder<OutboundRpcMessage> {
  private final BaseBackwardsCompatibilityHandler handler;


  BackwardsCompatibilityEncoder(BaseBackwardsCompatibilityHandler handler) {
    super(OutboundRpcMessage.class);
    this.handler = handler;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, OutboundRpcMessage msg, List<Object> out) throws Exception {
    if (msg.mode == RpcMode.REQUEST &&
      msg.rpcType == RpcType.QUERY_DATA.getNumber()) {

      final QueryData oldHeader = (QueryData) msg.getPBody();
      final ByteBuf[] oldData = msg.getDBodies();

      final QueryBatch oldBatch = new QueryBatch(oldHeader, oldData);
      final QueryBatch newBatch = handler.makeBatchCompatible(oldBatch);

      out.add(new OutboundRpcMessage(msg.mode, RpcType.QUERY_DATA, msg.coordinationId,
        newBatch.getHeader(), newBatch.getBuffers()));
    } else {
      out.add(msg);
    }
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    super.handlerRemoved(ctx);
    handler.close();
  }
}
