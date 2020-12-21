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
package com.dremio.sabot.rpc.user;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.rpc.InboundRpcMessage;
import com.dremio.sabot.rpc.user.BaseBackwardsCompatibilityHandler.QueryBatch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

/**
 * Decoder step for adding backward compatibility conversion step
 */
class BackwardsCompatibilityDecoder extends MessageToMessageDecoder<InboundRpcMessage> {
  private final BufferAllocator allocator;
  private final BaseBackwardsCompatibilityHandler handler;

  BackwardsCompatibilityDecoder(final BufferAllocator allocator,BaseBackwardsCompatibilityHandler handler) {
    super(InboundRpcMessage.class);
    this.allocator = allocator;
    this.handler = handler;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, InboundRpcMessage msg, List<Object> out) throws Exception {
    if (msg.mode == RpcMode.REQUEST &&
      msg.rpcType == RpcType.QUERY_DATA.getNumber()) {

      final QueryData oldHeader = QueryData.parseFrom(msg.pBody);
      final ByteBuf oldData = msg.dBody;

      final QueryBatch oldBatch = new QueryBatch(oldHeader, oldData);
      final QueryBatch newBatch = handler.makeBatchCompatible(oldBatch);

      // A copy might be needed to consolidate all buffers as client expect
      // one contiguous Arrow buffer.
      final ByteBuf newBuffer = consolidateBuffers(newBatch.getBuffers());

      out.add(new InboundRpcMessage(msg.mode, msg.rpcType, msg.coordinationId,
        newBatch.getHeader().toByteArray(), newBuffer));
    } else {
      out.add(msg);
    }
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    super.handlerRemoved(ctx);
    handler.close();
  }

  private ByteBuf consolidateBuffers(ByteBuf... buffers) {
    if (buffers.length == 0) {
      return NettyArrowBuf.unwrapBuffer(allocator.buffer(0));
    }

    if (buffers.length == 1){
      return buffers[0];
    }

    int readableBytes = 0;
    for(ByteBuf buffer: buffers) {
      readableBytes += buffer.readableBytes();
    }

    try {
      ByteBuf newBuffer = NettyArrowBuf.unwrapBuffer(allocator.buffer(readableBytes));
      for(ByteBuf buffer: buffers) {
        newBuffer.writeBytes(buffer);
      }
      return newBuffer;
    } finally {
      freeBuffers(buffers);
    }
  }

  private static void freeBuffers(ByteBuf... buffers) {
    for(ByteBuf buffer: buffers) {
      buffer.release();
    }
  }
}
