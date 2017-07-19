/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.services.fabric;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcBus;
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.proto.FabricProto.FabricIdentity;
import com.dremio.services.fabric.proto.FabricProto.FabricMessage;
import com.dremio.services.fabric.proto.FabricProto.RpcType;
import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;

/**
 * Handles messages associated with RPC fabric.
 */
class FabricMessageHandler {

  private final FabricProtocol[] protocols = new FabricProtocol[64];
  private AtomicLong sync = new AtomicLong();

  protected void handle(FabricIdentity remoteIdentity, FabricIdentity localIdentity, FabricConnection connection, int rpcType, byte[] pBody, ByteBuf dBody, ResponseSender responseSender) throws RpcException{
    final FabricMessage message = RpcBus.get(pBody, FabricMessage.PARSER);
    final int protocolId = message.getProtocolId();
    FabricProtocol protocol = getProtocol(protocolId);


        if(dBody != null){
          final ArrowBuf buf = (ArrowBuf) dBody;
          BufferAllocator allocator = protocol.getAllocator();
          if(allocator.getHeadroom() < buf.getPossibleMemoryConsumed()){
            throw new RpcException(String.format("Message of length %d arrived at node %s:%d, send from %s:%d. Unfortunately, local memory for protocol %d is %d which is too close to the limit of %d. Message rejected.",
                buf.getPossibleMemoryConsumed(),
                remoteIdentity.getAddress(),
                remoteIdentity.getPort(),
                localIdentity.getAddress(),
                localIdentity.getPort(),
                protocolId,
                allocator.getAllocatedMemory(),
                allocator.getLimit()
                ));
          }

          // Transfer data to protocol allocator. Disabled until we get shutdown ordering correct.
          // buf.transferOwnership(allocator);
        }
    protocol.handle(connection, message.getInnerRpcType(), message.getMessage(), dBody, new ChainedResponseSender(responseSender, protocolId));
  }

  private static class ChainedResponseSender implements ResponseSender {
    private ResponseSender innerSender;
    private int protocolId;

    public ChainedResponseSender(ResponseSender innerSender, int protocolId) {
      super();
      this.innerSender = innerSender;
      this.protocolId = protocolId;
    }

    @Override
    public void send(Response r) {
      FabricMessage message = FabricMessage.newBuilder()
        .setProtocolId(protocolId)
        .setInnerRpcType(r.rpcType.getNumber())
        .setMessage(r.pBody.toByteString())
        .build();

      innerSender.send(new Response(RpcType.MESSAGE, message, r.dBodies));
    }

  }

  void registerProtocol(FabricProtocol protocol){
    Preconditions.checkArgument(protocols[protocol.getProtocolId()] == null, "Protocols already registered at logical id " + protocol.getProtocolId());
    protocols[protocol.getProtocolId()] = protocol;
    sync.incrementAndGet();
  }

  public FabricProtocol getProtocol(int protocolId){
    FabricProtocol protocol = protocols[protocolId];
    Preconditions.checkNotNull(protocol, "Unknown protocol " + protocolId);
    return protocol;
  }

}
