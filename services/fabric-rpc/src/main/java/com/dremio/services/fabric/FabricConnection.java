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
package com.dremio.services.fabric;

import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.rpc.RemoteConnection;
import com.dremio.exec.rpc.RpcBus;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.dremio.services.fabric.proto.FabricProto.FabricIdentity;
import com.dremio.services.fabric.proto.FabricProto.RpcType;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;

/**
 * A client > server or server > client connection.
 */
class FabricConnection extends RemoteConnection implements PhysicalConnection {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FabricConnection.class);

  private final RpcBus<RpcType, FabricConnection> bus;
  private final BufferAllocator allocator;
  private volatile FabricIdentity identity;
  private final UUID id;

  private volatile ProxyCloseHandler proxyCloseHandler;

  FabricConnection(
      String name,
      SocketChannel channel,
      RpcBus<RpcType, FabricConnection> bus,
      BufferAllocator allocator) {
    super(channel, name, false);
    this.bus = bus;
    this.id = UUID.randomUUID();
    this.allocator = allocator;
  }

  void setIdentity(FabricIdentity identity) {
    assert this.identity == null : "Identity should only be set once.";
    this.identity = identity;
  }

  public FabricIdentity getIdentity(){
    return identity;
  }

  @Override
  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(
      RpcOutcomeListener<RECEIVE> outcomeListener,
      RpcType rpcType,
      SEND protobufBody,
      Class<RECEIVE> clazz,
      ByteBuf... dataBodies) {
    bus.send(outcomeListener, this, rpcType, protobufBody, clazz, dataBodies);
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void sendUnsafe(RpcOutcomeListener<RECEIVE> outcomeListener,
      RpcType rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies) {
    bus.send(outcomeListener, this, rpcType, protobufBody, clazz, true, dataBodies);
  }

  @Override
  public boolean isActive() {
    return super.isActive();
  }

  @Override
  public void setChannelCloseHandler(ChannelFutureListener closeHandler) {
    // wrap the passed handler in a proxyCloseHandler so we can later wrap the original handler into another one
    proxyCloseHandler = new ProxyCloseHandler(closeHandler);
    super.setChannelCloseHandler(proxyCloseHandler);
  }

  public void wrapCloseHandler(FabricConnectionManager.CloseHandlerCreator closeHandlerCreator) {
    proxyCloseHandler.setHandler(closeHandlerCreator.getHandler(this, proxyCloseHandler.getHandler()));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FabricConnection other = (FabricConnection) obj;
    if (id == null) {
      if (other.id != null) {
        return false;
      }
    } else if (!id.equals(other.id)) {
      return false;
    }
    return true;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  private static final class ProxyCloseHandler implements ChannelFutureListener {

    private volatile ChannelFutureListener handler;

    private ProxyCloseHandler(ChannelFutureListener handler) {
      super();
      this.handler = handler;
    }

    public ChannelFutureListener getHandler() {
      return handler;
    }

    public void setHandler(ChannelFutureListener handler) {
      this.handler = handler;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      handler.operationComplete(future);
    }

  }

}
