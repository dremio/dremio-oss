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
package com.dremio.services.fabric;

import org.apache.arrow.memory.ArrowByteBufAllocator;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.rpc.BasicServer;
import com.dremio.exec.rpc.MessageDecoder;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.proto.FabricProto.FabricHandshake;
import com.dremio.services.fabric.proto.FabricProto.FabricIdentity;
import com.dremio.services.fabric.proto.FabricProto.FabricMessage;
import com.dremio.services.fabric.proto.FabricProto.RpcType;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

/**
 * Fabric server that accepts connection.
 */
class FabricServer extends BasicServer<RpcType, FabricConnection>{
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FabricServer.class);


  private final FabricMessageHandler handler;
  private final ConnectionManagerRegistry connectionRegistry;
  private final String address;
  private final BufferAllocator allocator;
  private volatile Integer port;
  private volatile FabricIdentity localIdentity;

  FabricServer(
      String address,
      FabricMessageHandler handler,
      RpcConfig config,
      BufferAllocator allocator,
      ConnectionManagerRegistry connectionRegistry,
      EventLoopGroup eventLoopGroup
      ) {
    super(
        config,
        new ArrowByteBufAllocator(allocator),
        eventLoopGroup
        );
    this.connectionRegistry = connectionRegistry;
    this.allocator = allocator;
    this.handler = handler;
    this.address = address;
  }

  @Override
  public int bind(int initialPort, boolean allowPortHunting) {
    port = super.bind(initialPort, allowPortHunting);
    localIdentity = FabricIdentity.newBuilder().setAddress(address).setPort(port).build();
    return port;
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return FabricMessage.getDefaultInstance();
  }

  @Override
  protected void handle(FabricConnection connection, int rpcType, byte[] pBody, ByteBuf dBody, ResponseSender sender)
      throws RpcException {
    handler.handle(connection.getIdentity(), localIdentity, connection, rpcType, pBody, dBody, sender);
  }

  @Override
  protected Response handle(FabricConnection connection, int rpcType, byte[] pBody, ByteBuf dBody) throws RpcException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FabricConnection initRemoteConnection(SocketChannel channel) {
    return new FabricConnection("fabric server", channel, this, allocator);
  }

  @Override
  protected ServerHandshakeHandler<FabricHandshake> newHandshakeHandler(final FabricConnection connection) {
    return new ServerHandshakeHandler<FabricHandshake>(RpcType.HANDSHAKE, FabricHandshake.PARSER) {

      @Override
      public MessageLite getHandshakeResponse(FabricHandshake inbound) throws Exception {
//        logger.debug("Handling handshake from other bit. {}", inbound);
        if (inbound.getRpcVersion() != FabricRpcConfig.RPC_VERSION) {
          throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.", inbound.getRpcVersion(), FabricRpcConfig.RPC_VERSION));
        }
        if (!inbound.hasIdentity() || inbound.getIdentity().getAddress().isEmpty() || inbound.getIdentity().getPort() < 1) {
          throw new RpcException(String.format("RPC didn't provide valid counter identity.  Received %s.", inbound.getIdentity()));
        }
        connection.setIdentity(inbound.getIdentity());

        final boolean isLoopback = inbound.getIdentity().getAddress().equals(address) && inbound.getIdentity().getPort() == port;

        if (!isLoopback) {
          FabricConnectionManager manager = connectionRegistry.getConnectionManager(inbound.getIdentity());

          // update the close handler.
          connection.wrapCloseHandler(manager.getCloseHandlerCreator());

          // add to the connection manager.
          manager.addExternalConnection(connection);
        }

        return FabricHandshake.newBuilder().setRpcVersion(FabricRpcConfig.RPC_VERSION).build();
      }

    };
  }

  @Override
  protected MessageDecoder newDecoder(BufferAllocator allocator) {
    return new FabricProtobufLengthDecoder(allocator);
  }

}
