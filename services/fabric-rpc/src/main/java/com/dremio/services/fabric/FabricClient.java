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

import java.util.Optional;

import org.apache.arrow.memory.ArrowByteBufAllocator;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.rpc.BasicClient;
import com.dremio.exec.rpc.MessageDecoder;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.proto.FabricProto.FabricHandshake;
import com.dremio.services.fabric.proto.FabricProto.FabricIdentity;
import com.dremio.services.fabric.proto.FabricProto.FabricMessage;
import com.dremio.services.fabric.proto.FabricProto.RpcType;
import com.dremio.ssl.SSLEngineFactory;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

/**
 * Client used to connect to server.
 */
class FabricClient extends BasicClient<RpcType, FabricConnection, FabricHandshake, FabricHandshake>{

//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FabricClient.class);

  private final FabricMessageHandler handler;
  private final FabricIdentity remoteIdentity;
  private final FabricConnectionManager.CloseHandlerCreator closeHandlerFactory;
  private final FabricIdentity localIdentity;
  private final BufferAllocator allocator;

  public FabricClient(
      RpcConfig config,
      EventLoopGroup eventLoop,
      BufferAllocator allocator,
      FabricIdentity remoteIdentity,
      FabricIdentity localIdentity,
      FabricMessageHandler handler,
      FabricConnectionManager.CloseHandlerCreator closeHandlerFactory,
      Optional<SSLEngineFactory> engineFactory
  ) throws RpcException {
    super(
        config,
        new ArrowByteBufAllocator(allocator),
        eventLoop,
        RpcType.HANDSHAKE,
        FabricHandshake.class,
        FabricHandshake.PARSER,
        engineFactory
    );
    this.localIdentity = localIdentity;
    this.remoteIdentity = remoteIdentity;
    this.handler = handler;
    this.closeHandlerFactory = closeHandlerFactory;
    this.allocator = allocator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public FabricConnection initRemoteConnection(SocketChannel channel) {
    return new FabricConnection("fabric client", channel, this, allocator);
  }

  @Override
  protected ChannelFutureListener newCloseListener(SocketChannel ch, FabricConnection connection) {
    return closeHandlerFactory.getHandler(connection, super.newCloseListener(ch, connection));
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    return FabricMessage.getDefaultInstance();
  }

  @Override
  protected void handle(FabricConnection connection, int rpcType, byte[] pBody, ByteBuf dBody, ResponseSender sender)
      throws RpcException {
    handler.handle(remoteIdentity, localIdentity, connection, rpcType, pBody, dBody, sender);
  }

  @Override
  protected Response handle(FabricConnection connection, int rpcType, byte[] pBody, ByteBuf dBody) throws RpcException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void validateHandshake(FabricHandshake handshake) throws RpcException {
    if (handshake.getRpcVersion() != FabricRpcConfig.RPC_VERSION) {
      throw new RpcException(String.format("Invalid rpc version.  Expected %d, actual %d.", handshake.getRpcVersion(), FabricRpcConfig.RPC_VERSION));
    }
  }

  @Override
  protected void finalizeConnection(FabricHandshake handshake, FabricConnection connection) {
    connection.setIdentity(handshake.getIdentity());
  }

  @Override
  public MessageDecoder newDecoder(BufferAllocator allocator) {
    return new FabricProtobufLengthDecoder(allocator);
  }

}
