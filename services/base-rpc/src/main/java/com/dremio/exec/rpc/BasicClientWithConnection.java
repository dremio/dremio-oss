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

import java.util.Optional;

import org.apache.arrow.memory.ArrowByteBufAllocator;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.rpc.BasicClientWithConnection.ServerConnection;
import com.dremio.ssl.SSLEngineFactory;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

public abstract class BasicClientWithConnection<T extends EnumLite, HANDSHAKE_SEND extends MessageLite, HANDSHAKE_RESPONSE extends MessageLite> extends BasicClient<T, ServerConnection, HANDSHAKE_SEND, HANDSHAKE_RESPONSE>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicClientWithConnection.class);

  private BufferAllocator alloc;
  private final String connectionName;

  public BasicClientWithConnection(
      RpcConfig rpcMapping,
      BufferAllocator alloc,
      EventLoopGroup eventLoopGroup,
      T handshakeType,
      Class<HANDSHAKE_RESPONSE> responseClass,
      Parser<HANDSHAKE_RESPONSE> handshakeParser,
      String connectionName,
      Optional<SSLEngineFactory> engineFactory
  ) throws RpcException {
    super(rpcMapping, new ArrowByteBufAllocator(alloc), eventLoopGroup, handshakeType, responseClass, handshakeParser, engineFactory);
    this.alloc = alloc;
    this.connectionName = connectionName;
  }

  @Override
  protected Response handle(ServerConnection connection, int rpcType, byte[] pBody, ByteBuf dBody) throws RpcException {
    return handleReponse( (ConnectionThrottle) connection, rpcType, pBody, dBody);
  }

  protected abstract Response handleReponse(ConnectionThrottle throttle, int rpcType, byte[] pBody, ByteBuf dBody) throws RpcException ;

  @Override
  public ServerConnection initRemoteConnection(SocketChannel channel) {
    return new ServerConnection(connectionName, channel, alloc);
  }

  public static class ServerConnection extends RemoteConnection{

    private final BufferAllocator alloc;

    public ServerConnection(String name, SocketChannel channel, BufferAllocator alloc) {
      super(channel, name);
      this.alloc = alloc;
    }

    @Override
    public BufferAllocator getAllocator() {
      return alloc;
    }
  }
}
