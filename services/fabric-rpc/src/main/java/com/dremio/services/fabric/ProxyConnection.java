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

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.rpc.RemoteConnection;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.proto.FabricProto.FabricMessage;
import com.dremio.services.fabric.proto.FabricProto.RpcType;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import io.netty.buffer.ByteBuf;

/**
 * An artificial connection that allows us to reuse the command model for
 * existing users of that model but pass through a conversion layer to be used
 * for messaging.
 */
public class ProxyConnection extends RemoteConnection {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProxyConnection.class);

  private final RpcConfig rpcConfig;
  private final FabricConnection connection;
  private final FabricProtocol protocol;

  public ProxyConnection(FabricConnection connection, FabricProtocol protocol) {
    super(connection);
    this.connection = connection;
    this.protocol = protocol;
    this.rpcConfig = protocol.getConfig();
  }

  @Override
  protected BufferAllocator getAllocator() {
    return connection.getAllocator();
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void send(
      RpcOutcomeListener<RECEIVE> outcomeListener,
      EnumLite rpcType,
      SEND protobufBody,
      Class<RECEIVE> clazz,
      ByteBuf... dataBodies) {
    assert rpcConfig.checkSend(rpcType, protobufBody.getClass(), clazz);
    connection.send(new ProxyListener<RECEIVE>(outcomeListener), RpcType.MESSAGE, msg(rpcType, protobufBody), FabricMessage.class, dataBodies);
  }

  public <SEND extends MessageLite, RECEIVE extends MessageLite> void sendUnsafe(
      RpcOutcomeListener<RECEIVE> outcomeListener,
      EnumLite rpcType,
      SEND protobufBody,
      Class<RECEIVE> clazz,
      ByteBuf... dataBodies) {
    assert rpcConfig.checkSend(rpcType, protobufBody.getClass(), clazz);
    connection.sendUnsafe(new ProxyListener<RECEIVE>(outcomeListener), RpcType.MESSAGE, msg(rpcType, protobufBody), FabricMessage.class, dataBodies);
  }

  private <SEND extends MessageLite> FabricMessage msg(EnumLite rpcType, SEND protobufBody){
    return FabricMessage.newBuilder()
        .setProtocolId(protocol.getProtocolId())
        .setInnerRpcType(rpcType.getNumber())
        .setMessage(protobufBody.toByteString())
        .build();
  }

  private class ProxyListener<RECEIVE> implements RpcOutcomeListener<FabricMessage> {

    private RpcOutcomeListener<RECEIVE> innerListener;

    public ProxyListener(RpcOutcomeListener<RECEIVE> innerListener) {
      this.innerListener = innerListener;
    }

    @Override
    public void failed(RpcException ex) {
      innerListener.failed(ex);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void success(FabricMessage value, ByteBuf buffer) {
      try{
        MessageLite m = protocol.getResponseDefaultInstance(value.getInnerRpcType());
        assert rpcConfig.checkReceive(value.getInnerRpcType(), m.getClass());
        Parser<?> parser = m.getParserForType();
        Object convertedValue = parser.parseFrom(value.getMessage());
        innerListener.success((RECEIVE) convertedValue, buffer);
      }catch(Exception ex){
        logger.error("Failure", ex);
        innerListener.failed(new RpcException(ex));
      }
    }

    @Override
    public void interrupted(InterruptedException e) {
      innerListener.interrupted(e);
    }

  }
}
