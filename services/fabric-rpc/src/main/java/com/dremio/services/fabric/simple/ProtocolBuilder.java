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
package com.dremio.services.fabric.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcConfig.RpcConfigBuilder;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.services.fabric.api.FabricCommandRunner;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

/**
 * Builder for creating simplified protocols on top of the fabric infrastructure.
 */
public final class ProtocolBuilder {

  private int protocolId;
  private BufferAllocator allocator;
  private String name;
  private long timeoutMillis = 120 * 1000;
  private final ProxyFactory proxyFactory = new ProxyFactory();
  private Map<Integer, ReceiveHandler<MessageLite, MessageLite>> handlers = new HashMap<>();


  private ProtocolBuilder(){
  }

  public static ProtocolBuilder builder(){
    return new ProtocolBuilder();
  }

  @SuppressWarnings("unchecked")
  public <REQUEST extends MessageLite, RESPONSE extends MessageLite>
  SendEndpointCreator<REQUEST, RESPONSE> register(int id, ReceiveHandler<REQUEST, RESPONSE> handler) {
    Preconditions.checkArgument(id > -1 && id < 2048, "A request id must be between 0 and 2047.");
    Preconditions.checkNotNull(handler);
    Preconditions.checkArgument(!handlers.containsKey(id), "Only a single handler can be registered per id. You tried to register a handler for id %d twice.", id);
    handlers.put(id, (ReceiveHandler<MessageLite, MessageLite>) handler);
    return new EndpointCreator<REQUEST, RESPONSE>(proxyFactory, new PseudoEnum(id), (Class<RESPONSE>) handler.getDefaultResponse().getClass(), timeoutMillis);
  }

  public ProtocolBuilder name(String name) {
    this.name = name;
    return this;
  }

  public ProtocolBuilder timeout(long timeoutMillis) {
    Preconditions.checkArgument(timeoutMillis > -1);
    Preconditions.checkArgument(handlers.isEmpty(), "You can only set a timeout before registering any handlers. You've already registered %d handlers.", handlers.size());
    this.timeoutMillis = timeoutMillis;
    return this;
  }

  public ProtocolBuilder protocolId(int protocolId) {
    this.protocolId = protocolId;
    validateProtocol();
    return this;
  }

  public ProtocolBuilder allocator(BufferAllocator allocator) {
    Preconditions.checkNotNull(allocator);
    this.allocator = allocator;
    return this;
  }

  private void validateProtocol(){
    Preconditions.checkArgument(protocolId > 1 && protocolId < 64, "ProtocolId must be between 2 and 63. You tried to set it to %d.", protocolId);
  }

  public void register(FabricService fabric){
    Preconditions.checkArgument(proxyFactory.factory == null, "You can only register a protocol builder once.");
    validateProtocol();
    Preconditions.checkNotNull(name, "Name must be set.");
    Preconditions.checkNotNull(allocator, "Allocator must be set.");
    Preconditions.checkArgument(handlers.size() > 0, "You must add at least one handler to your protocol.");

    FabricProtocol protocol = new SimpleProtocol(protocolId, handlers, allocator, name);
    proxyFactory.factory = fabric.registerProtocol(protocol);
  }

  private static class SimpleProtocol implements FabricProtocol {

    private final int protocolId;
    private final ReceiveHandler<MessageLite,MessageLite>[] handlers;
    private final MessageLite[] defaultResponseInstances;
    private final MessageLite[] defaultRequestInstances;
    private final BufferAllocator allocator;
    private final RpcConfig config;

    @SuppressWarnings("unchecked")
    public SimpleProtocol(int protocolId, Map<Integer, ReceiveHandler<MessageLite, MessageLite>> handlers, BufferAllocator allocator, String name) {
      super();
      this.protocolId = protocolId;
      this.handlers = new ReceiveHandler[2048];
      this.defaultResponseInstances = new MessageLite[2048];
      this.defaultRequestInstances = new MessageLite[2048];
      RpcConfigBuilder builder = RpcConfig.newBuilder()
          .name(name)
          .timeout(0);
      for(Entry<Integer, ReceiveHandler<MessageLite, MessageLite>> e : handlers.entrySet()) {
        final int id = e.getKey();
        final ReceiveHandler<?,?> handler = e.getValue();
        final EnumLite num = new PseudoEnum(id);
        builder.add(num, (Class<? extends MessageLite>) handler.getDefaultRequest().getClass(), num, (Class<? extends MessageLite>) handler.getDefaultResponse().getClass());
        this.handlers[id] = e.getValue();
        this.defaultResponseInstances[id] = e.getValue().getDefaultResponse();
        this.defaultRequestInstances[id] = e.getValue().getDefaultRequest();
      }
      this.config = builder.build();
      this.allocator = allocator;
    }

    @Override
    public int getProtocolId() {
      return protocolId;
    }

    @Override
    public BufferAllocator getAllocator() {
      return allocator;
    }

    @Override
    public RpcConfig getConfig() {
      return config;
    }

    @Override
    public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
      return defaultResponseInstances[rpcType];
    }

    @Override
    public void handle(PhysicalConnection connection, int rpcType, ByteString pBody, ByteBuf dBody,
        ResponseSender sender) throws RpcException {
      MessageLite defaultInstance = defaultRequestInstances[rpcType];
      try{
        MessageLite value = defaultInstance.getParserForType().parseFrom(pBody);
        ArrowBuf dBody1 = dBody != null ? ((NettyArrowBuf) dBody).arrowBuf() : null;
        SentResponseMessage<MessageLite> response = handlers[rpcType].handle(value, dBody1);
        sender.send(new Response(new PseudoEnum(rpcType), response.getBody(), response.getBuffers()));
      } catch(Exception e){
        final String fail = String.format("Failure consuming message for protocol[%d], request[%d] in the %s rpc layer.", getProtocolId(), rpcType, getConfig().getName());
        throw new UserRpcException(NodeEndpoint.getDefaultInstance(), fail, e);
      }

    }
  }

  /**
   * A fabric runner factory that proxies another, checking it is set before returning any command runners.
   */
  private class ProxyFactory implements FabricRunnerFactory {
    private FabricRunnerFactory factory;
    @Override
    public FabricCommandRunner getCommandRunner(String address, int port) {
      Preconditions.checkNotNull(factory, "You must register your protocol before you attempt to send a message.");
      return factory.getCommandRunner(address, port);
    }

  }

  private static class PseudoEnum implements EnumLite {
    private final int number;

    public PseudoEnum(int number) {
      this.number = number;
    }

    @Override
    public int getNumber() {
      return number;
    }

    @Override
    public String toString() {
      return Integer.toString(number);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + number;
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
      PseudoEnum other = (PseudoEnum) obj;
      if (number != other.number) {
        return false;
      }
      return true;
    }


  }
}
