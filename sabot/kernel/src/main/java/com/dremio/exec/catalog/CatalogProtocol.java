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
package com.dremio.exec.catalog;

import static com.dremio.exec.rpc.RpcBus.get;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.SerializedExecutor;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.catalog.CatalogServiceImpl.CatalogChangeListener;
import com.dremio.exec.proto.CatalogRPC.RpcType;
import com.dremio.exec.proto.CatalogRPC.SourceWrapper;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcConstants;
import com.dremio.exec.rpc.RpcException;
import com.dremio.sabot.rpc.Protocols;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.services.fabric.api.FabricProtocol;
import com.dremio.services.fabric.api.PhysicalConnection;
import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.protostuff.ProtobufIOUtil;

/**
 * Protocol to communicate source changes between nodes.
 */
class CatalogProtocol implements FabricProtocol, AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CatalogProtocol.class);

  private static final Response OK = new Response(RpcType.ACK, Acks.OK);

  private final CatalogChangeListener listener;
  private final BufferAllocator allocator;
  private final SabotConfig config;
  private final SExecutor serializedExecutor;
  private final ExecutorService executor;

  public CatalogProtocol(BufferAllocator allocator, CatalogChangeListener listener, SabotConfig config) {
    this.listener = listener;
    this.allocator = allocator;
    this.config = config;
    this.executor = Executors.newSingleThreadExecutor();
    this.serializedExecutor = new SExecutor(executor);
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public RpcConfig getConfig() {
    return getMapping(config);
  }

  @Override
  public int getProtocolId() {
    return Protocols.CATALOG;
  }

  @Override
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch (rpcType) {
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();

    case RpcType.REQ_SOURCE_CONFIG_VALUE:
      return SourceWrapper.getDefaultInstance();

    case RpcType.REQ_DEL_SOURCE_VALUE:
      return SourceWrapper.getDefaultInstance();

    default:
      throw new UnsupportedOperationException();
    }
  }

  public void close() {
    executor.shutdownNow();
  }

  @Override
  public void handle(
      PhysicalConnection connection,
      int rpcType,
      ByteString pBody,
      ByteBuf dBody,
      ResponseSender sender) throws RpcException {

    switch (rpcType) {

    case RpcType.REQ_SOURCE_CONFIG_VALUE: {
      final SourceWrapper wrapper = get(pBody, SourceWrapper.PARSER);
      serializedExecutor.execute(new MessageHandler(wrapper, sender, true));
      break;
    }

    case RpcType.REQ_DEL_SOURCE_VALUE: {
      final SourceWrapper wrapper = get(pBody, SourceWrapper.PARSER);
      serializedExecutor.execute(new MessageHandler(wrapper, sender, false));
      break;
    }

    default:
      throw new RpcException("Message received that is not yet supported. Message type: " + rpcType);
    }
  }

  private static RpcConfig getMapping(SabotConfig config) {
    return RpcConfig.newBuilder()
        .name("CatalogProtocol")
        .timeout(config.getInt(RpcConstants.BIT_RPC_TIMEOUT))
        .add(RpcType.REQ_SOURCE_CONFIG, SourceWrapper.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_DEL_SOURCE, SourceWrapper.class, RpcType.ACK, Ack.class)
        .build();
  }

  private class MessageHandler implements Runnable {
    private final SourceWrapper wrapper;
    private final ResponseSender sender;
    private final boolean update;

    public MessageHandler(SourceWrapper wrapper, ResponseSender sender, boolean update) {
      super();
      this.wrapper = wrapper;
      this.sender = sender;
      this.update = update;
    }

    @Override
    public void run() {
      try {
        SourceConfig config = new SourceConfig();
        ProtobufIOUtil.mergeFrom(wrapper.getBytes().toByteArray(), config, SourceConfig.getSchema());
        if(update) {
          listener.sourceUpdate(config);
        } else {
          listener.sourceDelete(config);
        }
      } finally {
        sender.send(OK);
      }
    }

    @Override
    public String toString() {
      SourceConfig config = new SourceConfig();
      ProtobufIOUtil.mergeFrom(wrapper.getBytes().toByteArray(), config, SourceConfig.getSchema());
      return MoreObjects.toStringHelper(MessageHandler.class)
          .add("config", config)
          .add("update", update)
          .toString();
    }


  }

  private class SExecutor extends SerializedExecutor<MessageHandler> {

    SExecutor(Executor underlyingExecutor) {
      super("source-synchronization-message-handler", underlyingExecutor, false /** events are quick and to string is complex **/);
    }

    @Override
    protected void runException(MessageHandler runnable, Throwable exception) {
      logger.error("Exception occurred in catalog synchronization. Message {}", runnable, exception);
    }

  }
}
