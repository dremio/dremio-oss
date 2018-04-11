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
package com.dremio.services.fabric.api;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcException;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;

/**
 * An interface that allows application developers to express a pseudo-wire protocol.
 */
public interface FabricProtocol {

  /**
   * Returns a unique protocol id. In a particular fabric instance, each protocol must implement its own protocol id.
   * @return A unique id.
   */
  public int getProtocolId();

  /**
   * Returns the allocator that should be associated with this protocol.
   * @return An allocator.
   */
  public BufferAllocator getAllocator();

  /**
   * The REQUEST <> RESPONSE configuration that should be used for this
   * protocol. Note that Executor and Timeout options are ignored if set since
   * those are a purview of the actual wire protocol.
   *
   * @return A communication configuration.
   */
  public RpcConfig getConfig();

  /**
   * Returns the default response id for a particular incoming message type.
   * @param rpcType The number associated with the EnumLite that was used to send the message.
   * @return A default instance of the expected Message type.
   * @throws RpcException
   */
  public MessageLite getResponseDefaultInstance(int rpcType) throws RpcException;

  /**
   * How this protocol will handle incoming REQUEST messages.
   * @param connection The connection associated with the message.
   * @param rpcType The incoming message type received.
   * @param pBody The serialized protobuf body associated with the message.
   * @param dBody The optional collection of bytes associated with a message.
   * @param sender An callback handler than can be used to return the response.
   * @throws RpcException
   */
  public void handle(final PhysicalConnection connection, final int rpcType, final ByteString pBody, final ByteBuf dBody, final ResponseSender sender) throws RpcException;

}
