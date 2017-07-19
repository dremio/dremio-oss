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
package com.dremio.services.fabric.api;

import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.proto.FabricProto.RpcType;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;

/**
 * A simplified connection-like object for sending messages.
 */
public interface PhysicalConnection {

  <SEND extends MessageLite, RECEIVE extends MessageLite> void send(RpcOutcomeListener<RECEIVE> outcomeListener,
      RpcType rpcType, SEND protobufBody, Class<RECEIVE> clazz, ByteBuf... dataBodies);

  boolean isActive();

}
