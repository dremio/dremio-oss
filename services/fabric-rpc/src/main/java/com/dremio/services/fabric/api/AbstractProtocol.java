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
package com.dremio.services.fabric.api;

import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;

/**
 * An abstract protocol that provides a handle() method that treats requests as synchronous for
 * simplicity.
 */
public abstract class AbstractProtocol implements FabricProtocol {

  @Override
  public void handle(
      PhysicalConnection connection,
      int rpcType,
      ByteString pBody,
      ByteBuf dBody,
      ResponseSender sender)
      throws RpcException {
    sender.send(handle(connection, rpcType, pBody, dBody));
  }

  protected abstract Response handle(
      PhysicalConnection connection, int rpcType, ByteString pBody, ByteBuf dBody)
      throws RpcException;
}
