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

import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;

/**
 * An RpcBus specifically for client purposes. Primarily abstracted for testing purposes.
 *
 * @param <T> The type of the protocol enumeration.
 * @param <CONNECTION_TYPE> The connection type to have.
 * @param <OUTBOUND_HANDSHAKE> The handshake to send to the remote server.
 */
abstract class AbstractClient<T extends EnumLite, CONNECTION_TYPE extends RemoteConnection, OUTBOUND_HANDSHAKE extends MessageLite> extends RpcBus<T, CONNECTION_TYPE> {

  protected abstract void connectAsClient(RpcConnectionHandler<CONNECTION_TYPE> connectionHandler, OUTBOUND_HANDSHAKE handshakeValue, String host, int port);

  public AbstractClient(RpcConfig rpcConfig) {
    super(rpcConfig);
  }


}
