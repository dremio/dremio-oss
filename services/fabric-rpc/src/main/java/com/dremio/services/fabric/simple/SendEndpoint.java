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

import com.dremio.exec.rpc.RpcException;
import com.google.protobuf.MessageLite;
import org.apache.arrow.memory.ArrowBuf;

/**
 * Interface that the ProtocolBuilder returns to allow someone to send message to a registered
 * handler.
 *
 * @param <REQUEST>
 * @param <RESPONSE>
 */
public interface SendEndpoint<REQUEST extends MessageLite, RESPONSE extends MessageLite> {

  public ReceivedResponseMessage<RESPONSE> send(REQUEST message, ArrowBuf... bufs)
      throws RpcException;
}
