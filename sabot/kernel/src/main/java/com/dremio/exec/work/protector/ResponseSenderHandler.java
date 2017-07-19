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
package com.dremio.exec.work.protector;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.google.protobuf.Internal.EnumLite;
import com.google.protobuf.MessageLite;

public abstract class ResponseSenderHandler<T extends MessageLite> implements UserResponseHandler {

  private final Class<T> clazz;
  private final ResponseSender sender;
  private final EnumLite type;

  public ResponseSenderHandler(EnumLite type, Class<T> clazz, ResponseSender sender) {
    this.sender = sender;
    this.clazz = clazz;
    this.type = type;
  }

  @Override
  public final void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
    throw new UnsupportedOperationException("A response sender based implementation should send no data to end users.");
  }

  @Override
  public final void completed(UserResult result) {
    if(result.getException() != null){
      sender.send(new Response(type, getException(result.getException())));
    }else{
      T value = result.unwrap(clazz);
      sender.send(new Response(type, value));
    }
  }

  protected abstract T getException(UserException ex);


}