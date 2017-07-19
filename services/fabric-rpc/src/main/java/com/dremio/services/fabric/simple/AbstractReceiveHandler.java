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
package com.dremio.services.fabric.simple;

import com.dremio.exec.rpc.RpcException;
import com.google.protobuf.MessageLite;

/**
 * An abstract implementation of receiver handler that manage the default response and request fields.
 *
 * @param <REQUEST>
 * @param <RESPONSE>
 */
public abstract class AbstractReceiveHandler<REQUEST extends MessageLite, RESPONSE extends MessageLite> implements ReceiveHandler<REQUEST, RESPONSE> {

  private final REQUEST defaultRequest;
  private final RESPONSE defaultResponse;

  protected AbstractReceiveHandler(REQUEST defaultRequest, RESPONSE defaultResponse) throws RpcException {
    this.defaultRequest = defaultRequest;
    this.defaultResponse = defaultResponse;
  }

  @Override
  public RESPONSE getDefaultResponse() {
    return defaultResponse;
  }

  @Override
  public REQUEST getDefaultRequest() {
    return defaultRequest;
  }

}
