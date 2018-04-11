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
package com.dremio.services.fabric.simple;

import com.google.protobuf.MessageLite;

import io.netty.buffer.ArrowBuf;

/**
 * A response on the reception side. Note that this is similar to a response on
 * the sending side except that only holds a single buffer since all sent
 * buffers are concatenating when being sent.
 *
 * @param <RESPONSE>
 */
public class ReceivedResponseMessage<RESPONSE extends MessageLite> {

  private final RESPONSE body;
  private final ArrowBuf buffer;

  public ReceivedResponseMessage(RESPONSE body, ArrowBuf buffer) {
    super();
    this.body = body;
    this.buffer = buffer;
  }

  public RESPONSE getBody() {
    return body;
  }

  public ArrowBuf getBuffer() {
    return buffer;
  }

}
