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

import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;

/**
 * A response before it has transited the wire.
 *
 * @param <RESPONSE>
 */
public class SentResponseMessage<RESPONSE extends MessageLite> {

  private final RESPONSE body;
  private final ByteBuf[] buffers;

  public SentResponseMessage(RESPONSE body, ByteBuf... buffers) {
    super();
    this.body = body;
    this.buffers = buffers;
  }

  public RESPONSE getBody() {
    return body;
  }

  public ByteBuf[] getBuffers() {
    return buffers;
  }

}
