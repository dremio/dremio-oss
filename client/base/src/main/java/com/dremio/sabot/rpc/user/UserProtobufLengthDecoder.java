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
package com.dremio.sabot.rpc.user;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.rpc.MessageDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class UserProtobufLengthDecoder extends MessageDecoder {

  public UserProtobufLengthDecoder(BufferAllocator allocator) {
    super(allocator);

  }
  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    super.decode(ctx, in, out);
  }

}
