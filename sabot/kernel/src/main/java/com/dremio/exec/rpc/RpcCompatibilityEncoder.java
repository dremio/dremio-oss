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

import com.dremio.common.exceptions.ErrorCompatibility;
import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.google.protobuf.MessageLite;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;

/**
 * A message encoder to reencode error messages so they are backward compatible with Drill
 *
 * <p>Located in this package due to package privacy settings of OutboundRpcMessage.
 */
public final class RpcCompatibilityEncoder extends MessageToMessageEncoder<OutboundRpcMessage> {
  @Override
  protected void encode(ChannelHandlerContext context, OutboundRpcMessage message, List<Object> out)
      throws Exception {
    if (message.mode != RpcMode.RESPONSE_FAILURE) {
      out.add(message);
      return;
    }

    final MessageLite pBody = message.pBody;
    if (!(pBody instanceof DremioPBError)) {
      out.add(message);
      return;
    }

    DremioPBError error = (DremioPBError) pBody;
    DremioPBError newError = ErrorCompatibility.convertIfNecessary(error);

    out.add(
        new OutboundRpcMessage(
            message.mode, message.rpcType, message.coordinationId, newError, message.dBodies));
  }
}
