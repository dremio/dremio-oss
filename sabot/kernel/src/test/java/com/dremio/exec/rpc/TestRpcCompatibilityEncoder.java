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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import com.dremio.exec.proto.CoordRPC.RpcType;
import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/** Test class for {@code RpcCompatibilityEncoder} */
public class TestRpcCompatibilityEncoder {

  @Test
  public void testIgnoreOtherMessages() throws Exception {
    RpcCompatibilityEncoder encoder = new RpcCompatibilityEncoder();
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);

    OutboundRpcMessage message = new OutboundRpcMessage(RpcMode.PING, 0, 0, Acks.OK);
    List<Object> out = new ArrayList<>();

    encoder.encode(context, message, out);

    assertEquals(1, out.size());
    assertSame(message, out.get(0));
  }

  @Test
  public void testIgnoreNonDremioPBErrorMessage() throws Exception {
    RpcCompatibilityEncoder encoder = new RpcCompatibilityEncoder();
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);

    OutboundRpcMessage message = new OutboundRpcMessage(RpcMode.RESPONSE_FAILURE, 0, 0, Acks.OK);
    List<Object> out = new ArrayList<>();

    encoder.encode(context, message, out);

    assertEquals(1, out.size());
    assertSame(message, out.get(0));
  }

  @Test
  public void testUpdateErrorType() throws Exception {
    RpcCompatibilityEncoder encoder = new RpcCompatibilityEncoder();
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);

    DremioPBError error =
        DremioPBError.newBuilder()
            .setErrorType(ErrorType.IO_EXCEPTION)
            .setMessage("test message")
            .build();

    OutboundRpcMessage message =
        new OutboundRpcMessage(RpcMode.RESPONSE_FAILURE, RpcType.RESP_QUERY_PROFILE, 12, error);
    List<Object> out = new ArrayList<>();

    encoder.encode(context, message, out);

    assertEquals(1, out.size());
    OutboundRpcMessage received = (OutboundRpcMessage) out.get(0);
    assertEquals(RpcMode.RESPONSE_FAILURE, received.mode);
    assertEquals(12, received.coordinationId);
    DremioPBError newError = (DremioPBError) received.pBody;
    assertEquals(ErrorType.RESOURCE, newError.getErrorType());
    assertEquals("test message", newError.getMessage());
  }
}
