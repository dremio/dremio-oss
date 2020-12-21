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
package com.dremio.services.fabric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.ArrowBuf;
import org.junit.Test;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.simple.AbstractReceiveHandler;
import com.dremio.services.fabric.simple.ProtocolBuilder;
import com.dremio.services.fabric.simple.ReceivedResponseMessage;
import com.dremio.services.fabric.simple.SendEndpointCreator;
import com.dremio.services.fabric.simple.SentResponseMessage;

import io.netty.buffer.NettyArrowBuf;

/**
 * Protocol builder test.
 */
public class TestProtocolBuilder extends BaseTestFabric {

  @Test
  public void basicProtocolNoBuffers() throws Exception {
    ProtocolBuilder builder = ProtocolBuilder.builder().allocator(allocator).name("test-fabric").protocolId(3);

    SendEndpointCreator<NodeEndpoint, QueryId> ep1 = builder.register(1,
        new AbstractReceiveHandler<NodeEndpoint, QueryId>(NodeEndpoint.getDefaultInstance(),
            QueryId.getDefaultInstance()) {

          @Override
          public SentResponseMessage<QueryId> handle(NodeEndpoint request, ArrowBuf dBody) throws RpcException {
            if (!request.equals(expectedD)) {
              throw new RpcException("Objects not same.");
            }
            return new SentResponseMessage<>(expectedQ);
          }
        });


    builder.register(fabric);

    ReceivedResponseMessage<QueryId> resp1 = ep1.getEndpoint(address, fabric.getPort()).send(expectedD);
    assertEquals(resp1.getBody(), expectedQ);
    assertTrue(resp1.getBuffer() == null);
  }

  @Test(expected=RpcException.class)
  public void propagateFailure() throws Exception {
    ProtocolBuilder builder = ProtocolBuilder.builder().allocator(allocator).name("test-fabric").protocolId(3);

    SendEndpointCreator<NodeEndpoint, QueryId> ep1 = builder.register(1,
        new AbstractReceiveHandler<NodeEndpoint, QueryId>(NodeEndpoint.getDefaultInstance(),
            QueryId.getDefaultInstance()) {

          @Override
          public SentResponseMessage<QueryId> handle(NodeEndpoint request, ArrowBuf dBody) throws RpcException {
            throw new NullPointerException("an npe.");
          }
        });


    builder.register(fabric);
    ReceivedResponseMessage<QueryId> resp1 = ep1.getEndpoint(address, fabric.getPort()).send(expectedD);
    assertEquals(resp1.getBody(), expectedQ);
    assertTrue(resp1.getBuffer() == null);
  }



  @Test
  public void protocolWithBuffers() throws Exception {
    try (
        ArrowBuf random1 = allocator.buffer(1024);
        ArrowBuf random2 = allocator.buffer(1024);) {

      final byte[] r1B = new byte[1024];
      final byte[] r2B = new byte[1024];
      random.nextBytes(r1B);
      random.nextBytes(r2B);
      random1.writeBytes(r1B);
      random2.writeBytes(r2B);

      ProtocolBuilder builder = ProtocolBuilder.builder().allocator(allocator).name("test-fabric").protocolId(3);

      SendEndpointCreator<NodeEndpoint, QueryId> ep2 = builder.register(2,
          new AbstractReceiveHandler<NodeEndpoint, QueryId>(NodeEndpoint.getDefaultInstance(),
              QueryId.getDefaultInstance()) {

            @Override
            public SentResponseMessage<QueryId> handle(NodeEndpoint request, ArrowBuf dBody) throws RpcException {
              if (!request.equals(expectedD)) {
                throw new RpcException("Objects not same.");
              }
              assertEqualsRpc(r1B, dBody);
              random2.retain();
              return new SentResponseMessage<>(expectedQ, NettyArrowBuf.unwrapBuffer(random2));
            }
          });

      builder.register(fabric);

      random1.retain();
      ReceivedResponseMessage<QueryId> resp2 = ep2.getEndpoint(address, fabric.getPort()).send(expectedD, random1);
      assertEquals(resp2.getBody(), expectedQ);
      assertEqualsBytes(r2B, resp2.getBuffer());
      resp2.getBuffer().release();
    }
  }

}
