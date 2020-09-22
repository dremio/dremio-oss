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
package com.dremio.exec.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcException;

/**
 * Tests DremioClient connection resource cleanup upon encountering exceptions in connect() method.
 */
public class TestDremioClientConnectTearDown {
  private enum TestExceptionType {
    RUNTIME_EXCEPTION,
    GENERIC_RPC_EXCEPTION
  }

  private static class MockDremioClient extends DremioClient {
    private boolean isCleanUpResourcesCalled;
    private final TestExceptionType exceptionType;

    MockDremioClient(TestExceptionType exceptionType) {
      super();
      isCleanUpResourcesCalled = false;
      this.exceptionType = exceptionType;
    }

    @Override
    public NodeEndpoint setUpResources(String connect, Properties props) {
      return NodeEndpoint.newBuilder().build();
    }

    @Override
    public void connect(NodeEndpoint endpoint) throws RpcException {
      switch(exceptionType) {
        case RUNTIME_EXCEPTION:
          throw new RuntimeException("Test RuntimeException thrown in connect(NodeEndpoint)");
        case GENERIC_RPC_EXCEPTION:
          throw new RpcException("Test RpcException thrown in connect(NodeEndpoint)");
        default:
          break;
      }
    }

    @Override
    public void cleanUpResources() {
      isCleanUpResourcesCalled = true;
    }
  }

  @Test
  public void testGenericRpcExResourceCleanUp() {
    MockDremioClient client = new MockDremioClient(TestExceptionType.GENERIC_RPC_EXCEPTION);
    assertFalse(client.isCleanUpResourcesCalled);
    try {
      client.connect();
      fail();
    } catch(RpcException e) {
      assertTrue(client.isCleanUpResourcesCalled);
    }
  }

  @Test
  public void testRuntimeExResourceCleanUp() throws RpcException {
     MockDremioClient client = new MockDremioClient(TestExceptionType.RUNTIME_EXCEPTION);
    assertFalse(client.isCleanUpResourcesCalled);
     try {
       client.connect();
       fail();
     } catch (RuntimeException e) {
       assertTrue(client.isCleanUpResourcesCalled);
     }
  }
}
