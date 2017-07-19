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
package com.dremio.exec.rpc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.UserRpcException;

/**
 * Test class for {@code RpcException}
 */
public class TestRpcException {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestRpcException.class);

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testNullException() throws Exception {
    RpcException.propagateIfPossible(null, Exception.class);
  }

  @Test
  public void testNonRemoteException() throws Exception {
    RpcException e = new RpcException("Test message", new Exception());
    RpcException.propagateIfPossible(e, Exception.class);
  }

  @SuppressWarnings("serial")
  private static class TestException extends Exception{

    @SuppressWarnings("unused")
    public TestException(String message, Throwable cause) {
      super(message, cause);
    }

    public TestException(String message) {
      super(message);
    }
  }

  @Test
  public void testRemoteTestException() throws Exception {
    UserRemoteException ure = new UserRemoteException(UserException
        .unsupportedError(new UserRpcException(null, "user rpc exception", new TestException("test message")))
        .build(logger).getOrCreatePBError(false));

    exception.expect(TestException.class);
    exception.expectMessage("test message");
    RpcException.propagateIfPossible(new RpcException(ure), TestException.class);
  }

  @Test
  public void testRemoteRuntimeException() throws Exception {
    UserRemoteException ure = new UserRemoteException(UserException
        .unsupportedError(new UserRpcException(null, "user rpc exception", new RuntimeException("test message")))
        .build(logger).getOrCreatePBError(false));

    exception.expect(RuntimeException.class);
    exception.expectMessage("test message");
    RpcException.propagateIfPossible(new RpcException(ure), TestException.class);
  }
}
