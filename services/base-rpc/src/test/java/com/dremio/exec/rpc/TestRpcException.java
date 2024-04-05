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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import org.junit.Test;

/** Test class for {@code RpcException} */
public class TestRpcException {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestRpcException.class);

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
  private static class TestException extends Exception {

    @SuppressWarnings("unused")
    public TestException(String message, Throwable cause) {
      super(message, cause);
    }

    public TestException(String message) {
      super(message);
    }
  }

  @Test
  public void testRemoteTestException() {
    UserRemoteException ure =
        UserRemoteException.create(
            UserException.unsupportedError(
                    new UserRpcException(
                        null, "user rpc exception", new TestException("test message")))
                .build(logger)
                .getOrCreatePBError(false));

    assertThatThrownBy(
            () -> RpcException.propagateIfPossible(new RpcException(ure), TestException.class))
        .isInstanceOf(TestException.class)
        .hasMessageContaining("test message");
  }

  @Test
  public void testRemoteRuntimeException() {
    UserRemoteException ure =
        UserRemoteException.create(
            UserException.unsupportedError(
                    new UserRpcException(
                        null, "user rpc exception", new RuntimeException("test message")))
                .build(logger)
                .getOrCreatePBError(false));

    assertThatThrownBy(
            () -> RpcException.propagateIfPossible(new RpcException(ure), TestException.class))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("test message");
  }
}
