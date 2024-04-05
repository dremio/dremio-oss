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
package com.dremio.jdbc.impl;

import com.dremio.exec.rpc.ConnectionFailedException;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcExceptionStatus;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.Test;

/** Class-level unit test for {@link DremioExceptionMapper}. */
public class DremioExceptionMapperTest {
  @Test
  public void testDefaultException() {
    final RpcException rpcEx = new RpcException();
    testException(rpcEx, null, null);
    testExceptionArgs(rpcEx, null, "bar: %s", "foo");
  }

  @Test
  public void testMessageException() {
    final String message = "myMessage";
    final RpcException rpcEx = new RpcException(message);
    testException(rpcEx, null, message);
  }

  @Test
  public void testConnectionInvalidMessageException() {
    final String message = "Error Status: " + RpcExceptionStatus.CONNECTION_INVALID + ", myMessage";
    final RpcException rpcEx = new RpcException(message);
    testException(rpcEx, "01002", message);
    testExceptionArgs(rpcEx, "01002", "bar: %s", "foo");
  }

  @Test
  public void testConnectionInvalidStatusException() {
    final String message = "myMessage";
    final RpcException rpcEx =
        new RpcException(message, RpcExceptionStatus.CONNECTION_INVALID, "errorId");
    testException(rpcEx, "01002", message);
    testExceptionArgs(rpcEx, "01002", "bar: %s", "foo");
  }

  @Test
  public void testConnectionException() {
    final String message = "myMessage";
    final RpcException rpcEx = new RpcException(message);
    final ConnectionFailedException connEx = new ConnectionFailedException(rpcEx);
    testException(connEx, "08001", message);
    testExceptionArgs(connEx, "08001", "bar: %s", "foo");
  }

  @Test
  public void testAuthException() {
    final String message = "myMessage";
    final RpcException rpcEx = new RpcException(message, RpcExceptionStatus.AUTH_FAILED, "errorId");
    testException(rpcEx, "28000", message);
    testExceptionArgs(rpcEx, "28000", "bar: %s", "foo");
  }

  @Test
  public void testAuthStringException() {
    final String message = "Error Status: " + RpcExceptionStatus.AUTH_FAILED + ", myMessage";
    final RpcException rpcEx = new RpcException(message, null, "errorId");
    testException(rpcEx, "28000", message);
    testExceptionArgs(rpcEx, "28000", "bar: %s", "foo");
  }

  @Test
  public void testInvalidStatusException() {
    final String message = "myMessage";
    final RpcException rpcEx = new RpcException(message, "SomeStatus", "otherErrorId");
    testException(rpcEx, null, message);
    testExceptionArgs(rpcEx, null, "bar: %s", "foo");
  }

  @Test
  public void testInvalidStringStatusException() {
    final String message = "Error Status: INVALID, myMessage";
    final RpcException rpcEx = new RpcException(message, null, "otherErrorId");
    testException(rpcEx, null, message);
    testExceptionArgs(rpcEx, null, "bar: %s", "foo");
  }

  private void testException(RpcException rpcEx, String sqlState, String message) {
    final SQLException sqlEx = DremioExceptionMapper.map(rpcEx);
    Assert.assertEquals(message, sqlEx.getMessage());
    Assert.assertEquals(sqlState, sqlEx.getSQLState());
    Assert.assertEquals(sqlEx.getCause(), rpcEx);
  }

  private void testExceptionArgs(
      RpcException rpcEx, String sqlState, String message, String... args) {
    final SQLException sqlEx = DremioExceptionMapper.map(rpcEx, message, args);
    final String formattedMsg =
        (message != null && args != null) ? String.format(message, args) : message;
    Assert.assertEquals(formattedMsg, sqlEx.getMessage());
    Assert.assertEquals(sqlState, sqlEx.getSQLState());
    Assert.assertEquals(sqlEx.getCause(), rpcEx);
  }
}
