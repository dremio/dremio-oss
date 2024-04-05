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
package com.dremio.exec.rpc.user.security;

import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.TEST_USER_1;
import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.TEST_USER_1_PASSWORD;
import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.TEST_USER_2;
import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.TEST_USER_2_PASSWORD;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.BaseTestQuery;
import com.dremio.exec.rpc.RpcException;
import com.dremio.sabot.rpc.user.UserSession;
import java.util.Properties;
import org.junit.Test;

public class TestCustomUserAuthenticator extends BaseTestQuery {
  @Test
  public void positiveUserAuth() throws Exception {
    runTest(TEST_USER_1, TEST_USER_1_PASSWORD);
    runTest(TEST_USER_2, TEST_USER_2_PASSWORD);
  }

  @Test
  public void negativeUserAuth() throws Exception {
    negativeAuthHelper(TEST_USER_1, "blah.. blah..");
    negativeAuthHelper(TEST_USER_2, "blah.. blah..");
    negativeAuthHelper(TEST_USER_2, "");
    negativeAuthHelper("invalidUserName", "blah.. blah..");
  }

  @Test
  public void positiveUserAuthAfterNegativeUserAuth() throws Exception {
    negativeAuthHelper("blah.. blah..", "blah.. blah..");
    runTest(TEST_USER_2, TEST_USER_2_PASSWORD);
  }

  private static void negativeAuthHelper(final String user, final String password)
      throws Exception {
    assertThatThrownBy(() -> runTest(user, password))
        .isInstanceOf(RpcException.class)
        .hasMessageContaining("HANDSHAKE_VALIDATION : Status: AUTH_FAILED")
        .hasMessageContaining("Invalid credentials");
  }

  private static void runTest(final String user, final String password) throws Exception {
    final Properties connectionProps = new Properties();

    connectionProps.setProperty(UserSession.USER, user);
    connectionProps.setProperty(UserSession.PASSWORD, password);

    updateClient(connectionProps);

    // Run few queries using the new client
    test("SHOW SCHEMAS");
    test("USE INFORMATION_SCHEMA");
    test("SHOW TABLES");
    test("SELECT * FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_NAME LIKE 'COLUMNS'");
    test("SELECT * FROM cp.\"region.json\" LIMIT 5");
  }
}
