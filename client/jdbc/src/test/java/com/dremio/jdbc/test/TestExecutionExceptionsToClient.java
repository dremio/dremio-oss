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
package com.dremio.jdbc.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.jdbc.JdbcWithServerTestBase;
import java.sql.SQLException;
import java.sql.Statement;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExecutionExceptionsToClient extends JdbcWithServerTestBase {

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    JdbcWithServerTestBase.setUpConnection();
    try (Statement stmt = getConnection().createStatement()) {
      stmt.execute(
          String.format(
              "alter session set \"%s\" = false", ExecConstants.ENABLE_REATTEMPTS.getOptionName()));
    }
  }

  @Test
  public void testExecuteQueryThrowsRight1() throws Exception {
    final Statement statement = getConnection().createStatement();
    assertThatExceptionOfType(SQLException.class)
        .isThrownBy(() -> statement.executeQuery("SELECT one case of syntax error"))
        .havingCause()
        .isInstanceOf(UserRemoteException.class)
        .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class))
        .extracting(UserRemoteException::getErrorType)
        .satisfiesAnyOf(
            t -> assertThat(t).isEqualTo(ErrorType.SYSTEM),
            t -> assertThat(t).isEqualTo(ErrorType.PARSE));
  }

  @Test
  public void testExecuteThrowsRight1() throws Exception {
    final Statement statement = getConnection().createStatement();
    assertThatExceptionOfType(SQLException.class)
        .isThrownBy(() -> statement.execute("SELECT one case of syntax error"))
        .havingCause()
        .isInstanceOf(UserRemoteException.class)
        .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class))
        .extracting(UserRemoteException::getErrorType)
        .satisfiesAnyOf(
            t -> assertThat(t).isEqualTo(ErrorType.SYSTEM),
            t -> assertThat(t).isEqualTo(ErrorType.PARSE));
  }

  @Test
  public void testExecuteUpdateThrowsRight1() throws Exception {
    final Statement statement = getConnection().createStatement();
    assertThatExceptionOfType(SQLException.class)
        .isThrownBy(() -> statement.executeUpdate("SELECT one case of syntax error"))
        .havingCause()
        .isInstanceOf(UserRemoteException.class)
        .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class))
        .extracting(UserRemoteException::getErrorType)
        .satisfiesAnyOf(
            t -> assertThat(t).isEqualTo(ErrorType.SYSTEM),
            t -> assertThat(t).isEqualTo(ErrorType.PARSE));
  }

  @Test
  public void testExecuteQueryThrowsRight2() throws Exception {
    final Statement statement = getConnection().createStatement();
    assertThatExceptionOfType(SQLException.class)
        .isThrownBy(() -> statement.executeQuery("BAD QUERY 1"))
        .havingCause()
        .isInstanceOf(UserRemoteException.class)
        .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class))
        .extracting(UserRemoteException::getErrorType)
        .satisfiesAnyOf(
            t -> assertThat(t).isEqualTo(ErrorType.SYSTEM),
            t -> assertThat(t).isEqualTo(ErrorType.PARSE));
  }

  @Test
  public void testExecuteThrowsRight2() throws Exception {
    final Statement statement = getConnection().createStatement();
    assertThatExceptionOfType(SQLException.class)
        .isThrownBy(() -> statement.execute("worse query 2"))
        .havingCause()
        .isInstanceOf(UserRemoteException.class)
        .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class))
        .extracting(UserRemoteException::getErrorType)
        .satisfiesAnyOf(
            t -> assertThat(t).isEqualTo(ErrorType.SYSTEM),
            t -> assertThat(t).isEqualTo(ErrorType.PARSE));
  }

  @Test
  public void testExecuteUpdateThrowsRight2() throws Exception {
    final Statement statement = getConnection().createStatement();
    assertThatExceptionOfType(SQLException.class)
        .isThrownBy(() -> statement.executeUpdate("naughty, naughty query 3"))
        .havingCause()
        .isInstanceOf(UserRemoteException.class)
        .asInstanceOf(InstanceOfAssertFactories.type(UserRemoteException.class))
        .extracting(UserRemoteException::getErrorType)
        .satisfiesAnyOf(
            t -> assertThat(t).isEqualTo(ErrorType.SYSTEM),
            t -> assertThat(t).isEqualTo(ErrorType.PARSE));
  }
}
