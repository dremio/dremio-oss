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
package com.dremio.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


/**
 * Test for Dremio's implementation of Connection's methods (other than
 * main transaction-related methods in {@link ConnectionTransactionMethodsTest}).
 */
public class ConnectionTest extends JdbcWithServerTestBase {
  private static ExecutorService executor;

  @BeforeClass
  public static void setUpExecutor() {
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterClass
  public static void tearDownExecutor() {
    if (executor != null) {
      executor.shutdown();
    }
  }

  @Test
  public void testGetCatalog() throws SQLException {
    assertThat(getConnection().getCatalog()).isEqualTo("DREMIO");
  }

  ////////////////////////////////////////
  // Basic tests of statement creation methods (not necessarily executing
  // statements):

  //////////
  // Simplest cases of createStatement, prepareStatement, prepareCall:

  @Test
  public void testCreateStatementBasicCaseWorks() throws SQLException {
    Statement stmt = getConnection().createStatement();
    ResultSet rs = stmt.executeQuery("VALUES 1");
    assertThat(rs.next()).isTrue();
  }

  @Test
  public void testPrepareStatementBasicCaseWorks() throws SQLException {
    PreparedStatement stmt = getConnection().prepareStatement("VALUES 1");
    ResultSet rs = stmt.executeQuery();
    assertThat(rs.next()).isTrue();
  }

  @Test
  public void testPrepareCallThrows() {
    assertThatThrownBy(() -> getConnection().prepareCall("VALUES 1"))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  //////////
  // createStatement(int, int):

  @Test
  public void testCreateStatement_overload2_supportedCase_returns() throws SQLException {
    getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY);
  }

  @Test
  @Ignore("until unsupported characteristics are rejected")
  public void testCreateStatement_overload2_unsupportedType1_throws() {
    assertThatThrownBy(() -> getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
      ResultSet.CONCUR_READ_ONLY))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  @Ignore("until unsupported characteristics are rejected")
  public void testCreateStatement_overload2_unsupportedType2_throws() {
    assertThatThrownBy(() -> getConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
      ResultSet.CONCUR_READ_ONLY))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  @Ignore("until unsupported characteristics are rejected")
  public void testCreateStatement_overload2_unsupportedConcurrency_throws() {
    assertThatThrownBy(() -> getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_UPDATABLE))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }


  //////////
  // prepareStatement(String, int, int, int):

  @Test
  public void testPrepareStatement_overload2_supportedCase_returns() throws SQLException {
    getConnection().prepareStatement( "VALUES 1",
                                 ResultSet.TYPE_FORWARD_ONLY,
                                 ResultSet.CONCUR_READ_ONLY );
  }

  @Test
  @Ignore("until unsupported characteristics are rejected")
  public void testPrepareStatement_overload2_unsupportedType1_throws() {
    assertThatThrownBy(() -> getConnection().prepareStatement("VALUES 1",
      ResultSet.TYPE_SCROLL_INSENSITIVE,
      ResultSet.CONCUR_READ_ONLY))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  @Ignore("until unsupported characteristics are rejected")
  public void testPrepareStatement_overload2_unsupportedType2_throws() {
    assertThatThrownBy(() -> getConnection().prepareStatement("VALUES 1",
      ResultSet.TYPE_SCROLL_SENSITIVE,
      ResultSet.CONCUR_READ_ONLY)).isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  @Ignore("until unsupported characteristics are rejected")
  public void testPrepareStatement_overload2_unsupportedConcurrency_throws() {
    assertThatThrownBy(() -> getConnection().prepareStatement("VALUES 1",
      ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_UPDATABLE))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }


  //////////
  // createStatement(int, int, int) (case not covered with
  // createStatement(int, int)):


  @Test
  @Ignore( "until unsupported characteristics are rejected" )
  public void testCreateStatement_overload3_unsupportedHoldability_throws() {
    assertThatThrownBy(() -> getConnection().createStatement( ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_READ_ONLY,
                                ResultSet.CLOSE_CURSORS_AT_COMMIT))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }


  //////////
  // prepareStatement(int, int, int) (case not covered with
  // prepareStatement(int, int)):

  @Test
  @Ignore( "until unsupported characteristics are rejected" )
  public void testPrepareStatement_overload3_unsupportedHoldability_throws() {
    assertThatThrownBy(() -> getConnection().prepareStatement( "VALUES 1",
                                 ResultSet.TYPE_FORWARD_ONLY,
                                 ResultSet.CONCUR_READ_ONLY,
                                 ResultSet.CLOSE_CURSORS_AT_COMMIT ))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  //////////
  // prepareCall(String, int, int, int):

  @Test
  public void testCreateCall_overload3_throws() {
    assertThatThrownBy(() -> getConnection().prepareCall("VALUES 1",
      ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY,
      ResultSet.HOLD_CURSORS_OVER_COMMIT))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  //////////
  // remaining prepareStatement(...):

  @Test
  public void testPrepareStatement_overload4_throws() {
    assertThatThrownBy(
      () -> getConnection().prepareStatement("VALUES 1", Statement.RETURN_GENERATED_KEYS))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  public void testPrepareStatement_overload5_throws() {
    assertThatThrownBy(
      () -> getConnection().prepareStatement("VALUES 1", new int[]{1}))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  public void testPrepareStatement_overload6_throws() {
    assertThatThrownBy(
      () -> getConnection().prepareStatement("VALUES 1 AS colA", new String[]{"colA"}))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }


  ////////////////////////////////////////
  // Network timeout methods:

  //////////
  // getNetworkTimeout():

  /** Tests that getNetworkTimeout() indicates no timeout set. */
  @Test
  public void testGetNetworkTimeoutSaysNoTimeout() throws SQLException {
    assertThat(getConnection().getNetworkTimeout()).isEqualTo(0);
  }

  //////////
  // setNetworkTimeout(...):

  /** Tests that setNetworkTimeout(...) accepts (redundantly) setting to
   *  no-timeout mode. */
  @Test
  public void testSetNetworkTimeoutAcceptsNotimeoutRequest() throws SQLException {
    getConnection().setNetworkTimeout(executor, 0);
  }

  /** Tests that setNetworkTimeout(...) rejects setting a timeout. */
  @Test
  public void testSetNetworkTimeoutRejectsTimeoutRequest() {
    assertThatThrownBy(() -> getConnection().setNetworkTimeout(executor, 1_000))
      .isInstanceOf(SQLFeatureNotSupportedException.class)
      .satisfiesAnyOf(t -> assertThat(t.getMessage()).contains("Timeout"),
        t -> assertThat(t.getMessage()).contains("timeout"));
  }

  /** Tests that setNetworkTimeout(...) rejects setting a timeout (different
   *  value). */
  @Test
  public void testSetNetworkTimeoutRejectsTimeoutRequest2() {
    assertThatThrownBy(() -> getConnection().setNetworkTimeout(executor, Integer.MAX_VALUE))
      .isInstanceOf(SQLFeatureNotSupportedException.class);
  }

  @Test
  public void testSetNetworkTimeoutRejectsBadTimeoutValue() {
    assertThatThrownBy(() -> getConnection().setNetworkTimeout(executor, -1))
      .isInstanceOf(InvalidParameterSqlException.class)
      .satisfiesAnyOf(t -> assertThat(t.getMessage()).contains("Timeout"),
        t -> assertThat(t.getMessage()).contains("timeout"),
        t -> assertThat(t.getMessage()).contains("milliseconds"));
  }

  @Test
  public void testSetNetworkTimeoutRejectsBadExecutorValue() {
    assertThatThrownBy(() -> getConnection().setNetworkTimeout(null, 1))
      .isInstanceOf(InvalidParameterSqlException.class)
      .satisfiesAnyOf(t -> assertThat(t.getMessage()).contains("executor"),
        t -> assertThat(t.getMessage()).contains("Executor"));
  }
}
