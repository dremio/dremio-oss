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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
  public static void setUpExecutor() throws SQLException {
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterClass
  public static void tearDownExecutor() throws SQLException {
    if (executor != null) {
      executor.shutdown();
    }
  }

  private static void emitSupportExceptionWarning() {
    System.err.println(
        "Note:  Still throwing older-Avatica UnsupportedOperationException"
        + " instead of less-noncompliant SQLFeatureNotSupportedException" );
  }

  @Test
  public void testGetCatalog() throws SQLException {
    assertEquals("DREMIO", getConnection().getCatalog());
  }

  ////////////////////////////////////////
  // Basic tests of statement creation methods (not necessarily executing
  // statements):

  //////////
  // Simplest cases of createStatement, prepareStatement, prepareCall:

  @Test
  public void testCreateStatementBasicCaseWorks() throws SQLException {
    Statement stmt = getConnection().createStatement();
    ResultSet rs = stmt.executeQuery( "VALUES 1" );
    assertTrue( rs.next() );
  }

  @Test
  public void testPrepareStatementBasicCaseWorks() throws SQLException {
    PreparedStatement stmt = getConnection().prepareStatement( "VALUES 1" );
    ResultSet rs = stmt.executeQuery();
    assertTrue( rs.next() );
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testPrepareCallThrows() throws SQLException {
    try {
      getConnection().prepareCall( "VALUES 1" );
    }
    catch ( UnsupportedOperationException e) {
      // TODO(DRILL-2769):  Purge this mapping when right exception is thrown.
      emitSupportExceptionWarning();
      throw new SQLFeatureNotSupportedException(
          "Note: Still throwing UnsupportedOperationException ", e );
    }
  }

  //////////
  // createStatement(int, int):

  @Test
  public void testCreateStatement_overload2_supportedCase_returns() throws SQLException {
      getConnection().createStatement( ResultSet.TYPE_FORWARD_ONLY,
                                  ResultSet.CONCUR_READ_ONLY );
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  @Ignore( "until unsupported characteristics are rejected" )
  public void testCreateStatement_overload2_unsupportedType1_throws() throws SQLException {
    getConnection().createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_READ_ONLY );
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  @Ignore( "until unsupported characteristics are rejected" )
  public void testCreateStatement_overload2_unsupportedType2_throws() throws SQLException {
    getConnection().createStatement( ResultSet.TYPE_SCROLL_SENSITIVE,
                                ResultSet.CONCUR_READ_ONLY);
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  @Ignore( "until unsupported characteristics are rejected" )
  public void testCreateStatement_overload2_unsupportedConcurrency_throws() throws SQLException {
    getConnection().createStatement( ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE );
  }


  //////////
  // prepareStatement(String, int, int, int):

  @Test
  public void testPrepareStatement_overload2_supportedCase_returns() throws SQLException {
    getConnection().prepareStatement( "VALUES 1",
                                 ResultSet.TYPE_FORWARD_ONLY,
                                 ResultSet.CONCUR_READ_ONLY );
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  @Ignore( "until unsupported characteristics are rejected" )
  public void testPrepareStatement_overload2_unsupportedType1_throws() throws SQLException {
    getConnection().prepareStatement( "VALUES 1",
                                 ResultSet.TYPE_SCROLL_INSENSITIVE,
                                 ResultSet.CONCUR_READ_ONLY );
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  @Ignore( "until unsupported characteristics are rejected" )
  public void testPrepareStatement_overload2_unsupportedType2_throws() throws SQLException {
    getConnection().prepareStatement( "VALUES 1",
                                 ResultSet.TYPE_SCROLL_SENSITIVE,
                                 ResultSet.CONCUR_READ_ONLY );
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  @Ignore( "until unsupported characteristics are rejected" )
  public void testPrepareStatement_overload2_unsupportedConcurrency_throws() throws SQLException {
    getConnection().prepareStatement( "VALUES 1",
                                 ResultSet.TYPE_FORWARD_ONLY,
                                 ResultSet.CONCUR_UPDATABLE );
  }


  //////////
  // createStatement(int, int, int) (case not covered with
  // createStatement(int, int)):


  @Test( expected = SQLFeatureNotSupportedException.class )
  @Ignore( "until unsupported characteristics are rejected" )
  public void testCreateStatement_overload3_unsupportedHoldability_throws() throws SQLException {
    getConnection().createStatement( ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_READ_ONLY,
                                ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }


  //////////
  // prepareStatement(int, int, int) (case not covered with
  // prepareStatement(int, int)):

  @Test( expected = SQLFeatureNotSupportedException.class )
  @Ignore( "until unsupported characteristics are rejected" )
  public void testPrepareStatement_overload3_unsupportedHoldability_throws() throws SQLException {
    getConnection().prepareStatement( "VALUES 1",
                                 ResultSet.TYPE_FORWARD_ONLY,
                                 ResultSet.CONCUR_READ_ONLY,
                                 ResultSet.CLOSE_CURSORS_AT_COMMIT );
  }

  //////////
  // prepareCall(String, int, int, int):

  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testCreateCall_overload3_throws() throws SQLException {
    try {
      getConnection().prepareCall( "VALUES 1",
                              ResultSet.TYPE_FORWARD_ONLY,
                              ResultSet.CONCUR_READ_ONLY,
                              ResultSet.HOLD_CURSORS_OVER_COMMIT );
    }
    catch ( UnsupportedOperationException e) {
      // TODO(DRILL-2769):  Purge this mapping when right exception is thrown.
      emitSupportExceptionWarning();
      throw new SQLFeatureNotSupportedException(
          "Note: Still throwing UnsupportedOperationException ", e );
    }
  }

  //////////
  // remaining prepareStatement(...):

  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testPrepareStatement_overload4_throws() throws SQLException {
    try {
      getConnection().prepareStatement( "VALUES 1", Statement.RETURN_GENERATED_KEYS );
    }
    catch ( UnsupportedOperationException e) {
      // TODO(DRILL-2769):  Purge this mapping when right exception is thrown.
      emitSupportExceptionWarning();
      throw new SQLFeatureNotSupportedException(
          "Note: Still throwing UnsupportedOperationException ", e );
    }
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testPrepareStatement_overload5_throws() throws SQLException {
    try {
      getConnection().prepareStatement( "VALUES 1", new int[] { 1 } );
    }
    catch ( UnsupportedOperationException e) {
      // TODO(DRILL-2769):  Purge this mapping when right exception is thrown.
      emitSupportExceptionWarning();
      throw new SQLFeatureNotSupportedException(
          "Note: Still throwing UnsupportedOperationException ", e );
    }
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testPrepareStatement_overload6_throws() throws SQLException {
    try {
       getConnection().prepareStatement( "VALUES 1 AS colA", new String[] { "colA" } );
    }
    catch ( UnsupportedOperationException e) {
      // TODO(DRILL-2769):  Purge this mapping when right exception is thrown.
      emitSupportExceptionWarning();
      throw new SQLFeatureNotSupportedException(
          "Note: Still throwing UnsupportedOperationException ", e );
    }
  }


  ////////////////////////////////////////
  // Network timeout methods:

  //////////
  // getNetworkTimeout():

  /** Tests that getNetworkTimeout() indicates no timeout set. */
  @Test
  public void testGetNetworkTimeoutSaysNoTimeout() throws SQLException {
    assertThat( getConnection().getNetworkTimeout(), equalTo( 0 ) );
  }

  //////////
  // setNetworkTimeout(...):

  /** Tests that setNetworkTimeout(...) accepts (redundantly) setting to
   *  no-timeout mode. */
  @Test
  public void testSetNetworkTimeoutAcceptsNotimeoutRequest() throws SQLException {
    getConnection().setNetworkTimeout( executor, 0 );
  }

  /** Tests that setNetworkTimeout(...) rejects setting a timeout. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testSetNetworkTimeoutRejectsTimeoutRequest() throws SQLException {
    try {
      getConnection().setNetworkTimeout( executor, 1_000 );
    }
    catch ( SQLFeatureNotSupportedException e ) {
      // Check exception for some mention of network timeout:
      assertThat( e.getMessage(), anyOf( containsString( "Timeout" ),
                                         containsString( "timeout" ) ) );
      throw e;
    }
  }

  /** Tests that setNetworkTimeout(...) rejects setting a timeout (different
   *  value). */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testSetNetworkTimeoutRejectsTimeoutRequest2() throws SQLException {
    getConnection().setNetworkTimeout( executor, Integer.MAX_VALUE );
  }

  @Test( expected = InvalidParameterSqlException.class )
  public void testSetNetworkTimeoutRejectsBadTimeoutValue() throws SQLException {
    try {
      getConnection().setNetworkTimeout( executor, -1 );
    }
    catch ( InvalidParameterSqlException e ) {
      // Check exception for some mention of parameter name or semantics:
      assertThat( e.getMessage(), anyOf( containsString( "milliseconds" ),
                                         containsString( "timeout" ),
                                         containsString( "Timeout" ) ) );
      throw e;
    }
  }

  @Test( expected = InvalidParameterSqlException.class )
  public void testSetNetworkTimeoutRejectsBadExecutorValue() throws SQLException {
    try {
      getConnection().setNetworkTimeout( null, 1 );
    }
    catch ( InvalidParameterSqlException e ) {
      // Check exception for some mention of parameter name or semantics:
      assertThat( e.getMessage(), anyOf( containsString( "executor" ),
                                         containsString( "Executor" ) ) );
      throw e;
    }
  }

}
