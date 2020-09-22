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
import static org.junit.Assert.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.work.foreman.AttemptManager;


/**
 * Test for Dremio's implementation of Statement's methods (most).
 */
public class StatementTest extends JdbcWithServerTestBase {
  private static final String SYS_VERSION_SQL = "select * from sys.version";
  ////////////////////////////////////////
  // Query timeout methods:

  //////////
  // getQueryTimeout():

  /** Tests that getQueryTimeout() indicates no timeout set. */
  @Test
  public void testGetQueryTimeoutSaysNoTimeout() throws SQLException {
    try(Statement statement = getConnection().createStatement()) {
      assertThat( statement.getQueryTimeout(), equalTo( 0 ) );
    }
  }

  //////////
  // setQueryTimeout(...):

  /** Tests that setQueryTimeout(...) accepts (redundantly) setting to
   *  no-timeout mode. */
  @Test
  public void testSetQueryTimeoutAcceptsNotimeoutRequest() throws SQLException {
    try(Statement statement = getConnection().createStatement()) {
      statement.setQueryTimeout( 0 );
    }
  }


  @Test
  public void testSetQueryTimeoutRejectsBadTimeoutValue() throws SQLException {
    try(Statement statement = getConnection().createStatement()) {
      statement.setQueryTimeout( -2 );
    }
    catch ( SQLException e ) {
      // Check exception for some mention of parameter name or semantics:
      assertThat( e.getMessage(), anyOf( containsString( "seconds" ),
                                         containsString( "timeout" ),
                                         containsString( "Timeout" ) ) );
    }
  }

  /**
   * Test setting a valid timeout
   */
  @Test
  public void testValidSetQueryTimeout() throws SQLException {
    try(Statement statement = getConnection().createStatement()) {
      // Setting positive value
      statement.setQueryTimeout(1_000);
      assertThat( statement.getQueryTimeout(), equalTo( 1_000 ) );
    };
  }

  /**
   * Test setting timeout for a query that actually times out
   */
  @Test ( expected = SqlTimeoutException.class )
  public void testTriggeredQueryTimeout() throws SQLException {
    // Prevent the server to complete the query to trigger a timeout
    final String controls = Controls.newBuilder()
      .addPause(AttemptManager.class, "foreman-cleanup", 0)
      .build();

    try(Statement statement = getConnection().createStatement()) {
      assertThat(
          statement.execute(String.format(
              "ALTER session SET \"%s\" = '%s'",
              ExecConstants.NODE_CONTROL_INJECTIONS,
              controls)),
          equalTo(true));
    }
    String queryId = null;
    try(Statement statement = getConnection().createStatement()) {
      int timeoutDuration = 3;
      //Setting to a very low value (3sec)
      statement.setQueryTimeout(timeoutDuration);
      ResultSet rs = statement.executeQuery(SYS_VERSION_SQL);
      queryId = ((DremioResultSet) rs).getQueryId();
      //Fetch rows
      while (rs.next()) {
        rs.getBytes(1);
      }
    } catch (SQLException sqlEx) {
      if (sqlEx instanceof SqlTimeoutException) {
        throw (SqlTimeoutException) sqlEx;
      }
    } finally {
      // Do not forget to unpause to avoid memory leak.
      if (queryId != null) {
        DremioClient client = ((DremioConnection) getConnection()).getClient();
        client.resumeQuery(QueryIdHelper.getQueryIdFromString(queryId));
      }
    }
  }

}
