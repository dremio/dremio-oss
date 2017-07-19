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
package com.dremio.jdbc;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test for Dremio's implementation of Statement's methods (most).
 */
public class StatementTest extends JdbcTestBase {

  private static Connection connection;
  private static Statement statement;

  @BeforeClass
  public static void setUpStatement() throws SQLException {
    // (Note: Can't use JdbcTest's connect(...) because JdbcTest closes
    // Connection--and other JDBC objects--on test method failure, but this test
    // class uses some objects across methods.)
    connection = new Driver().connect( "jdbc:dremio:zk=local", null );
    statement = connection.createStatement();
  }

  @AfterClass
  public static void tearDownStatement() throws SQLException {
    connection.close();
  }


  ////////////////////////////////////////
  // Query timeout methods:

  //////////
  // getQueryTimeout():

  /** Tests that getQueryTimeout() indicates no timeout set. */
  @Test
  public void testGetQueryTimeoutSaysNoTimeout() throws SQLException {
    assertThat( statement.getQueryTimeout(), equalTo( 0 ) );
  }

  //////////
  // setQueryTimeout(...):

  /** Tests that setQueryTimeout(...) accepts (redundantly) setting to
   *  no-timeout mode. */
  @Test
  public void testSetQueryTimeoutAcceptsNotimeoutRequest() throws SQLException {
    statement.setQueryTimeout( 0 );
  }

  /** Tests that setQueryTimeout(...) rejects setting a timeout. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testSetQueryTimeoutRejectsTimeoutRequest() throws SQLException {
    try {
      statement.setQueryTimeout( 1_000 );
    }
    catch ( SQLFeatureNotSupportedException e ) {
      // Check exception for some mention of query timeout:
      assertThat( e.getMessage(), anyOf( containsString( "Timeout" ),
                                         containsString( "timeout" ) ) );
      throw e;
    }
  }

  /** Tests that setQueryTimeout(...) rejects setting a timeout (different
   *  value). */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testSetQueryTimeoutRejectsTimeoutRequest2() throws SQLException {
    statement.setQueryTimeout( Integer.MAX_VALUE / 2 );
  }

  @Test( expected = InvalidParameterSqlException.class )
  public void testSetQueryTimeoutRejectsBadTimeoutValue() throws SQLException {
    try {
      statement.setQueryTimeout( -2 );
    }
    catch ( InvalidParameterSqlException e ) {
      // Check exception for some mention of parameter name or semantics:
      assertThat( e.getMessage(), anyOf( containsString( "milliseconds" ),
                                         containsString( "timeout" ),
                                         containsString( "Timeout" ) ) );
      throw e;
    }
  }

}
