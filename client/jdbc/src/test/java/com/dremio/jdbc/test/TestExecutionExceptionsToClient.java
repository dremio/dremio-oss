/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.jdbc.JdbcWithServerTestBase;


public class TestExecutionExceptionsToClient extends JdbcWithServerTestBase {
  @BeforeClass
  public static void setUpConnection() throws SQLException {
    JdbcWithServerTestBase.setUpConnection();
    try(Statement stmt = getConnection().createStatement()) {
      stmt.execute(String.format("alter session set `%s` = false", ExecConstants.ENABLE_REATTEMPTS.getOptionName()));
    }
  }

  @Test
  public void testExecuteQueryThrowsRight1() throws Exception {
    final Statement statement = getConnection().createStatement();
    try {
      statement.executeQuery( "SELECT one case of syntax error" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertTrue( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
                  hasType(e, ErrorType.SYSTEM) || hasType(e, ErrorType.PARSE) );
    }
  }

  private boolean hasType(SQLException e, ErrorType errorType) {
    UserRemoteException userException = ((UserRemoteException) e.getCause());
    return userException.getErrorType() == errorType;
  }

  @Test
  public void testExecuteThrowsRight1() throws Exception {
    final Statement statement = getConnection().createStatement();
    try {
      statement.execute( "SELECT one case of syntax error" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertTrue( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
              hasType(e, ErrorType.SYSTEM) || hasType(e, ErrorType.PARSE) );
    }
  }

  @Test
  public void testExecuteUpdateThrowsRight1() throws Exception {
    final Statement statement = getConnection().createStatement();
    try {
      statement.executeUpdate( "SELECT one case of syntax error" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertTrue( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
              hasType(e, ErrorType.SYSTEM) || hasType(e, ErrorType.PARSE) );
    }
  }

  @Test
  public void testExecuteQueryThrowsRight2() throws Exception {
    final Statement statement = getConnection().createStatement();
    try {
      statement.executeQuery( "BAD QUERY 1" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertTrue( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
              hasType(e, ErrorType.SYSTEM) || hasType(e, ErrorType.PARSE) );
    }
  }

  @Test
  public void testExecuteThrowsRight2() throws Exception {
    final Statement statement = getConnection().createStatement();
    try {
      statement.execute( "worse query 2" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertTrue( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
              hasType(e, ErrorType.SYSTEM) || hasType(e, ErrorType.PARSE) );
    }
  }

  @Test
  public void testExecuteUpdateThrowsRight2() throws Exception {
    final Statement statement = getConnection().createStatement();
    try {
      statement.executeUpdate( "naughty, naughty query 3" );
    }
    catch ( SQLException e ) {
      assertThat( "Null getCause(); missing expected wrapped exception",
                  e.getCause(), notNullValue() );

      assertThat( "Unexpectedly wrapped another SQLException",
                  e.getCause(), not( instanceOf( SQLException.class ) ) );

      assertThat( "getCause() not UserRemoteException as expected",
                  e.getCause(), instanceOf( UserRemoteException.class ) );

      assertTrue( "No expected current \"SYSTEM ERROR\"/eventual \"PARSE ERROR\"",
              hasType(e, ErrorType.SYSTEM) || hasType(e, ErrorType.PARSE) );
    }
  }

}
