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
package com.dremio.jdbc.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.dremio.exec.ExecTest;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.jdbc.SabotNodeRule;

/**
 * Test of TracingProxyDriver other than loading of driver classes.
 */
public class TracingProxyDriverTest extends ExecTest {
  @ClassRule
  public static final SabotNodeRule sabotNode = new SabotNodeRule();

  private static Driver proxyDriver;
  private static Connection proxyConnection;

  @BeforeClass
  public static void setUpTestCase() throws SQLException,
                                            ClassNotFoundException {
    Class.forName( "com.dremio.jdbc.proxy.TracingProxyDriver" );
    proxyDriver =
        DriverManager.getDriver(
            "jdbc:proxy:com.dremio.jdbc.Driver:" + sabotNode.getJDBCConnectionString() );
    proxyConnection =
        DriverManager.getConnection( "jdbc:proxy::" + sabotNode.getJDBCConnectionString(), UserServiceTestImpl.ANONYMOUS, "");
  }

  @Test
  public void testBasicProxying() throws SQLException {
    try ( final Statement stmt = proxyConnection.createStatement() ) {
      final ResultSet rs =
          stmt.executeQuery( "SELECT * FROM INFORMATION_SCHEMA.CATALOGS" );
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString(1)).isEqualTo("DREMIO");
      assertThat(rs.getObject(1)).isEqualTo((Object) "DREMIO");
    }
  }

  private static class StdErrCapturer {
    private final PrintStream savedStdErr;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final PrintStream capturingStream = new PrintStream( buffer );
    private boolean redirected;

    StdErrCapturer() {
      savedStdErr = System.err;
    }

    void redirect() {
      assertThat(redirected).isFalse();
      redirected = true;
      System.setErr(capturingStream);
    }

    void unredirect() {
      assertThat(redirected).isTrue();
      redirected = false;
      System.setErr(savedStdErr);
    }

    String getOutput() {
      assertThat(redirected).isFalse();
      return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void testBasicReturnTrace() throws SQLException {
    final StdErrCapturer nameThis = new StdErrCapturer();

    try {
      nameThis.redirect();
      proxyConnection.isClosed();
    }
    finally {
      nameThis.unredirect();
    }

    // Check captured System.err:

    final String output = nameThis.getOutput();
    final String[] lines = output.split( "\n" );
    assertThat(lines).hasSize(2);
    final String callLine = lines[ 0 ];
    final String returnLine = lines[ 1 ];

    // Expect something like current:
    // TRACER: CALL:   ((Connection) <id=3> ...) . isClosed()
    // TRACER: RETURN: ((Connection) <id=3> ...) . isClosed(), RESULT: (boolean) false

    assertThat(callLine).contains(" CALL:");
    assertThat(returnLine).contains(" RETURN:");
    assertThat(callLine).contains("(Connection)");
    assertThat(returnLine).contains("(Connection)");
    assertThat(callLine).contains("isClosed()");
    assertThat(returnLine).contains("isClosed()");
    assertThat(callLine).doesNotContain(" (boolean) ");
    assertThat(returnLine).contains(" (boolean) ");
    assertThat(callLine).doesNotContain("false");
    assertThat(returnLine).contains("false");
  }

  @Test
  public void testBasicThrowTrace() throws SQLException {
    final StdErrCapturer stdErrCapturer = new StdErrCapturer();

    final Statement statement = proxyConnection.createStatement();
    statement.close();

    try {
      stdErrCapturer.redirect();
      statement.execute( "" );
    }
    catch ( final SQLException e ) {
      // "already closed" is expected
    }
    finally {
      stdErrCapturer.unredirect();
    }

    // Check captured System.err:

    final String output = stdErrCapturer.getOutput();
    final String[] lines = output.split( "\n" );
    assertThat(lines).hasSize(2);
    final String callLine = lines[ 0 ];
    final String returnLine = lines[ 1 ];

    // Expect something like current:
    // TRACER: CALL:   ((Statement) <id=6> ...) . execute( (String) "" )
    // TRACER: THROW:  ((Statement) <id=6> ...) . execute( (String) "" ), th\
    // rew: (com.dremio.jdbc.AlreadyClosedSqlException) org.apache.dri\
    // ll.jdbc.AlreadyClosedSqlException: Statement is already closed.

    assertThat(callLine).contains(" CALL:");
    assertThat(returnLine).contains(" THROW:");
    assertThat(callLine).contains("(Statement)");
    assertThat(returnLine).contains("(Statement)");
    assertThat(callLine).contains("execute(");
    assertThat(returnLine).contains("execute(");
    assertThat(callLine).doesNotContain("threw:");
    assertThat(returnLine).contains("threw:");
    assertThat(callLine).doesNotContain("xception");
    assertThat(returnLine).contains("xception");
    assertThat(callLine).doesNotContain("losed");
    assertThat(returnLine).contains("losed");
  }

  // TODO:  Clean up these assorted remnants; probably move into separate test
  // methods.
  @Test
  public void testUnsortedMethods() throws SQLException {

    // Exercise these, even though we don't check results.
    proxyDriver.getMajorVersion();
    proxyDriver.getMinorVersion();
    proxyDriver.jdbcCompliant();
    proxyDriver.getParentLogger();
    proxyDriver.getPropertyInfo( "jdbc:proxy::" + sabotNode.getJDBCConnectionString(), new Properties() );

    final DatabaseMetaData dbMetaData = proxyConnection.getMetaData();
    assertThat(dbMetaData).isNotNull().isInstanceOf(DatabaseMetaData.class);

    assertThat(dbMetaData.getConnection()).isSameAs(proxyConnection);

    dbMetaData.allTablesAreSelectable();
    assertThatThrownBy(() -> dbMetaData.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY))
      .satisfiesAnyOf(t -> assertThat(t).isInstanceOf(SQLException.class),
        t -> assertThat(t).isInstanceOf(RuntimeException.class));

    final ResultSet catalogsResultSet = dbMetaData.getCatalogs();
    assertThat(catalogsResultSet).isNotNull().isInstanceOf(ResultSet.class);

    catalogsResultSet.next();
    catalogsResultSet.getString(1);
    catalogsResultSet.getObject(1);

    final ResultSetMetaData rsMetaData = catalogsResultSet.getMetaData();
    assertThat(rsMetaData).isNotNull().isInstanceOf(ResultSetMetaData.class);

    int colCount = rsMetaData.getColumnCount();
    for (int cx = 1; cx <= colCount; cx++) {
      catalogsResultSet.getObject(cx);
      catalogsResultSet.getString(cx);
      int finalcx = cx;
      assertThatThrownBy(() -> catalogsResultSet.getInt(finalcx))
        .isInstanceOf(SQLException.class);
    }

    assertThat(proxyConnection.getMetaData()).isSameAs(dbMetaData);
    assertThat(catalogsResultSet.getMetaData()).isSameAs(rsMetaData);
  }
} // class ProxyDriverTest
