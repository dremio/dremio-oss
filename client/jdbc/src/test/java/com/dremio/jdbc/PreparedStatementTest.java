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

import static java.sql.ResultSetMetaData.columnNullable;
import static java.sql.Types.BIGINT;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.VARCHAR;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.ischema.InfoSchemaConstants;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.work.foreman.AttemptManager;
import com.google.common.collect.ImmutableList;


/**
 * Test for Dremio's implementation of PreparedStatement's methods.
 */
public class PreparedStatementTest extends JdbcWithServerTestBase {
  private static final String SAMPLE_SQL = "select * from INFORMATION_SCHEMA.CATALOGS";

  /** Fuzzy matcher for parameters-not-supported message assertions.  (Based on
   *  current "Prepared-statement dynamic parameters are not supported.") */
  private static final Matcher<String> PARAMETERS_NOT_SUPPORTED_MSG_MATCHER =
      allOf( containsString( "arameter" ),   // allows "Parameter"
             containsString( "not" ),        // (could have false matches)
             containsString( "support" ) );  // allows "supported"

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    JdbcWithServerTestBase.setUpConnection();
    try(Statement stmt = getConnection().createStatement()) {
      stmt.execute(String.format("alter session set \"%s\" = true", PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY));
      stmt.execute(String.format("alter session set \"%s\" = false", ExecConstants.ENABLE_REATTEMPTS.getOptionName()));
    }
  }

  //////////
  // Basic querying-works test:

  /** Tests that basic executeQuery() (with query statement) works. */
  @Test
  public void testExecuteQueryBasicCaseWorks() throws SQLException {
    try (PreparedStatement stmt = getConnection().prepareStatement( "VALUES 11" )) {
      try(ResultSet rs = stmt.executeQuery()) {
        assertThat("Unexpected column count",
            rs.getMetaData().getColumnCount(), equalTo(1)
        );
        assertTrue("No expected first row", rs.next());
        assertThat(rs.getInt(1), equalTo(11));
        assertFalse("Unexpected second row", rs.next());
      }
    }
  }

  @Test
  public void testQueryMetadataInPreparedStatement() throws SQLException {
    try(PreparedStatement stmt = getConnection().prepareStatement(
        "SELECT " +
            "cast(1 as INTEGER ) as int_field, " +
            "cast(12384729 as BIGINT ) as bigint_field, " +
            "'varchar_value' as varchar_field, " +
            "timestamp '2008-2-23 10:00:20.123' as ts_field, " +
            "date '2008-2-23' as date_field, " +
            // DX-4852
            //"cast('99999912399.4567' as decimal(18, 5)) as decimal_field" +
            "cast('99999912399.4567' as double) as double_field" +
            " FROM INFORMATION_SCHEMA.CATALOGS")) {

      List<ExpectedColumnResult> exp = ImmutableList.of(
          new ExpectedColumnResult("int_field", INTEGER, columnNullable, 11, 0, 0, true, Integer.class.getName()),
          new ExpectedColumnResult("bigint_field", BIGINT, columnNullable, 20, 0, 0, true, Long.class.getName()),
          new ExpectedColumnResult("varchar_field", VARCHAR, columnNullable, 65536, 65536, 0, false, String.class.getName()),
          new ExpectedColumnResult("ts_field", TIMESTAMP, columnNullable, 23, 0, 0, false, Timestamp.class.getName()),
          new ExpectedColumnResult("date_field", DATE, columnNullable, 10, 0, 0, false, Date.class.getName()),
          // DX-4852
          //new ExpectedColumnResult("decimal_field", DECIMAL, columnNullable, 20, 18, 5, true, BigDecimal.class.getName())
          new ExpectedColumnResult("double_field", DOUBLE, columnNullable, 24, 0, 0, true, Double.class.getName())
      );

      ResultSetMetaData prepareMetadata = stmt.getMetaData();
      verifyMetadata(prepareMetadata, exp);

      try (ResultSet rs = stmt.executeQuery()) {
        ResultSetMetaData executeMetadata = rs.getMetaData();
        verifyMetadata(executeMetadata, exp);

        assertTrue("No expected first row", rs.next());
        assertThat(rs.getInt(1), equalTo(1));
        assertThat(rs.getLong(2), equalTo(12384729L));
        assertThat(rs.getString(3), equalTo("varchar_value"));
        assertThat(rs.getTimestamp(4), equalTo(Timestamp.valueOf("2008-2-23 10:00:20.123")));
        assertThat(rs.getDate(5), equalTo(Date.valueOf("2008-2-23")));
        //assertThat(rs.getBigDecimal(6), equalTo(new BigDecimal("99999912399.45670")));
        assertThat(rs.getDouble(6), equalTo(99999912399.4567D));
        assertFalse("Unexpected second row", rs.next());
      }
    }
  }

  private static void verifyMetadata(ResultSetMetaData act, List<ExpectedColumnResult> exp) throws SQLException {
    assertEquals(exp.size(), act.getColumnCount());
    int i = 0;
    for(ExpectedColumnResult e : exp) {
      ++i;
      assertTrue("Failed to find the expected column metadata. Expected " + e + ". Was: " + toString(act, i), e.isEqualsTo(act, i));
    }
  }

  private static String toString(ResultSetMetaData metadata, int colNum) throws SQLException {
    return "ResultSetMetaData(" + colNum + ")[" +
        "columnName='" + metadata.getColumnName(colNum) + '\'' +
        ", type='" + metadata.getColumnType(colNum) + '\'' +
        ", nullable=" + metadata.isNullable(colNum) +
        ", displaySize=" + metadata.getColumnDisplaySize(colNum) +
        ", precision=" + metadata.getPrecision(colNum) +
        ", scale=" + metadata.getScale(colNum) +
        ", signed=" + metadata.isSigned(colNum) +
        ", className='" + metadata.getColumnClassName(colNum) + '\'' +
        ']';
  }

  private static class ExpectedColumnResult {
    final String columnName;
    final int type;
    final int nullable;
    final int displaySize;
    final int precision;
    final int scale;
    final boolean signed;
    final String className;

    ExpectedColumnResult(String columnName, int type, int nullable, int displaySize, int precision,
        int scale, boolean signed, String className) {
      this.columnName = columnName;
      this.type = type;
      this.nullable = nullable;
      this.displaySize = displaySize;
      this.precision = precision;
      this.scale = scale;
      this.signed = signed;
      this.className = className;
    }

    boolean isEqualsTo(ResultSetMetaData metadata, int colNum) throws SQLException {
      return
          metadata.getCatalogName(colNum).equals(InfoSchemaConstants.IS_CATALOG_NAME) &&
          metadata.getSchemaName(colNum).isEmpty() &&
          metadata.getTableName(colNum).isEmpty() &&
          metadata.getColumnName(colNum).equals(columnName) &&
          metadata.getColumnLabel(colNum).equals(columnName) &&
          metadata.getColumnType(colNum) == type &&
          metadata.isNullable(colNum) == nullable &&
          // There is an existing bug where query results doesn't contain the precision for VARCHAR field.
          //metadata.getPrecision(colNum) == precision &&
          metadata.getScale(colNum) == scale &&
          metadata.isSigned(colNum) == signed &&
          metadata.getColumnDisplaySize(colNum) == displaySize &&
          metadata.getColumnClassName(colNum).equals(className) &&
          metadata.isSearchable(colNum) &&
          metadata.isAutoIncrement(colNum) == false &&
          metadata.isCaseSensitive(colNum) == false &&
          metadata.isReadOnly(colNum) &&
          metadata.isWritable(colNum) == false &&
          metadata.isDefinitelyWritable(colNum) == false &&
          metadata.isCurrency(colNum) == false;
    }

    @Override
    public String toString() {
      return "ExpectedColumnResult[" +
          "columnName='" + columnName + '\'' +
          ", type='" + type + '\'' +
          ", nullable=" + nullable +
          ", displaySize=" + displaySize +
          ", precision=" + precision +
          ", scale=" + scale +
          ", signed=" + signed +
          ", className='" + className + '\'' +
          ']';
    }
  }

  //////////
  // Parameters-not-implemented tests:

  /** Tests that basic case of trying to create a prepare statement with parameters. */
  @Test( expected = SQLException.class )
  public void testSqlQueryWithParamNotSupported() throws SQLException {

    try {
      getConnection().prepareStatement( "VALUES ?, ?" );
    }
    catch ( final SQLException e ) {
      assertThat(
          "Check whether params.-unsupported wording changed or checks changed.",
          e.toString(), containsString("Illegal use of dynamic parameter") );
      throw e;
    }
  }

  /** Tests that "not supported" has priority over possible "no parameters"
   *  check. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testParamSettingWhenNoParametersIndexSaysUnsupported() throws SQLException {
    try(PreparedStatement prepStmt = getConnection().prepareStatement( "VALUES 1" )) {
      try {
        prepStmt.setBytes(4, null);
      } catch (final SQLFeatureNotSupportedException e) {
        assertThat(
            "Check whether params.-unsupported wording changed or checks changed.",
            e.toString(), PARAMETERS_NOT_SUPPORTED_MSG_MATCHER
        );
        throw e;
      }
    }
  }

  /** Tests that "not supported" has priority over possible "type not supported"
   *  check. */
  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testParamSettingWhenUnsupportedTypeSaysUnsupported() throws SQLException {
    try(PreparedStatement prepStmt = getConnection().prepareStatement( "VALUES 1" )) {
      try {
        prepStmt.setClob(2, (Clob) null);
      } catch (final SQLFeatureNotSupportedException e) {
        assertThat(
            "Check whether params.-unsupported wording changed or checks changed.",
            e.toString(), PARAMETERS_NOT_SUPPORTED_MSG_MATCHER
        );
        throw e;
      }
    }
  }

////////////////////////////////////////
  // Query timeout methods:

  //////////
  // getQueryTimeout():

  /** Tests that getQueryTimeout() indicates no timeout set. */
  @Test
  public void testGetQueryTimeoutSaysNoTimeout() throws SQLException {
    try(PreparedStatement statement = getConnection().prepareStatement(SAMPLE_SQL)) {
      assertThat( statement.getQueryTimeout(), equalTo( 0 ) );
    }
  }

  //////////
  // setQueryTimeout(...):

  /** Tests that setQueryTimeout(...) accepts (redundantly) setting to
   *  no-timeout mode. */
  @Test
  public void testSetQueryTimeoutAcceptsNotimeoutRequest() throws SQLException {
    try(PreparedStatement statement = getConnection().prepareStatement(SAMPLE_SQL)) {
      statement.setQueryTimeout( 0 );
    }
  }


  @Test
  public void testSetQueryTimeoutRejectsBadTimeoutValue() throws SQLException {
    try(PreparedStatement statement = getConnection().prepareStatement(SAMPLE_SQL)) {
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
    try(PreparedStatement statement = getConnection().prepareStatement(SAMPLE_SQL)) {
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
    String queryId = null;
    try(PreparedStatement statement = getConnection().prepareStatement(SAMPLE_SQL)) {
      // Prevent the server to complete the query to trigger a timeout
      // For prepared statement, two queries are made: one to create the plan and one to
      // execute. We only pause the execution one.

      try(Statement controlStatement = getConnection().createStatement()) {
        final String controls = Controls.newBuilder()
            .addPause(AttemptManager.class, "foreman-cleanup", 0)
            .build();
        assertThat(
            controlStatement.execute(String.format(
                "ALTER session SET \"%s\" = '%s'",
                ExecConstants.NODE_CONTROL_INJECTIONS,
                controls)),
            equalTo(true));
      }
      int timeoutDuration = 3;
      //Setting to a very low value (3sec)
      statement.setQueryTimeout(timeoutDuration);
      ResultSet rs = statement.executeQuery();
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
