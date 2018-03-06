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
package com.dremio.jdbc;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.jdbc.test.JdbcAssert;

/**
 * Test that prepared statements works even if not supported on server, to some extent.
 */
public class LegacyPreparedStatementTest extends JdbcWithServerTestBase {
  /** Fuzzy matcher for parameters-not-supported message assertions.  (Based on
   *  current "Prepared-statement dynamic parameters are not supported.") */
  private static final Matcher<String> PARAMETERS_NOT_SUPPORTED_MSG_MATCHER =
      allOf( containsString( "arameter" ),   // allows "Parameter"
             containsString( "not" ),        // (could have false matches)
             containsString( "support" ) );  // allows "supported"

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    Properties properties = JdbcAssert.getDefaultProperties();
    properties.setProperty("server.preparedstatement.disabled", "true");

    setupConnection(properties);
    assertTrue(((DremioConnection) getConnection()).getConfig().isServerPreparedStatementDisabled());
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

  //////////
  // Parameters-not-implemented tests:

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

}
