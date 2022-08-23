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

import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.ANONYMOUS;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.config.DremioConfig;
import com.dremio.jdbc.Driver;
import com.dremio.jdbc.JdbcTestBase;
import com.dremio.jdbc.SabotNodeRule;
import com.dremio.test.TemporarySystemProperties;

public class JdbcQuotingProperityTest extends JdbcTestBase {

  @ClassRule
  public static final SabotNodeRule sabotNode = new SabotNodeRule();

  private static final String VIEW_NAME =
    JdbcQuotingProperityTest.class.getSimpleName() + "_View";

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void before() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @Test
  public void testDoubleQuote() throws Exception {
    final Properties properties = JdbcAssert.getDefaultProperties();
    properties.put("user", ANONYMOUS);
    properties.put("quoting", "DOUBLE_QUOTE");

    Driver.load();
    final Connection connection = DriverManager.getConnection(sabotNode.getJDBCConnectionString(), properties);

    final Statement stmt = connection.createStatement();
    ResultSet util;

    // Test query PDS
    util = stmt.executeQuery("select * from \"cp\".\"employee.json\"");
    assertTrue( util.next() );

    // Create a view
    util = stmt.executeQuery(
      "CREATE OR REPLACE VIEW \"dfs_test\".\"" + VIEW_NAME + "\" AS "
        + "\n  SELECT * from \"cp\".\"employee.json\"");
    assertTrue( util.next() );
    assertTrue( "Error creating temporary view " + VIEW_NAME + ": "
      + util.getString( 2 ), util.getBoolean( 1 ) );

    // Test query VDS
    util = stmt.executeQuery("select * from \"dfs_test\".\"" + VIEW_NAME + "\"");
    assertTrue( util.next() );

    // Clean up the test view:
    util = stmt.executeQuery( "DROP VIEW \"dfs_test\".\"" + VIEW_NAME + "\"");
    assertTrue( util.next() );
    assertTrue( "Error dropping temporary view " + VIEW_NAME + ": "
      + util.getString( 2 ), util.getBoolean( 1 ) );

    connection.close();
  }

  @Test
  public void testBackTick() throws Exception {
    final Properties properties = JdbcAssert.getDefaultProperties();
    properties.put("user", ANONYMOUS);
    properties.put("quoting", "BACK_TICK");

    Driver.load();
    final Connection connection = DriverManager.getConnection(sabotNode.getJDBCConnectionString(), properties);

    final Statement stmt = connection.createStatement();
    ResultSet util;

    // Test query PDS
    util = stmt.executeQuery("select * from `cp`.`employee.json`");
    assertTrue( util.next() );

    // Create a view
    util = stmt.executeQuery(
      "CREATE OR REPLACE VIEW `dfs_test`.`" + VIEW_NAME + "` AS "
        + "\n  SELECT * from `cp`.`employee.json`");
    assertTrue( util.next() );
    assertTrue( "Error creating temporary view " + VIEW_NAME + ": "
          + util.getString( 2 ), util.getBoolean( 1 ) );

    // Test query VDS
    // Note that when Dremio expands the view, the VDS query will be: SELECT * FROM "cp"."employee.json"
    // So default DOUBLE_QUOTE will be used.
    // The query will fail without the DX-52443 fix
    util = stmt.executeQuery("select * from `dfs_test`.`" + VIEW_NAME + "`");
    assertTrue( util.next() );

    // Clean up the test view:
    util = stmt.executeQuery( "DROP VIEW `dfs_test`.`" + VIEW_NAME + "`");
    assertTrue( util.next() );
    assertTrue( "Error dropping temporary view " + VIEW_NAME + ": "
         + util.getString( 2 ), util.getBoolean( 1 ) );

    connection.close();
  }

  @Test
  public void testBracket() throws Exception {
    final Properties properties = JdbcAssert.getDefaultProperties();
    properties.put("user", ANONYMOUS);
    properties.put("quoting", "BRACKET");

    Driver.load();
    final Connection connection = DriverManager.getConnection(sabotNode.getJDBCConnectionString(), properties);

    final Statement stmt = connection.createStatement();
    ResultSet util;

    // Test query PDS
    util = stmt.executeQuery("select * from [cp].[employee.json]");
    assertTrue( util.next() );

    // Create a view
    util = stmt.executeQuery(
      "CREATE OR REPLACE VIEW [dfs_test].[" + VIEW_NAME + "] AS "
        + "\n  SELECT * from [cp].[employee.json]");
    assertTrue( util.next() );
    assertTrue( "Error creating temporary view " + VIEW_NAME + ": "
      + util.getString( 2 ), util.getBoolean( 1 ) );

    // Test query VDS
    // Note that when Dremio expands the view, the VDS query will be: SELECT * FROM "cp"."employee.json"
    // So default DOUBLE_QUOTE will be used.
    // The query will fail without the DX-52443 fix.
    util = stmt.executeQuery("select * from [dfs_test].[" + VIEW_NAME + "]");
    assertTrue( util.next() );

    // Clean up the test view:
    util = stmt.executeQuery( "DROP VIEW [dfs_test].[" + VIEW_NAME + "]");
    assertTrue( util.next() );
    assertTrue( "Error dropping temporary view " + VIEW_NAME + ": "
      + util.getString( 2 ), util.getBoolean( 1 ) );

    connection.close();
  }
}
