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

import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.config.DremioConfig;
import com.dremio.jdbc.JdbcWithServerTestBase;
import com.dremio.test.TemporarySystemProperties;


public class Drill2461IntervalsBreakInfoSchemaBugTest extends JdbcWithServerTestBase {
  private static final String VIEW_NAME =
      Drill2461IntervalsBreakInfoSchemaBugTest.class.getSimpleName() + "_View";

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void before() {
    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
  }

  @Test
  public void testIntervalInViewDoesntCrashInfoSchema() throws Exception {
    final Statement stmt = getConnection().createStatement();
    ResultSet util;

    // Create a view using an INTERVAL type:
    util = stmt.executeQuery( "USE dfs_test" );
    assertTrue( util.next() );
    assertTrue( "Error setting schema to dfs_test: " + util.getString( 2 ), util.getBoolean( 1 ) );
    util = stmt.executeQuery(
        "CREATE OR REPLACE VIEW " + VIEW_NAME + " AS "
      + "\n  SELECT CAST( NULL AS INTERVAL HOUR(4) TO MINUTE ) AS optINTERVAL_HM "
      + "\n  FROM INFORMATION_SCHEMA.CATALOGS "
      + "\n  LIMIT 1 " );
    assertTrue( util.next() );
    assertTrue( "Error creating temporary test-columns view " + VIEW_NAME + ": "
          + util.getString( 2 ), util.getBoolean( 1 ) );

    // Test whether query INFORMATION_SCHEMA.COLUMNS works (doesn't crash):
    util = stmt.executeQuery( "SELECT * FROM INFORMATION_SCHEMA.COLUMNS" );
    assertTrue( util.next() );

    // Clean up the test view:
    util = getConnection().createStatement().executeQuery( "DROP VIEW " + VIEW_NAME );
    assertTrue( util.next() );
    assertTrue( "Error dropping temporary test-columns view " + VIEW_NAME + ": "
         + util.getString( 2 ), util.getBoolean( 1 ) );
  }
}
