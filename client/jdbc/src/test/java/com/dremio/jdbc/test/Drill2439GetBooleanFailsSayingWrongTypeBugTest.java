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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.jdbc.JdbcWithServerTestBase;


public class Drill2439GetBooleanFailsSayingWrongTypeBugTest extends JdbcWithServerTestBase {
  private static Statement statement;

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    JdbcWithServerTestBase.setUpConnection();
    statement = getConnection().createStatement();
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    statement.close();
    JdbcWithServerTestBase.tearDownConnection();
  }

  @Test
  public void testGetBooleanGetsTrue() throws Exception {
    ResultSet rs =
        statement.executeQuery( "SELECT TRUE FROM INFORMATION_SCHEMA.CATALOGS" );
    rs.next();
    assertThat( "getBoolean(...) for TRUE", rs.getBoolean( 1 ), equalTo( true ) );
    assertThat( "wasNull", rs.wasNull(), equalTo( false ) );
  }

  @Test
  public void testGetBooleanGetsFalse() throws Exception {
    ResultSet rs =
        statement.executeQuery( "SELECT FALSE FROM INFORMATION_SCHEMA.CATALOGS" );
    rs.next();
    assertThat( "getBoolean(...) for FALSE", rs.getBoolean( 1 ), equalTo( false ) );
    assertThat( "wasNull", rs.wasNull(), equalTo( false ) );
  }

  @Test
  public void testGetBooleanGetsNull() throws Exception {
    ResultSet rs = statement.executeQuery(
        "SELECT CAST( NULL AS BOOLEAN ) FROM INFORMATION_SCHEMA.CATALOGS" );
    rs.next();
    assertThat( "getBoolean(...) for BOOLEAN NULL", rs.getBoolean( 1 ), equalTo( false ) );
    assertThat( "wasNull", rs.wasNull(), equalTo( true ) );
  }

}
