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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.jdbc.JdbcWithServerTestBase;


public class Drill2463GetNullsFailedWithAssertionsBugTest extends JdbcWithServerTestBase {

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

  // Test primitive types vs. non-primitive types:

  @Test
  public void testGetPrimitiveTypeNullAsOwnType() throws Exception {
    final ResultSet rs = statement.executeQuery(
      "SELECT CAST( NULL AS INTEGER ) FROM INFORMATION_SCHEMA.CATALOGS");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt(1)).isEqualTo(0);
    assertThat(rs.wasNull()).isTrue();
  }

  @Test
  public void testGetPrimitiveTypeNullAsObject() throws Exception {
    final ResultSet rs = statement.executeQuery(
      "SELECT CAST( NULL AS INTEGER ) FROM INFORMATION_SCHEMA.CATALOGS");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getObject(1)).isNull();
    assertThat(rs.wasNull()).isTrue();
  }

  @Test
  public void testGetNonprimitiveTypeNullAsOwnType() throws Exception {
    final ResultSet rs = statement.executeQuery(
      "SELECT CAST( NULL AS VARCHAR ) FROM INFORMATION_SCHEMA.CATALOGS");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getString(1)).isNull();
    assertThat(rs.wasNull()).isTrue();
  }

  // Test a few specifics

  @Test
  public void testGetBooleanNullAsOwnType() throws Exception {
    final ResultSet rs = statement.executeQuery(
      "SELECT CAST( NULL AS BOOLEAN ) FROM INFORMATION_SCHEMA.CATALOGS");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getBoolean(1)).isEqualTo(false);
    assertThat(rs.wasNull()).isTrue();
  }

  @Test
  public void testGetBooleanNullAsObject() throws Exception {
    final ResultSet rs = statement.executeQuery(
      "SELECT CAST( NULL AS BOOLEAN ) FROM INFORMATION_SCHEMA.CATALOGS");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getObject(1)).isNull();
    assertThat(rs.wasNull()).isTrue();
  }

  @Test
  public void testGetIntegerNullAsOwnType() throws Exception {
    final ResultSet rs = statement.executeQuery(
      "SELECT CAST( NULL AS INTEGER ) FROM INFORMATION_SCHEMA.CATALOGS");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getInt(1)).isEqualTo(0);
    assertThat(rs.wasNull()).isTrue();
  }

  @Test
  public void testGetIntegerNullAsObject() throws Exception {
    final ResultSet rs = statement.executeQuery(
      "SELECT CAST( NULL AS INTEGER ) FROM INFORMATION_SCHEMA.CATALOGS");
    assertThat(rs.next()).isTrue();
    assertThat(rs.getObject(1)).isNull();
    assertThat(rs.wasNull()).isTrue();
  }
}
