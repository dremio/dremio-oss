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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.jdbc.test.JdbcAssert;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Properties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test class for the parameterized prepared statement for the legacy Jdbc driver. */
public class TestPreparedStatementOnLegacyJDBCDriver extends JdbcWithServerTestBase {

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    Properties properties = JdbcAssert.getDefaultProperties();
    setupConnection(properties);
  }

  @Test
  public void testPreparedStatementWithoutParameters() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from (values (1), (2), (null)) as a(id)")) {
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.next()).isTrue();
          assertThat(rs.next()).isTrue();
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testIntParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values (1), (2), (null)) as a(id) where id=?")) {
        stmt.setInt(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getInt(1)).isEqualTo(1);
          assertThat(rs.next()).isFalse();
        }
        stmt.setShort(1, (short) 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getInt(1)).isEqualTo(1);
          assertThat(rs.next()).isFalse();
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testCharParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values ('1'), null) as a(id) where id=?")) {
        stmt.setString(1, "1");
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString(1)).isEqualTo("1");
          assertThat(rs.next()).isFalse();
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testStringParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values ('1'), ('2'), (null)) as a(id) where id=?")) {
        stmt.setString(1, "1");
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString(1)).isEqualTo("1");
          assertThat(rs.next()).isFalse();
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testDateParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values (DATE '2024-02-20'), (null)) as a(id) where id=?")) {
        Date date = Date.valueOf(LocalDate.of(2024, 02, 20));
        stmt.setDate(1, date);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getDate(1)).isEqualTo(date);
          assertThat(rs.next()).isFalse();
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testDecimalParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where d = ?")) {
        stmt.setShort(1, (short) 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getShort(4)).isEqualTo((short) 1);
        }
        stmt.setInt(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getInt(4)).isEqualTo(1);
        }
        stmt.setLong(1, 1L);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getLong(4)).isEqualTo(1L);
        }
        stmt.setBigDecimal(1, new BigDecimal(1));
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getBigDecimal(4)).isEqualTo(new BigDecimal(1));
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testDoubleParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where ee = ?")) {
        stmt.setDouble(1, 1.5);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getDouble(5)).isEqualTo(1.5);
        }
        stmt.setFloat(1, 1.5f);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getFloat(5)).isEqualTo(1.5f);
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testFloatParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where f = ?")) {
        stmt.setFloat(1, 1.6f);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getFloat(6)).isEqualTo(1.6f);
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testVarcharParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values ('asdf'), (null)) as a(id) where id=?")) {
        stmt.setString(1, "asdf");
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString(1)).isEqualTo("asdf");
          assertThat(rs.next()).isFalse();
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testBooleanParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values (true), (null)) as a(id) where id=?")) {
        stmt.setBoolean(1, true);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getBoolean(1)).isEqualTo(true);
          assertThat(rs.next()).isFalse();
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testBigIntParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where i = ?")) {
        stmt.setInt(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getInt(9)).isEqualTo(1);
        }
        stmt.setShort(1, (short) 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getShort(9)).isEqualTo((short) 1);
        }
        stmt.setLong(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getLong(9)).isEqualTo(1L);
        }
        stmt.setBigDecimal(1, new BigDecimal(1));
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getBigDecimal(9)).isEqualTo(new BigDecimal(1));
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testTimeParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values (TIME '12:34:56'), (null)) as a(id) where id=?")) {
        Time time = Time.valueOf(LocalTime.of(12, 34, 56));
        stmt.setTime(1, time);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getTime(1).getTime()).isEqualTo(time.getTime());
          assertThat(rs.next()).isFalse();
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testTimeStampParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values (TIMESTAMP '2024-02-20 12:34:56.000025'), (null)) as a(id) where id=?")) {
        Timestamp timeStamp = Timestamp.valueOf(LocalDateTime.of(2024, 02, 20, 12, 34, 56, 10));
        stmt.setTimestamp(1, timeStamp);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
          assertThat(rs.next()).isTrue();
          assertThat(rs.getTimestamp(1).getTime()).isEqualTo(timeStamp.getTime());
          assertThat(rs.next()).isFalse();
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testNumericParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where n = ?")) {
        stmt.setInt(1, 32);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getInt(12)).isEqualTo(32);
        }
        stmt.setShort(1, (short) 32);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getShort(12)).isEqualTo((short) 32);
        }
        stmt.setLong(1, 32);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getLong(12)).isEqualTo(32L);
        }
        stmt.setBigDecimal(1, new BigDecimal(32));
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getBigDecimal(12)).isEqualTo(new BigDecimal(32));
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testVarBinaryParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values (CAST('Hello, Dremio!' AS VARBINARY)), (null)) as a(id) where id=?")) {
        stmt.setBytes(1, "Hello, Dremio!".getBytes());
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getBytes(1)).isEqualTo("Hello, Dremio!".getBytes());
          assertThat(rs.next()).isFalse();
        }
        stmt.setNull(1, 1);
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testIntParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values (1), (2), (null)) as a(id) where id=?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setBoolean(1, false);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testStringParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values ('1'), ('2'), (null)) as a(id) where id=?")) {
        try {
          stmt.setInt(1, 1);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setBoolean(1, false);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testDateParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values (DATE '2024-02-20'), (null)) as a(id) where id=?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setInt(1, 1);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testDecimalParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where d = ?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setDouble(1, 1.5);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testDoubleParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where ee = ?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setInt(1, 1);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testFloatParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where f = ?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setShort(1, (short) 1);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testVarcharParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values ('asdf'), (null)) as a(id) where id=?")) {
        try {
          stmt.setBoolean(1, false);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setDouble(1, 1.5);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testBooleanParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values (true), (null)) as a(id) where id=?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setDouble(1, 1.5);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testBigIntParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where i = ?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setDouble(1, 1.5);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testTimeParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values (TIME '12:34:56'), (null)) as a(id) where id=?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setDouble(1, 1.5);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testTimeStampParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values (TIMESTAMP '2024-02-20 12:34:56'), (null)) as a(id) where id=?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setDouble(1, 1.5);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testNumericParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where n = ?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setDouble(1, 1.5);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testVarBinaryParameterNegative() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement(
                  "select * from (values (CAST('Hello, Dremio!' AS VARBINARY)), (null)) as a(id) where id=?")) {
        try {
          stmt.setString(1, "1");
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
        try {
          stmt.setDouble(1, 1.5);
        } catch (UserException e) {
          Assert.assertTrue(
              e.getMessage().contains("Parameter value has not been set properly for the index"));
        }
      }
    }
  }

  @Test
  public void testSqlInjection() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where g = ?")) {
        stmt.setString(1, "asdf and a = 1");
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testSetParameterMultipleTimes() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection().prepareStatement("select * from cp.\"temp.parquet\" where g = ?")) {
        stmt.setString(1, "wert");
        stmt.setString(1, "asdf");
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isTrue();
        }
        stmt.setString(1, "asdf");
        stmt.setString(1, "wert");
        try (ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }
    }
  }

  @Test
  public void testParametersNotSet() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values (1), (null)) as a(id) where id=?")) {
        assertThatThrownBy(stmt::executeQuery)
            .isInstanceOf(UserException.class)
            .hasMessageContaining(
                "Not all the parameters are set for the given prepared statement.");
        stmt.setInt(1, 1);
        stmt.clearParameters();
        assertThatThrownBy(stmt::executeQuery)
            .isInstanceOf(UserException.class)
            .hasMessageContaining(
                "Not all the parameters are set for the given prepared statement.");
      }
    }
  }

  @Test
  public void testExecutingParameterizedPreparedStatementAfterDisablingDynamicParam()
      throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values (1), (null)) as a(id) where id=?")) {
        stmt.setInt(1, 1);
        try (AutoCloseable ignored1 =
            withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, false)) {
          assertThatThrownBy(stmt::executeQuery)
              .isInstanceOf(SQLException.class)
              .hasMessageContaining(
                  "Parameter values are provided despite parameterized prepared statements being disabled.");
        }
      }
    }
  }

  @Test
  public void testParametersWithDynamicParamDisabled() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, false)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values (1), (null)) as a(id) where id=?")) {
        stmt.setInt(1, 1);
      } catch (SQLException e) {
        assertThat(
                e.getMessage()
                    .contains(
                        "Cannot convert RexNode to equivalent Dremio expression. "
                            + "RexNode Class: org.apache.calcite.rex.RexDynamicParam, RexNode Digest: ?0"))
            .isTrue();
      }
    }
  }

  @Test
  public void testIsNullParameter() throws Exception {
    try (AutoCloseable ignored = withOption(PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE, true)) {
      try (PreparedStatement stmt =
          getConnection()
              .prepareStatement("select * from (values (1), (null)) as a(id) where id is ?")) {
        stmt.setNull(1, 1);
      } catch (SQLException se) {
        assertThat(se.getMessage()).contains("Encountered \\\"is ?\\\"");
      }
    }
  }
}
