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

import static com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl.ANONYMOUS;

import com.dremio.common.utils.SqlUtils;
import com.dremio.jdbc.test.JdbcAssert;
import com.dremio.options.OptionValidator;
import com.dremio.options.TypeValidators.BooleanValidator;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Subclass of {@code JdbcTestBase} which starts a Sabot node to test against. */
public class JdbcWithServerTestBase extends JdbcTestBase {

  private static Connection connection;

  public static Connection getConnection() {
    return connection;
  }

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    setupConnection(JdbcAssert.getDefaultProperties());
  }

  protected static void setupConnection(Properties properties) throws SQLException {
    Driver.load();
    properties.put("user", ANONYMOUS);
    connection = DriverManager.getConnection(getJDBCURL(), properties);
  }

  public static AutoCloseable withOption(final BooleanValidator validator, boolean value)
      throws Exception {
    return withOptionInternal(validator, value);
  }

  private static AutoCloseable withOptionInternal(final OptionValidator validator, Object value)
      throws Exception {
    final String optionScope = "SESSION";
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(
          String.format(
              "ALTER %s SET %s%s%s = %s",
              optionScope, SqlUtils.QUOTE, validator.getOptionName(), SqlUtils.QUOTE, value));
    }
    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        try (Statement stmt = connection.createStatement()) {
          stmt.execute(
              String.format(
                  "ALTER %s RESET %s%s%s",
                  optionScope, SqlUtils.QUOTE, validator.getOptionName(), SqlUtils.QUOTE));
        }
      }
    };
  }

  @AfterClass
  public static void tearDownConnection() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }
}
