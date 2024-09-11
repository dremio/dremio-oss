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

import com.dremio.jdbc.test.JdbcAssert;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test that prepared statements works even if not supported on server, to some extent. */
public class LegacyPreparedStatementTest extends JdbcWithServerTestBase {

  @BeforeClass
  public static void setUpConnection() throws SQLException {
    Properties properties = JdbcAssert.getDefaultProperties();
    properties.setProperty("server.preparedstatement.disabled", "true");

    setupConnection(properties);
    assertThat(((DremioConnection) getConnection()).getConfig().isServerPreparedStatementDisabled())
        .isTrue();
  }

  //////////
  // Basic querying-works test:

  /** Tests that basic executeQuery() (with query statement) works. */
  @Test
  public void testExecuteQueryBasicCaseWorks() throws SQLException {
    try (PreparedStatement stmt = getConnection().prepareStatement("VALUES 11")) {
      try (ResultSet rs = stmt.executeQuery()) {
        assertThat(rs.getMetaData().getColumnCount()).isEqualTo(1);
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(11);
        assertThat(rs.next()).isFalse();
      }
    }
  }
}
