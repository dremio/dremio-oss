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

import com.google.common.base.Function;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class Bug1735ResultSetCloseReleasesBuffersTest extends JdbcTestQueryBase {

  // TODO:  Purge nextUntilEnd(...) and calls when remaining fragment race
  // conditions are fixed (not just DRILL-2245 fixes).
  /// **
  // * Calls {@link ResultSet#next} on given {@code ResultSet} until it returns
  // * false.  (For TEMPORARY workaround for query cancelation race condition.)
  // */
  // private void nextUntilEnd(final ResultSet resultSet) throws SQLException {
  //  while (resultSet.next()) {
  //  }
  // }

  @Test
  public void test() throws Exception {
    JdbcAssert.withNoDefaultSchema(getJDBCURL())
        .withConnection(
            new Function<Connection, Void>() {
              @Override
              public Void apply(Connection connection) {
                try {
                  Statement statement = connection.createStatement();
                  ResultSet resultSet = statement.executeQuery("USE dfs_test");
                  // TODO:  Purge nextUntilEnd(...) and calls when remaining fragment
                  // race conditions are fixed (not just DRILL-2245 fixes).
                  // resultSet.close( resultSet );
                  statement.close();
                  // connection.close() is in withConnection(...)
                  return null;
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }
            });
  }
}
