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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class DremioResultSetTest extends JdbcWithServerTestBase {

  @Test
  public void test_next_blocksFurtherAccessAfterEnd() throws SQLException {
    Statement statement = getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT 1 AS x FROM cp.\"donuts.json\" LIMIT 2");

    // Advance to first row; confirm can access data.
    assertThat(resultSet.next()).isTrue();
    assertThat(resultSet.getInt(1)).isEqualTo(1);

    // Advance from first to second (last) row, confirming data access.
    assertThat(resultSet.next()).isTrue();
    assertThat(resultSet.getInt(1)).isEqualTo(1);

    // Now advance past last row.
    assertThat(resultSet.next()).isFalse();

    // Main check:  That row data access methods now throw SQLException.
    assertThatThrownBy(() -> resultSet.getInt(1))
        .isInstanceOf(InvalidCursorStateSqlException.class)
        .hasMessageContaining("past");

    assertThat(resultSet.next()).isFalse();

    // TODO:  Ideally, test all other accessor methods.
  }

  @Test
  public void test_next_blocksFurtherAccessWhenNoRows() throws Exception {
    Statement statement = getConnection().createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT 'Hi' AS x FROM cp.\"donuts.json\" WHERE false");

    // Do initial next(). (Advance from before results to next possible
    // position (after the set of zero rows).
    assertThat(resultSet.next()).isFalse();

    // Main check:  That row data access methods throw SQLException.
    // "Result set cursor is already positioned past all rows."
    assertThatThrownBy(() -> resultSet.getString(1))
        .isInstanceOf(InvalidCursorStateSqlException.class)
        .hasMessageContaining("past")
        .hasMessageContaining("rows");

    assertThat(resultSet.next()).isFalse();

    // TODO:  Ideally, test all other accessor methods.
  }

  @Test
  public void test_getRow_isOneBased() throws Exception {
    Statement statement = getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery("VALUES (1), (2)");

    // Expect 0 when before first row:
    assertThat(resultSet.getRow()).isEqualTo(0);

    resultSet.next();

    // Expect 1 at first row:
    assertThat(resultSet.getRow()).isEqualTo(1);

    resultSet.next();

    // Expect 2 at second row:
    assertThat(resultSet.getRow()).isEqualTo(2);

    resultSet.next();

    // Expect 0 again when after last row:
    assertThat(resultSet.getRow()).isEqualTo(0);
    resultSet.next();
    assertThat(resultSet.getRow()).isEqualTo(0);
  }

  // TODO:  Ideally, test other methods.

}
