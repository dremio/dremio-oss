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
package com.dremio.exec.planner.sql;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.SqlAlterTableChangeColumnSetOption;
import com.google.common.collect.Sets;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSQLAlterTableChangeColumnSetOption {
  private final ParserConfig parserConfig =
      new ParserConfig(
          ParserConfig.QUOTING,
          100,
          PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  public static Stream<Arguments> testChangeColumnOption() {
    return Stream.of(
        Arguments.of("dummy_table", "dummy_column", "Modify", "prop1", "strValue"),
        Arguments.of("dummy_table", "dummy_column", "change", "prop1", false),
        Arguments.of("dummy_table", "dummy_column", "alter", "prop1", 10),
        Arguments.of("dummy_table", "dummy_column", "alter", "nullProp", null),
        Arguments.of("dummy_table", "dummy_column", "Modify column", "prop1", "strValue"),
        Arguments.of("dummy_table", "dummy_column", "change column", "prop1", false),
        Arguments.of("dummy_table", "dummy_column", "alter column", "prop1", 10),
        Arguments.of("dummy_table", "dummy_column", "alter column", "nullProp", null));
  }

  @ParameterizedTest(name = "{index}: {0}, {1}, {2}, {3}, {4}")
  @MethodSource
  public void testChangeColumnOption(
      String table,
      String column,
      String changeColumnKeyword,
      String propertyName,
      Object propertyValue) {
    final String sql;
    if (propertyValue == null) {
      sql =
          String.format(
              "ALTER TABLE %s %s %s RESET %s", table, changeColumnKeyword, column, propertyName);
    } else {
      sql =
          String.format(
              "ALTER TABLE %s %s %s SET %s = %s",
              table,
              changeColumnKeyword,
              column,
              propertyName,
              propertyValue instanceof String
                  ? String.format("'%s'", propertyValue)
                  : propertyValue);
    }

    verifyNode(sql, table, column, propertyValue);
  }

  void verifyNode(String sql, String table, String column, Object propertyValue) {
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assertions.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.SET_OPTION)));

    final SqlAlterTableChangeColumnSetOption alterTableChangeColumnSetOption =
        (SqlAlterTableChangeColumnSetOption) sqlNode;
    Assertions.assertEquals(column, alterTableChangeColumnSetOption.getColumn());
    Assertions.assertEquals(table, alterTableChangeColumnSetOption.getTable().toString());

    if (propertyValue == null) {
      Assertions.assertNull(alterTableChangeColumnSetOption.getValue());
    } else if (propertyValue == Optional.empty()) {
      Assertions.assertNull(alterTableChangeColumnSetOption.getValue());
    } else if (propertyValue instanceof String) {
      Assertions.assertEquals(
          propertyValue, ((SqlLiteral) alterTableChangeColumnSetOption.getValue()).toValue());
    } else if (propertyValue instanceof Boolean) {
      Assertions.assertEquals(
          propertyValue, ((SqlLiteral) alterTableChangeColumnSetOption.getValue()).booleanValue());
    } else if (propertyValue instanceof Integer) {
      Assertions.assertEquals(
          propertyValue, ((SqlLiteral) alterTableChangeColumnSetOption.getValue()).intValue(true));
    }
  }
}
