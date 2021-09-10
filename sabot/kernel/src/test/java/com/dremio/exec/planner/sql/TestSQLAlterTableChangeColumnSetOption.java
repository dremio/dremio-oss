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

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.SqlAlterTableChangeColumnSetOption;
import com.google.common.collect.Sets;


@RunWith(Parameterized.class)
public class TestSQLAlterTableChangeColumnSetOption {
  private final ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}, {3}, {4}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
      {"dummy_table", "dummy_column", "Modify", "prop1", "strValue"},
      {"dummy_table", "dummy_column", "change", "prop1", false},
      {"dummy_table", "dummy_column", "alter", "prop1", 10},
      {"dummy_table", "dummy_column", "alter", "nullProp", null},
      {"dummy_table", "dummy_column", "Modify column", "prop1", "strValue"},
      {"dummy_table", "dummy_column", "change column", "prop1", false},
      {"dummy_table", "dummy_column", "alter column", "prop1", 10},
      {"dummy_table", "dummy_column", "alter column", "nullProp", null}
    });
  }

  private final String table;
  private final String column;
  private final String changeColumnKeyword;
  private final String propertyName;
  private final Object propertyValue;

  public TestSQLAlterTableChangeColumnSetOption(String table, String column, String changeColumnKeyword, String propertyName, Object propertyValue) {
    this.table = table;
    this.column = column;
    this.changeColumnKeyword = changeColumnKeyword;
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
  }

  @Test
  public void testChangeColumnOption() {
    final String sql;
    if (propertyValue == null) {
      sql = String.format("ALTER TABLE %s %s %s RESET %s", table, changeColumnKeyword, column, propertyName);
    } else {
      sql = String.format("ALTER TABLE %s %s %s SET %s = %s", table, changeColumnKeyword, column, propertyName,
        propertyValue instanceof String ? String.format("'%s'", propertyValue) : propertyValue);
    }

    verifyNode(sql);
  }

  void verifyNode(String sql) {
    final SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.SET_OPTION)));

    final SqlAlterTableChangeColumnSetOption alterTableChangeColumnSetOption = (SqlAlterTableChangeColumnSetOption) sqlNode;
    Assert.assertEquals(column, alterTableChangeColumnSetOption.getColumn());
    Assert.assertEquals(table, alterTableChangeColumnSetOption.getTable().toString());

    if (propertyValue == null) {
      Assert.assertNull(alterTableChangeColumnSetOption.getValue());
    } else if (propertyValue == Optional.empty()) {
      Assert.assertNull(alterTableChangeColumnSetOption.getValue());
    } else if (propertyValue instanceof String) {
      Assert.assertEquals(propertyValue, ((SqlLiteral) alterTableChangeColumnSetOption.getValue()).toValue());
    } else if (propertyValue instanceof Boolean) {
      Assert.assertEquals(propertyValue, ((SqlLiteral) alterTableChangeColumnSetOption.getValue()).booleanValue());
    } else if (propertyValue instanceof Integer) {
      Assert.assertEquals(propertyValue, ((SqlLiteral) alterTableChangeColumnSetOption.getValue()).intValue(true));
    }
  }
}
