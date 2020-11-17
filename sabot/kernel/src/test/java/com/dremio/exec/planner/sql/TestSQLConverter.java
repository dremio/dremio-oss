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

import static org.junit.Assert.assertEquals;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.physical.PlannerSettings;

public class TestSQLConverter {

  private void validateFormattedIs(String sql, SqlParserPos pos, String expected) {
    String formatted = SqlConverter.formatSQLParsingError(sql, pos);
    assertEquals(expected, formatted);
  }

  @Test
  public void testErrorFormating() {
    String sql = "Select * from Foo\nwhere tadadidada;\n";
    validateFormattedIs(sql, new SqlParserPos(1, 2),
        "Select * from Foo\n"
      + " ^\n"
      + "where tadadidada;\n");
    validateFormattedIs(sql, new SqlParserPos(2, 2),
        "Select * from Foo\n"
      + "where tadadidada;\n"
      + " ^\n" );
    validateFormattedIs(sql, new SqlParserPos(1, 10),
        "Select * from Foo\n"
      + "         ^\n"
      + "where tadadidada;\n");
    validateFormattedIs(sql, new SqlParserPos(-11, -10), sql);
    validateFormattedIs(sql, new SqlParserPos(0, 10), sql);
    validateFormattedIs(sql, new SqlParserPos(100, 10), sql);
  }

  @Test(expected = UserException.class)
  public void testFailMultipleQueries() {
    ParserConfig config = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlConverter.parseSingleStatementImpl("select * from t1; select * from t2", config, false);
  }

  @Test
  public void testPassSemicolon() {
    ParserConfig config = new ParserConfig(ParserConfig.QUOTING, 100, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlNode node = SqlConverter.parseSingleStatementImpl("select * from t1;", config, false);
    assertEquals("SELECT *\n" +
      "FROM \"t1\"", node.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
  }
}
