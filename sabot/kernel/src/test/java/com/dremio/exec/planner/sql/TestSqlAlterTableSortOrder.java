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

import static com.dremio.test.UserExceptionAssert.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.planner.sql.parser.SqlAlterTableSortOrder;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlAlterTableSortOrder extends BaseTestQuery {

  private final ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100);

  @Test
  public void testParseMalformedQueries() throws Exception {
    List<String> malformedQueries =
        new ArrayList<String>() {
          {
            add("ALTER TABLE t1 LOCALSORT BY");
            add("ALTER TABLE t1 LOCALSORT BY (");
            add("ALTER TABLE t1 LOCALSORT BY )");
            add("ALTER TABLE t1 LOCALSORT BY (()");
            add("ALTER TABLE t1 LOCALSORT BY ((a))");
            add("ALTER TABLE t1 LOCALSORT BY (b a e");
            add("ALTER TABLE t1 LOCALSORT BY (b a e)");
          }
        };
    for (String malformedQuery : malformedQueries) {
      parseAndVerifyMalFormat(malformedQuery);
    }
  }

  @Test
  public void testWellFormedQueries() throws Exception {
    List<String> malformedQueries =
        new ArrayList<String>() {
          {
            add("ALTER TABLE t1 LOCALSORT BY (a)");
            add("ALTER TABLE t1 LOCALSORT BY (a, b, c, d, e, f, g, h)");
            add("ALTER TABLE t1 LOCALSORT BY (blah, foo, block, mic, ToTaL_COST23)");
            add("ALTER TABLE t1 DROP LOCALSORT");
          }
        };
    for (String malformedQuery : malformedQueries) {
      parseAndVerifyWellFormat(malformedQuery);
    }
  }

  @Test
  public void testParseAndUnparse() throws Exception {
    Map<String, String> queryExpectedString =
        new HashMap<String, String>() {
          {
            put("ALTER TABLE t1 LOCALSORT BY (a)", "ALTER TABLE \"t1\" LOCALSORT BY (\"a\")");
            put(
                "ALTER TABLE t1 LOCALSORT BY (a, b, c, d, e, f, g, h)",
                "ALTER TABLE \"t1\" LOCALSORT BY (\"a\", \"b\", \"c\", \"d\", \"e\", \"f\", \"g\", \"h\")");
            put(
                "ALTER TABLE t1 LOCALSORT BY (blah, foo, block, mic, ToTaL_COST23)",
                "ALTER TABLE \"t1\" LOCALSORT BY (\"blah\", \"foo\", \"block\", \"mic\", \"ToTal_COST23\")");
            put("ALTER TABLE t1 DROP LOCALSORT", "ALTER TABLE \"t1\" DROP LOCALSORT");
          }
        };

    for (Map.Entry<String, String> entry : queryExpectedString.entrySet()) {
      parseAndVerifyUnparse(entry.getKey(), entry.getValue());
    }
  }

  private void parseAndVerifyWellFormat(String sql) {
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode instanceof SqlAlterTableSortOrder);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.ALTER_TABLE)));
  }

  private void parseAndVerifyMalFormat(String sql) {
    assertThatThrownBy(() -> SqlConverter.parseSingleStatementImpl(sql, parserConfig, false))
        .isInstanceOf(UserException.class)
        .satisfies(
            e -> assertEquals((e).getErrorType(), UserBitShared.DremioPBError.ErrorType.PARSE));
  }

  private void parseAndVerifyUnparse(String sql, String expectedString) {
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode instanceof SqlAlterTableSortOrder);

    // verify Unparse
    SqlDialect DREMIO_DIALECT =
        new SqlDialect(
            SqlDialect.DatabaseProduct.UNKNOWN,
            "Dremio",
            Character.toString(SqlUtils.QUOTE),
            NullCollation.FIRST);
    SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);
    sqlNode.unparse(writer, 0, 0);
    String actual = writer.toString();
    Assert.assertEquals(expectedString.toLowerCase(), actual.toLowerCase());
  }
}
