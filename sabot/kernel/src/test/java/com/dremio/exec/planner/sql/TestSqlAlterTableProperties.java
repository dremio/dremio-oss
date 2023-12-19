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

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.parser.SqlAlterTableProperties;
import com.dremio.exec.proto.UserBitShared;
import com.google.common.collect.Sets;

public class TestSqlAlterTableProperties extends BaseTestQuery {
  private final ParserConfig parserConfig = new ParserConfig(ParserConfig.QUOTING, 100,
          PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  @Test
  public void testParseMalformedQueries() throws Exception {
    List<String> malformedQueries = new ArrayList<String>() {{
      add("ALTER TABLE t1 SET TBLPROPERTIES ()");
      add("ALTER TABLE t1 SET TBLPROPERTIES");
      add("ALTER TABLE t1 SET TBLPROPERTIES ('property_name' = 'property_value', 'property_name1' = )");
      add("ALTER TABLE t1 SET TBLPROPERTIES ('property_name' )");
      add("ALTER TABLE t1 SET TBLPROPERTIES ('property_name', 'property_name1' )");
      add("ALTER TABLE t1 UNSET TBLPROPERTIES ('property_name' = 'property_value')");
      add("ALTER TABLE t1 UNSET TBLPROPERTIES ('property_name',  'property_name1' = )");
      add("ALTER TABLE t1 SET TBLPROPERTIES ('property_name', 'property_name1' = 'property_value' )");
      add("ALTER TABLE t1 SETUNSET TBLPROPERTIES ('property_name' )");
    }};

    for (String malformedQuery : malformedQueries) {
      parseAndVerifyMalFormat(malformedQuery);
    }
  }

  @Test
  public void testParseWellformedQueries() throws Exception {
    List<String> wellformedQueries = new ArrayList<String>() {{
      add("ALTER TABLE t1 SET TBLPROPERTIES ('property_name' = 'property_value')");
      add("ALTER TABLE t1 SET TBLPROPERTIES ('property_name' = 'property_value', 'property_name1' = 'property_value1')");
      add("ALTER TABLE t1 UNSET TBLPROPERTIES ('property_name')");
      add("ALTER TABLE t1 UNSET TBLPROPERTIES ('property_name', 'property_name1')");
    }};

    for (String wellformedQuery : wellformedQueries) {
      parseAndVerifyWellFormat(wellformedQuery);
    }
  }

  @Test
  public void testParseAndUnparse() throws Exception {
    Map<String, String> queryExpectedStrings = new HashMap<String, String>() {{
      put("ALTER TABLE t1 SET TBLPROPERTIES ('property_name' = 'property_value', 'property_name1' = 'property_value1')",
              "ALTER TABLE \"t1\" SET TBLPROPERTIES ('property_name' = 'property_value', 'property_name1' = 'property_value1')");
      put("ALTER TABLE t1 UNSET TBLPROPERTIES ('property_name', 'property_name1')",
              "ALTER TABLE \"t1\" UNSET TBLPROPERTIES ('property_name', 'property_name1')");
    }};

    for (Map.Entry<String,String> entry : queryExpectedStrings.entrySet())  {
      parseAndVerifyUnparse(entry.getKey(), entry.getValue());
    }
  }

  private void parseAndVerifyWellFormat(String sql) {
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode instanceof SqlAlterTableProperties);
    Assert.assertTrue(sqlNode.isA(Sets.immutableEnumSet(SqlKind.ALTER_TABLE)));
  }

  private void parseAndVerifyUnparse(String sql, String expectedString) {
    SqlNode sqlNode = SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    Assert.assertTrue(sqlNode instanceof SqlAlterTableProperties);

    // verify unParse
    SqlDialect DREMIO_DIALECT =
            new SqlDialect(SqlDialect.DatabaseProduct.UNKNOWN, "Dremio", Character.toString(SqlUtils.QUOTE), NullCollation.FIRST);
    SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);
    sqlNode.unparse(writer, 0, 0);
    String actual = writer.toString();
    Assert.assertEquals(expectedString.toLowerCase(), actual.toLowerCase());
  }

  private void parseAndVerifyMalFormat(String sql) {
    try {
      SqlConverter.parseSingleStatementImpl(sql, parserConfig, false);
    } catch (UserException ue) {
      assertEquals(ue.getErrorType(), UserBitShared.DremioPBError.ErrorType.PARSE);
    }
  }
}
