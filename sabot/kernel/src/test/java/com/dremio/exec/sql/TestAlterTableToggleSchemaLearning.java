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
package com.dremio.exec.sql;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.TemporarySystemProperties;
import com.dremio.test.UserExceptionAssert;

public class TestAlterTableToggleSchemaLearning extends BaseTestQuery {

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void setup() {
    setSystemOption(ExecConstants.ENABLE_INTERNAL_SCHEMA, "true");
  }

  private SqlNode parse(String toParse) throws SqlParseException {
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlParser parser = SqlParser.create(toParse, config);
    return parser.parseStmt();
  }

  @Test
  public void badSql() {
    String[] queries = {
      "ALTER TABLE ENABLE SCHEMA LEARNING",
      "ALTER TABLE tbl ENABLE SCHEMALEARNING"};
    for (String q : queries) {
      UserExceptionAssert
        .assertThatThrownBy(() -> test(q))
        .hasErrorType(UserBitShared.DremioPBError.ErrorType.PARSE);
    }
  }

  @Test
  public void TestParsingEnableSchemaLearningCommand () throws SqlParseException {
    parse("ALTER TABLE tbl ENABLE SCHEMA LEARNING");
  }

  @Test
  public void TestParsingDisableSchemaLearning () throws SqlParseException  {
    parse("ALTER TABLE tbl DISABLE SCHEMA LEARNING");
  }

}
