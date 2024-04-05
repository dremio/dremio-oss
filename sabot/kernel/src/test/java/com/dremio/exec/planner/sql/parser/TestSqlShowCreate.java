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
package com.dremio.exec.planner.sql.parser;

import static com.dremio.exec.calcite.SqlNodes.DREMIO_DIALECT;
import static com.dremio.exec.planner.sql.parser.TestParserUtil.parse;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Test;

/** Tests for SHOW CREATE (VIEW | TABLE) syntax */
public class TestSqlShowCreate {
  private SqlPrettyWriter writer = new SqlPrettyWriter(DREMIO_DIALECT);

  @Test
  public void testShowCreateBasic() throws SqlParseException {
    testShowCreateBasicHelper("VIEW", true);
    testShowCreateBasicHelper("TABLE", false);
  }

  private void testShowCreateBasicHelper(String datasetType, boolean isView)
      throws SqlParseException {
    SqlNode parsed = parse(String.format("SHOW CREATE %s x.y.z", datasetType));
    assertThat(parsed).isInstanceOf(SqlShowCreate.class);

    SqlShowCreate sqlShowCreate = (SqlShowCreate) parsed;
    assertThat(sqlShowCreate.getIsView()).isEqualTo(isView);
    assertThat(sqlShowCreate.getFullName()).isEqualTo("x.y.z");
    assertThat(sqlShowCreate.getRefType()).isNull();
    assertThat(sqlShowCreate.getRefValue()).isNull();

    writer.reset();
    parsed.unparse(writer, 0, 0);
    assertThat(writer.toString())
        .isEqualTo(String.format("SHOW CREATE %s \"x\".\"y\".\"z\"", datasetType));
  }

  @Test
  public void testShowCreateWithVersionContext() throws SqlParseException {
    testShowCreateBasicHelper("VIEW", true);
    testShowCreateBasicHelper("TABLE", false);
  }

  private void testShowCreateWithVersionContextHelper(String datasetType, boolean isView)
      throws SqlParseException {
    SqlNode parsed = parse(String.format("SHOW CREATE %s x.y.z AT BRANCH test", datasetType));
    assertThat(parsed).isInstanceOf(SqlShowCreate.class);

    SqlShowCreate sqlShowCreate = (SqlShowCreate) parsed;
    assertThat(sqlShowCreate.getIsView()).isEqualTo(isView);
    assertThat(sqlShowCreate.getFullName()).isEqualTo("x.y.z");
    assertThat(sqlShowCreate.getRefType()).isEqualTo(ReferenceType.BRANCH);
    assertThat(sqlShowCreate.getRefValue().toString()).isEqualTo("test");

    writer.reset();
    parsed.unparse(writer, 0, 0);
    assertThat(writer.toString())
        .isEqualTo(
            String.format("SHOW CREATE %s \"x\".\"y\".\"z\" AT BRANCH \"test\"", datasetType));
  }
}
