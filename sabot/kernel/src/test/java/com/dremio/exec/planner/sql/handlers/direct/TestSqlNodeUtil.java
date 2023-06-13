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
package com.dremio.exec.planner.sql.handlers.direct;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;

public class TestSqlNodeUtil {

  @Test
  public void testQueryKindSelect() {
    SqlSelect sqlSelect = mock(SqlSelect.class);
    doReturn(SqlKind.SELECT).when(sqlSelect).getKind();
    assertEquals(SqlKind.SELECT.lowerName, SqlNodeUtil.getQueryKind(sqlSelect));
  }

  @Test
  public void testQueryKindSelectWithOrderBy() {
    SqlSelect sqlSelect = mock(SqlSelect.class);
    doReturn(SqlKind.SELECT).when(sqlSelect).getKind();
    SqlOrderBy sqlOrderBy = new SqlOrderBy(new SqlParserPos(0, 0), sqlSelect, null, null, null);
    assertEquals(SqlKind.SELECT.lowerName, SqlNodeUtil.getQueryKind(sqlOrderBy));
  }

  @Test
  public void testQueryKindSelectWithWith() {
    SqlSelect sqlSelect = mock(SqlSelect.class);
    doReturn(SqlKind.SELECT).when(sqlSelect).getKind();
    SqlWith sqlWith = new SqlWith(new SqlParserPos(0, 0), null, sqlSelect);
    assertEquals(SqlKind.SELECT.lowerName, SqlNodeUtil.getQueryKind(sqlWith));
  }

  @Test
  public void testQueryKindDelete() {
    SqlDelete sqlDelete = mock(SqlDelete.class);
    doReturn(SqlKind.DELETE).when(sqlDelete).getKind();
    assertEquals(SqlKind.DELETE.lowerName, SqlNodeUtil.getQueryKind(sqlDelete));
  }

  @Test
  public void testQueryKindNull() {
    assertEquals("unknown", SqlNodeUtil.getQueryKind(null));
  }
}
