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

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

import com.dremio.exec.planner.physical.PlannerSettings;

public class TestParserAlterTableChangeColumnComplexType {
  @Test
  public void alterTableChangeStruct() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x:BIGINT NOT NULL>");
  }

  @Test
  public void alterTableAlterStruct() throws SqlParseException {
    parse("ALTER TABLE a.b.c ALTER COLUMN point point STRUCT<x:BIGINT NOT NULL>");
  }

  @Test
  public void alterTableModifyStruct() throws SqlParseException {
    parse("ALTER TABLE a.b.c MODIFY COLUMN point point STRUCT<x:BIGINT NOT NULL>");
  }

  @Test
  public void alterTableChangeStructFieldName() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point2 STRUCT<x:BIGINT NOT NULL>");
  }

  @Test
  public void alterTableChangeStructDecimal() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x:DECIMAL(38,10)>");
  }

  @Test
  public void alterTableChangeStructOfList() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x:LIST<BIGINT NOT NULL>>");
  }

  @Test
  public void alterTableChangeStructOfStruct() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x:BIGINT, y:STRUCT<x:int, y:int>>");
  }

  @Test
  public void alterTableChangeList() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point LIST<BIGINT NULL>");
  }

  @Test
  public void alterTableChangeListFieldName() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point2 LIST<BIGINT NULL>");
  }

  @Test
  public void alterTableChangeListDecimal() throws SqlParseException {
    parse("ALTER TABLE a.b.c MODIFY COLUMN point point LIST<DECIMAL(35,2) NULL>");
  }

  @Test
  public void alterTableChangeListOfStruct() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point LIST<STRUCT<x:int NULL,y:int>>");
  }

  @Test
  public void alterTableChangeListOfList() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point LIST<LIST<BIGINT>>");
  }

  @Test
  public void alterTableChangeArrayOfArray() throws SqlParseException {
    // ARRAY and LIST are interchangeable
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point ARRAY<ARRAY<BIGINT>>");
  }

  private SqlNode parse(String toParse) throws SqlParseException {
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());
    SqlParser parser = SqlParser.create(toParse, config);
    return parser.parseStmt();
  }
}
