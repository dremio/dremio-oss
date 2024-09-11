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

public class TestParserAlterTableChangeColumnComplexType {
  @Test
  public void alterTableChangeRow() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point ROW(x BIGINT NOT NULL )");
  }

  @Test
  public void alterTableChangeStruct() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x BIGINT NOT NULL>");
  }

  @Test
  public void alterTableChangeMap() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point MAP<BIGINT, BIGINT>");
  }

  @Test
  public void alterTableAlterRow() throws SqlParseException {
    parse("ALTER TABLE a.b.c ALTER COLUMN point point ROW(x BIGINT NOT NULL )");
  }

  @Test
  public void alterTableAlterStruct() throws SqlParseException {
    parse("ALTER TABLE a.b.c ALTER COLUMN point point STRUCT<x : BIGINT NOT NULL>");
  }

  @Test
  public void alterTableAlterMap() throws SqlParseException {
    parse("ALTER TABLE a.b.c ALTER COLUMN point point MAP<BIGINT, BIGINT>");
  }

  @Test
  public void alterTableModifyStruct() throws SqlParseException {
    parse("ALTER TABLE a.b.c MODIFY COLUMN point point ROW(x BIGINT NOT NULL )");
  }

  @Test
  public void alterTableChangeStructFieldName() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point2 ROW(x BIGINT NOT NULL )");
  }

  @Test
  public void alterTableChangeRowDecimal() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point ROW(x DECIMAL(38,10) )");
  }

  @Test
  public void alterTableChangeStructDecimal() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x: DECIMAL(38,10) >");
  }

  @Test
  public void alterTableChangeMapDecimal() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point MAP<BIGINT, DECIMAL(38,10)>");
  }

  @Test
  public void alterTableChangeRowOfArray() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point ROW(x  ARRAY(BIGINT NOT NULL ) )");
  }

  @Test
  public void alterTableChangeStructOfArray() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x : ARRAY(BIGINT NOT NULL )>");
  }

  @Test
  public void alterTableChangeStructOfList() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x : LIST<BIGINT NOT NULL >>");
  }

  @Test
  public void alterTableChangeMapOfArray() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x : MAP(BIGINT, BIGINT)>");
  }

  @Test
  public void alterTableChangeRowOfRow() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point ROW(x BIGINT, y ROW(x int, y int ) )");
  }

  @Test
  public void alterTableChangeStructOfStruct() throws SqlParseException {
    parse(
        "ALTER TABLE a.b.c CHANGE COLUMN point point STRUCT<x: BIGINT, y : STRUCT<x :int, y: int>>");
  }

  @Test
  public void alterTableChangeMapAsMapValue() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point MAP(BIGINT, MAP(BIGINT,BIGINT) )");
  }

  /** The parser will allow this construction, type is checked later. */
  @Test
  public void alterTableChangeMapAsMapKey() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point MAP(MAP(BIGINT,BIGINT), BIGINT)");
  }

  @Test
  public void alterTableChangeArray() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point  ARRAY(BIGINT NULL )");
  }

  @Test
  public void alterTableChangeList() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point  LIST<BIGINT NULL>");
  }

  @Test
  public void alterTableChangeListFieldName() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point2  ARRAY(BIGINT NULL )");
  }

  @Test
  public void alterTableChangeMapFieldName() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point2 MAP(BIGINT, BIGINT)");
  }

  @Test
  public void alterTableChangeArrayDecimal() throws SqlParseException {
    parse("ALTER TABLE a.b.c MODIFY COLUMN point point  ARRAY(DECIMAL(35,2) NULL )");
  }

  @Test
  public void alterTableChangeListDecimal() throws SqlParseException {
    parse("ALTER TABLE a.b.c MODIFY COLUMN point point  LIST<DECIMAL(35,2) NULL>");
  }

  @Test
  public void alterTableChangeArrayOfRow() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point  ARRAY(ROW(x int NULL,y int ) )");
  }

  @Test
  public void alterTableChangeListOfStruct() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point  LIST<STRUCT<x: int NULL,y : int>>");
  }

  @Test
  public void alterTableChangeListOfList() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point  LIST<LIST<BIGINT>>");
  }

  @Test
  public void alterTableChangeArrayOfArray() throws SqlParseException {
    parse("ALTER TABLE a.b.c CHANGE COLUMN point point ARRAY(ARRAY(BIGINT ) )");
  }

  private SqlNode parse(String toParse) throws SqlParseException {
    ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255);
    SqlParser parser = SqlParser.create(toParse, config);
    return parser.parseStmt();
  }
}
