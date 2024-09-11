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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dremio.exec.planner.sql.ParserConfig;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

public class TestCreateTable {

  ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255);

  @Test
  public void testCreateTableWithSimpleMapColumn() throws SqlParseException {
    parse("CREATE TABLE a.b.c (col MAP<INT, INT>)");
  }

  @Test
  public void testCreateTableWithSimpleMapColumnNullKey() throws SqlParseException {
    assertThrows(
        SqlParseException.class,
        () -> {
          parse("CREATE TABLE a.b.c (col MAP<INT NULL, INT>)");
        });
  }

  @Test
  public void testCreateTableWithSimpleMapColumnNotNullKey() throws SqlParseException {
    assertThrows(
        SqlParseException.class,
        () -> {
          parse("CREATE TABLE a.b.c (col MAP<INT NOT NULL, INT>)");
        });
  }

  @Test
  public void testCreateTableWithSimpleMapColumnNullValue() throws SqlParseException {
    parse("CREATE TABLE a.b.c (col MAP<INT, INT NOT NULL>)");
  }

  @Test
  public void testCreateTableWithSimpleMapColumnNotNullValue() throws SqlParseException {
    parse("CREATE TABLE a.b.c (col MAP<INT, INT NOT NULL>)");
  }

  @Test
  public void testCreateTableWithMapAsListElem() throws SqlParseException {
    parse("CREATE TABLE a.b.c (col LIST<MAP<INT, INT>>)");
  }

  @Test
  public void testCreateTableWithMapAsMapKey() throws SqlParseException {
    parse("CREATE TABLE a.b.c (col MAP<MAP<INT,INT>, INT>)");
  }

  @Test
  public void testCreateTableWithMapAsMapValue() throws SqlParseException {
    parse("CREATE TABLE a.b.c (col MAP<INT, MAP<INT,INT>>)");
  }

  private SqlNode parse(String toParse) throws SqlParseException {
    SqlParser parser = SqlParser.create(toParse, config);
    return parser.parseStmt();
  }
}
