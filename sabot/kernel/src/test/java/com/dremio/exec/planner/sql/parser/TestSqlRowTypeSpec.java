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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.dremio.exec.planner.sql.ParserConfig;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

public class TestSqlRowTypeSpec {

  ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255);

  @Test
  public void testValidationCorrectBasicType() throws SqlParseException {
    DremioSqlRowTypeSpec rowTypeSpec = createRowTypeSpec("CREATE TABLE a.b.c (col STRUCT<x: INT>)");
    assertNull(rowTypeSpec.validateType());
  }

  @Test
  public void testValidationIncorrectBasicType() throws SqlParseException {
    DremioSqlRowTypeSpec rowTypeSpec =
        createRowTypeSpec("CREATE TABLE a.b.c (col STRUCT<x: Q_INT>)");
    SqlDataTypeSpec invalidTypeSpec = rowTypeSpec.validateType();
    assertEquals(rowTypeSpec.getFieldTypes().get(0), invalidTypeSpec);
  }

  @Test
  public void testValidationNestedType() throws SqlParseException {
    DremioSqlRowTypeSpec rowTypeSpec =
        createRowTypeSpec("CREATE TABLE a.b.c (col STRUCT<x: STRUCT<y: INT>>)");
    assertNull(rowTypeSpec.validateType());
  }

  @Test
  public void testValidationNestedTypeIncorrectBasicType() throws SqlParseException {
    DremioSqlRowTypeSpec rowTypeSpec =
        createRowTypeSpec("CREATE TABLE a.b.c (col STRUCT<x: STRUCT<y: Q_INT>>)");
    SqlDataTypeSpec invalidTypeSpec = rowTypeSpec.validateType();
    DremioSqlRowTypeSpec expected =
        (DremioSqlRowTypeSpec) rowTypeSpec.getFieldTypes().get(0).getTypeNameSpec().getTypeName();
    assertEquals(expected.getFieldTypes().get(0), invalidTypeSpec);
  }

  @Test
  public void testValidationStructMultiple() throws SqlParseException {
    DremioSqlRowTypeSpec rowTypeSpec =
        createRowTypeSpec("CREATE TABLE a.b.c (col1 STRUCT<col1: INT>, col1 INT)");
    assertNull(rowTypeSpec.validateType());
  }

  private DremioSqlRowTypeSpec createRowTypeSpec(String toParse) throws SqlParseException {
    SqlParser parser = SqlParser.create(toParse, config);
    SqlCreateEmptyTable node = (SqlCreateEmptyTable) parser.parseStmt();
    DremioSqlColumnDeclaration column = (DremioSqlColumnDeclaration) node.getFieldList().get(0);
    SqlComplexDataTypeSpec dataType = (SqlComplexDataTypeSpec) column.getDataType();
    return (DremioSqlRowTypeSpec) dataType.getTypeNameSpec().getTypeName();
  }
}
