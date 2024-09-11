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

public class TestSqlArrayTypeSpec {

  ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255);

  @Test
  public void testValidationCorrectBasicTypesList() throws SqlParseException {
    SqlArrayTypeSpec arrayTypeSpec = createArrayTypeSpec("CREATE TABLE a.b.c (col LIST<INT>)");
    assertNull(arrayTypeSpec.validateType());
  }

  @Test
  public void testValidationCorrectBasicTypesArray() throws SqlParseException {
    SqlArrayTypeSpec arrayTypeSpec = createArrayTypeSpec("CREATE TABLE a.b.c (col ARRAY<INT>)");
    assertNull(arrayTypeSpec.validateType());
  }

  @Test
  public void testValidationIncorrectBasicTypeList() throws SqlParseException {
    SqlArrayTypeSpec arrayTypeSpec = createArrayTypeSpec("CREATE TABLE a.b.c (col LIST<Q_INT>)");
    SqlDataTypeSpec invalidTypeSpec = arrayTypeSpec.validateType();
    assertEquals(arrayTypeSpec.getSpec(), invalidTypeSpec);
  }

  @Test
  public void testValidationIncorrectBasicTypeArray() throws SqlParseException {
    SqlArrayTypeSpec arrayTypeSpec = createArrayTypeSpec("CREATE TABLE a.b.c (col ARRAY<Q_INT>)");
    SqlDataTypeSpec invalidTypeSpec = arrayTypeSpec.validateType();
    assertEquals(arrayTypeSpec.getSpec(), invalidTypeSpec);
  }

  @Test
  public void testValidationNestedType() throws SqlParseException {
    SqlArrayTypeSpec arrayTypeSpec =
        createArrayTypeSpec("CREATE TABLE a.b.c (col LIST<LIST<INT>>)");
    assertNull(arrayTypeSpec.validateType());
  }

  @Test
  public void testValidationNestedTypeIncorrectBasicTypeList() throws SqlParseException {
    SqlArrayTypeSpec arrayTypeSpec =
        createArrayTypeSpec("CREATE TABLE a.b.c (col LIST<LIST<Q_INT>>)");
    SqlDataTypeSpec invalidTypeSpec = arrayTypeSpec.validateType();
    SqlArrayTypeSpec expected =
        (SqlArrayTypeSpec) arrayTypeSpec.getSpec().getTypeNameSpec().getTypeName();
    assertEquals(expected.getSpec(), invalidTypeSpec);
  }

  @Test
  public void testValidationStruct() throws SqlParseException {
    SqlArrayTypeSpec arrayTypeSpec =
        createArrayTypeSpec("CREATE TABLE a.b.c (col LIST<STRUCT<x: INT>>)");
    assertNull(arrayTypeSpec.validateType());
  }

  @Test
  public void testValidationStructMultiple() throws SqlParseException {
    SqlArrayTypeSpec arrayTypeSpec =
        createArrayTypeSpec("CREATE TABLE a.b.c (col1 LIST<STRUCT<col1: INT>>, col1 INT)");
    assertNull(arrayTypeSpec.validateType());
  }

  private SqlArrayTypeSpec createArrayTypeSpec(String toParse) throws SqlParseException {
    SqlParser parser = SqlParser.create(toParse, config);
    SqlCreateEmptyTable node = (SqlCreateEmptyTable) parser.parseStmt();
    DremioSqlColumnDeclaration column = (DremioSqlColumnDeclaration) node.getFieldList().get(0);
    SqlComplexDataTypeSpec dataType = (SqlComplexDataTypeSpec) column.getDataType();
    return (SqlArrayTypeSpec) dataType.getTypeNameSpec().getTypeName();
  }
}
