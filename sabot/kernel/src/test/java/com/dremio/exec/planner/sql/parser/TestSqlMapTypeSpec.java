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

public class TestSqlMapTypeSpec {

  ParserConfig config = new ParserConfig(Quoting.DOUBLE_QUOTE, 255);

  @Test
  public void testValidationCorrectBasicTypes() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec = createMapTypeSpec("CREATE TABLE a.b.c (col MAP<INT, INT>)");
    assertNull(mapTypeSpec.validateType());
  }

  @Test
  public void testValidationIncorrectBasicKeyType() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec = createMapTypeSpec("CREATE TABLE a.b.c (col MAP<Q_INT, INT>)");
    SqlDataTypeSpec invalidTypeSpec = mapTypeSpec.validateType();
    assertEquals(mapTypeSpec.getSpecKey(), invalidTypeSpec);
  }

  @Test
  public void testValidationIncorrectBasicValueType() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec = createMapTypeSpec("CREATE TABLE a.b.c (col MAP<INT, Q_INT>)");
    SqlDataTypeSpec invalidTypeSpec = mapTypeSpec.validateType();
    assertEquals(mapTypeSpec.getSpecValue(), invalidTypeSpec);
  }

  @Test
  public void testValidationKeyCannotBeComplexType() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec =
        createMapTypeSpec("CREATE TABLE a.b.c (col MAP<MAP<INT,INT>, INT>)");
    SqlDataTypeSpec invalidTypeSpec = mapTypeSpec.validateType();
    assertEquals(mapTypeSpec.getSpecKey(), invalidTypeSpec);
  }

  @Test
  public void testValidationValueCanBeComplexType() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec =
        createMapTypeSpec("CREATE TABLE a.b.c (col MAP<INT, MAP<INT,INT>>)");
    SqlDataTypeSpec invalidTypeSpec = mapTypeSpec.validateType();
    assertNull(invalidTypeSpec);
  }

  @Test
  public void testValidationNestedTypeIncorrectBasicType() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec =
        createMapTypeSpec("CREATE TABLE a.b.c (col MAP<INT, MAP<Q_INT,INT>>)");
    SqlDataTypeSpec invalidTypeSpec = mapTypeSpec.validateType();
    SqlMapTypeSpec expected =
        (SqlMapTypeSpec) mapTypeSpec.getSpecValue().getTypeNameSpec().getTypeName();
    assertEquals(expected.getSpecKey(), invalidTypeSpec);
  }

  @Test
  public void testValidationNestedTypeIncorrectMapKeyType() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec =
        createMapTypeSpec("CREATE TABLE a.b.c (col MAP<INT, MAP<LIST<INT>,INT>>)");
    SqlDataTypeSpec invalidTypeSpec = mapTypeSpec.validateType();
    SqlMapTypeSpec expected =
        (SqlMapTypeSpec) mapTypeSpec.getSpecValue().getTypeNameSpec().getTypeName();
    assertEquals(expected.getSpecKey(), invalidTypeSpec);
  }

  @Test
  public void testValidationStructValue() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec =
        createMapTypeSpec("CREATE TABLE a.b.c (col MAP<INT,STRUCT<x: INT>>)");
    SqlDataTypeSpec invalidTypeSpec = mapTypeSpec.validateType();
    assertNull(invalidTypeSpec);
  }

  @Test
  public void testValidationStructKey() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec =
        createMapTypeSpec("CREATE TABLE a.b.c (col MAP<STRUCT<x: INT>, INT>)");
    SqlDataTypeSpec invalidTypeSpec = mapTypeSpec.validateType();
    assertEquals(mapTypeSpec.getSpecKey(), invalidTypeSpec);
  }

  @Test
  public void testValidationStructMultiple() throws SqlParseException {
    SqlMapTypeSpec mapTypeSpec =
        createMapTypeSpec("CREATE TABLE a.b.c (col1 MAP<STRUCT<col1: INT>, INT>, col1 INT)");
    SqlDataTypeSpec invalidTypeSpec = mapTypeSpec.validateType();
    assertEquals(mapTypeSpec.getSpecKey(), invalidTypeSpec);
  }

  private SqlMapTypeSpec createMapTypeSpec(String toParse) throws SqlParseException {
    SqlParser parser = SqlParser.create(toParse, config);
    SqlCreateEmptyTable node = (SqlCreateEmptyTable) parser.parseStmt();
    DremioSqlColumnDeclaration column = (DremioSqlColumnDeclaration) node.getFieldList().get(0);
    SqlComplexDataTypeSpec dataType = (SqlComplexDataTypeSpec) column.getDataType();
    return (SqlMapTypeSpec) dataType.getTypeNameSpec().getTypeName();
  }
}
