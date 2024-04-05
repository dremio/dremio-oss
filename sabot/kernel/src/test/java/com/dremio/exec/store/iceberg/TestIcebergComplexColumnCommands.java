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
package com.dremio.exec.store.iceberg;

import com.dremio.BaseTestQuery;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.test.UserExceptionAssert;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.Test;

@SuppressWarnings("UseCorrectAssertInTests")
public class TestIcebergComplexColumnCommands extends BaseTestQuery {

  @Test
  public void testCreateComplexList() throws Exception {
    final String newTblName = "createTableSimpleList";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1  ARRAY(INT))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isListType();
      assert table.schema().columns().get(0).type().asListType().elementType()
          == Types.IntegerType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateComplexListDecimal() throws Exception {
    final String newTblName = "createTableSimpleListDecimal";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1  ARRAY(DECIMAL(3,8)))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isListType();
      assert table.schema().columns().get(0).type().asListType().elementType()
          instanceof Types.DecimalType;
      assert ((Types.DecimalType) table.schema().columns().get(0).type().asListType().elementType())
              .scale()
          == 8;
      assert ((Types.DecimalType) table.schema().columns().get(0).type().asListType().elementType())
              .precision()
          == 3;
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateComplexStructDecimal() throws Exception {
    final String newTblName = "createTableSimpleStructDecimal";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1 ROW(x  DECIMAL(3,8)))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isStructType();
      assert table.schema().columns().get(0).type().asStructType().fields().size() == 1;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table.schema().columns().get(0).type().asStructType().fields().get(0).type()
          instanceof Types.DecimalType;
      assert ((Types.DecimalType)
                  table.schema().columns().get(0).type().asStructType().fields().get(0).type())
              .scale()
          == 8;
      assert ((Types.DecimalType)
                  table.schema().columns().get(0).type().asStructType().fields().get(0).type())
              .precision()
          == 3;
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateComplexStruct() throws Exception {
    final String newTblName = "createTableSimpleStruct";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 ROW(x INT))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isStructType();
      assert table.schema().columns().get(0).type().asStructType().fields().size() == 1;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table.schema().columns().get(0).type().asStructType().fields().get(0).type()
          == Types.IntegerType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateComplexListOfList() throws Exception {
    final String newTblName = "createTableSimpleListOfList";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1  ARRAY( ARRAY(DECIMAL(40,8))))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isListType();
      assert table.schema().columns().get(0).type().asListType().elementType().isListType();
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asListType()
              .elementType()
              .asListType()
              .elementType()
          instanceof Types.DecimalType;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asListType()
                      .elementType()
                      .asListType()
                      .elementType())
              .scale()
          == 8;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asListType()
                      .elementType()
                      .asListType()
                      .elementType())
              .precision()
          == 38;
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateComplexListOfListOfList() throws Exception {
    final String newTblName = "createTableSimpleListOfListOfList";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1  ARRAY( ARRAY( ARRAY(DECIMAL(40,8)))))",
              TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isListType();
      assert table.schema().columns().get(0).type().asListType().elementType().isListType();
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asListType()
          .elementType()
          .asListType()
          .elementType()
          .isListType();
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asListType()
              .elementType()
              .asListType()
              .elementType()
              .asListType()
              .elementType()
          instanceof Types.DecimalType;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asListType()
                      .elementType()
                      .asListType()
                      .elementType()
                      .asListType()
                      .elementType())
              .scale()
          == 8;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asListType()
                      .elementType()
                      .asListType()
                      .elementType()
                      .asListType()
                      .elementType())
              .precision()
          == 38;
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateComplexListOfStruct() throws Exception {
    final String newTblName = "createTableSimpleListOfStruct";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1  ARRAY(ROW( x   DECIMAL(40,8))))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isListType();
      assert table.schema().columns().get(0).type().asListType().elementType().isStructType();
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asListType()
              .elementType()
              .asStructType()
              .fields()
              .size()
          == 1;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asListType()
          .elementType()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asListType()
              .elementType()
              .asStructType()
              .fields()
              .get(0)
              .type()
          instanceof Types.DecimalType;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asListType()
                      .elementType()
                      .asStructType()
                      .fields()
                      .get(0)
                      .type())
              .scale()
          == 8;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asListType()
                      .elementType()
                      .asStructType()
                      .fields()
                      .get(0)
                      .type())
              .precision()
          == 38;
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateComplexStructOfList() throws Exception {
    final String newTblName = "createTableSimpleStructOfList";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1 ROW(x    ARRAY(DECIMAL(40,8))))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isStructType();
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .type()
          .isListType();
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(0)
              .type()
              .asListType()
              .elementType()
          instanceof Types.DecimalType;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asStructType()
                      .fields()
                      .get(0)
                      .type()
                      .asListType()
                      .elementType())
              .scale()
          == 8;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asStructType()
                      .fields()
                      .get(0)
                      .type()
                      .asListType()
                      .elementType())
              .precision()
          == 38;
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateComplexStructOfStruct() throws Exception {
    final String newTblName = "createTableSimpleStructOfStruct";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1 ROW(x  ROW( x  DECIMAL(40,8))))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isStructType();
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .type()
          .isStructType();
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(0)
              .type()
          instanceof Types.DecimalType;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asStructType()
                      .fields()
                      .get(0)
                      .type()
                      .asStructType()
                      .fields()
                      .get(0)
                      .type())
              .scale()
          == 8;
      assert ((Types.DecimalType)
                  table
                      .schema()
                      .columns()
                      .get(0)
                      .type()
                      .asStructType()
                      .fields()
                      .get(0)
                      .type()
                      .asStructType()
                      .fields()
                      .get(0)
                      .type())
              .precision()
          == 38;
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testAddComplexList() throws Exception {
    final String newTblName = "addTableSimpleList";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 int)", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s add columns (col2  ARRAY(int))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 2;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(1).name().equalsIgnoreCase("col2");
      assert table.schema().columns().get(1).type().isListType();
      assert table.schema().columns().get(1).type().asListType().elementType()
          == Types.IntegerType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testAddComplexStruct() throws Exception {
    final String newTblName = "addTableSimpleStruct";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 int)", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s add columns (col2 ROW(x  int))", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 2;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(1).name().equalsIgnoreCase("col2");
      assert table.schema().columns().get(1).type().isStructType();
      assert table.schema().columns().get(1).type().asStructType().fields().size() == 1;
      assert table
          .schema()
          .columns()
          .get(1)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table.schema().columns().get(1).type().asStructType().fields().get(0).type()
          == Types.IntegerType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testCreateElementNamedColumn() throws Exception {
    final String newTblName = "createTableSimpleStructOfList";
    try {
      String query =
          String.format(
              "create table %s.%s (element ROW(x ARRAY(DECIMAL(40,8))))", TEMP_SCHEMA, newTblName);
      // element is a keyword and cannot be used as column name. Without this restriction we cannot
      // parse LIST datatype.
      UserExceptionAssert.assertThatThrownBy(() -> test(query))
          .hasErrorType(UserBitShared.DremioPBError.ErrorType.PARSE);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void testUnsupportedChangeComplexStruct() throws Exception {
    final String newTblName = "unSupportedChangeTableSimpleStruct";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 ROW(x int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1 ROW(x  varchar)", TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "VALIDATION ERROR: Cannot change data type of column [col1.x] from INTEGER to VARCHAR");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangePrimitiveToComplexStruct() throws Exception {
    final String newTblName = "unSupportedChangePrimitiveTableSimpleStruct";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 ROW(x int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1 ROW(x  ROW(z int))",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert a primitive field [col1.x] to a complex type");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangePrimitiveToComplexStructRoot() throws Exception {
    final String newTblName = "unSupportedChangePrimitiveTableSimpleStructRoot";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 int)", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1 ROW(x int)", TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert a primitive field [col1] to a complex type");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangePrimitiveToComplexList() throws Exception {
    final String newTblName = "unSupportedChangePrimitiveTableSimpleList";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1  ARRAY(int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1  ARRAY( ARRAY(int))",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert a primitive field [col1.element] to a complex type");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangePrimitiveToComplexListRoot() throws Exception {
    final String newTblName = "unSupportedChangePrimitiveTableSimpleListRoot";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 int)", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1  ARRAY(int)", TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert a primitive field [col1] to a complex type");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangeComplexStructToPrimitiveRoot() throws Exception {
    final String newTblName = "unSupportedChangeChangeComplexStructToPrimitiveRoot";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 ROW(x int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format("alter table %s.%s change column col1 col1  int", TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert a complex field [col1] to a primitive type");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangeComplexStructToPrimitive() throws Exception {
    final String newTblName = "unSupportedChangeChangeComplexStructToPrimitive";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1 ROW(y ROW(x int)))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1  ROW(y int)", TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert a complex field [col1.y] to a primitive type");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangeComplexListToPrimitiveRoot() throws Exception {
    final String newTblName = "unSupportedChangeComplexListToPrimitiveRoot";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 int)", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1  ARRAY(int)", TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert a primitive field [col1] to a complex type");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangeComplexListToPrimitive() throws Exception {
    final String newTblName = "unSupportedChangeComplexListToPrimitive";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1  ARRAY(int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1  ARRAY( ARRAY(int))",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert a primitive field [col1.element] to a complex type");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangeComplexListToStructRoot() throws Exception {
    final String newTblName = "unSupportedChangeComplexListToStructRoot";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1  ARRAY(int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1 ROW(y int)", TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert complex field [col1] from [list<int>] to [Struct]");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testUnsupportedChangeComplexListToStruct() throws Exception {
    final String newTblName = "unSupportedChangeComplexListToStruct";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1  ARRAY( ARRAY(int)))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1  ARRAY(ROW(y int))",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert complex field [col1.element] from [list<int>] to [Struct]");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testChangeComplexStruct() throws Exception {
    final String newTblName = "changeTableSimpleStruct";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query = String.format("create table %s.%s (col1 ROW(x int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s alter column col1 col1 ROW(x  bigint)", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isStructType();
      assert table.schema().columns().get(0).type().asStructType().fields().size() == 1;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table.schema().columns().get(0).type().asStructType().fields().get(0).type()
          == Types.LongType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testChangeComplexList() throws Exception {
    final String newTblName = "changeTableSimpleList";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1  ARRAY(int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s alter column col1 col1  ARRAY(bigint)", TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isListType();
      assert table.schema().columns().get(0).type().asListType().elementType()
          == Types.LongType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testChangeComplexStructDropAddFields() throws Exception {
    final String newTblName = "changeTableSimpleStructDropAddFields";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1 ROW(x  int, y  bigint, z  varchar))",
              TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s modify column col1 col2 ROW(x  bigint,y  bigint, c  varchar, d  DECIMAL(40,3), e    ARRAY(int))",
              TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col2");
      assert table.schema().columns().get(0).type().isStructType();
      assert table.schema().columns().get(0).type().asStructType().fields().size() == 5;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table.schema().columns().get(0).type().asStructType().fields().get(0).type()
          == Types.LongType.get();
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .name()
          .equalsIgnoreCase("y");
      assert table.schema().columns().get(0).type().asStructType().fields().get(1).type()
          == Types.LongType.get();
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(2)
          .name()
          .equalsIgnoreCase("c");
      assert table.schema().columns().get(0).type().asStructType().fields().get(2).type()
          == Types.StringType.get();
      assert table.schema().columns().get(0).type().asStructType().fields().get(3).type()
          instanceof Types.DecimalType;
      assert ((Types.DecimalType)
                  table.schema().columns().get(0).type().asStructType().fields().get(3).type())
              .scale()
          == 3;
      assert ((Types.DecimalType)
                  table.schema().columns().get(0).type().asStructType().fields().get(3).type())
              .precision()
          == 38;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(4)
          .name()
          .equalsIgnoreCase("e");
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(4)
              .type()
              .asListType()
              .elementType()
          == Types.IntegerType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testChangeComplexStructDropAddFields3Levels() throws Exception {
    final String newTblName = "changeTableSimpleStructDropAddFields3Levels";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1 ROW(x  int, y  ROW( x  ROW(z int)), z  varchar))",
              TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s modify column col1 col1 ROW(x  int, y  ROW( x  ROW(z bigint, x   ARRAY(INT))), z  varchar)",
              TEMP_SCHEMA, newTblName);
      test(query);
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isStructType();
      assert table.schema().columns().get(0).type().asStructType().fields().size() == 3;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table.schema().columns().get(0).type().asStructType().fields().get(0).type()
          == Types.IntegerType.get();
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .name()
          .equalsIgnoreCase("y");
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .type()
          .isStructType();
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(1)
              .type()
              .asStructType()
              .fields()
              .size()
          == 1;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .type()
          .isStructType();
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(1)
              .type()
              .asStructType()
              .fields()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .size()
          == 2;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("z");
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(1)
              .type()
              .asStructType()
              .fields()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(0)
              .type()
          == Types.LongType.get();
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .name()
          .equalsIgnoreCase("x");
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .type()
          .isListType();
      assert table
              .schema()
              .columns()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(1)
              .type()
              .asStructType()
              .fields()
              .get(0)
              .type()
              .asStructType()
              .fields()
              .get(1)
              .type()
              .asListType()
              .elementType()
          == Types.IntegerType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testSingleCommitNatureOfChangeColumn() throws Exception {
    final String newTblName = "SingleCommit";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1 ROW(a int,b int,c int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s change column col1 col1 ROW(b bigint,c  ARRAY(int))",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(
          query,
          "UNSUPPORTED_OPERATION ERROR: Cannot convert a primitive field [col1.c] to a complex type");
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isStructType();
      assert table.schema().columns().get(0).type().asStructType().fields().size() == 3;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("a");
      assert table.schema().columns().get(0).type().asStructType().fields().get(0).type()
          == Types.IntegerType.get();
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(1)
          .name()
          .equalsIgnoreCase("b");
      assert table.schema().columns().get(0).type().asStructType().fields().get(1).type()
          == Types.IntegerType.get();
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(2)
          .name()
          .equalsIgnoreCase("c");
      assert table.schema().columns().get(0).type().asStructType().fields().get(2).type()
          == Types.IntegerType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testChangeComplexStructDuplicateFields() throws Exception {
    final String newTblName = "changeTableStructDupFields";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1 ROW(x  int))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s modify column col1 col1 ROW(x  bigint, X double)",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(query, "PARSE ERROR: Column [X] specified multiple times.");
      Table table = getIcebergTable(file);
      assert table.schema().columns().size() == 1;
      assert table.schema().columns().get(0).name().equalsIgnoreCase("col1");
      assert table.schema().columns().get(0).type().isStructType();
      assert table.schema().columns().get(0).type().asStructType().fields().size() == 1;
      assert table
          .schema()
          .columns()
          .get(0)
          .type()
          .asStructType()
          .fields()
          .get(0)
          .name()
          .equalsIgnoreCase("x");
      assert table.schema().columns().get(0).type().asStructType().fields().get(0).type()
          == Types.IntegerType.get();
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testChangeComplexListOfStructDuplicateFields() throws Exception {
    final String newTblName = "changeTableListOfStructDupFields";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1  ARRAY(ROW(x  int)))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s modify column col1 col1  ARRAY(ROW(y  bigint, Y double))",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(query, "PARSE ERROR: Column [Y] specified multiple times.");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testChangeComplexListOfListOfStructDuplicateFields() throws Exception {
    final String newTblName = "changeTableListOfListOfStructDupFields";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1  ARRAY( ARRAY(ROW(x  int))))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s modify column col1 col1  ARRAY( ARRAY(ROW(y  bigint, Y double)))",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(query, "PARSE ERROR: Column [Y] specified multiple times.");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testChangeComplexStructOfListOfStructDuplicateFields() throws Exception {
    final String newTblName = "changeTableStructOfListOfStructDupFields";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format(
              "create table %s.%s (col1 ROW(x   ARRAY(ROW(x  int))))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s modify column col1 col1 ROW(x   ARRAY(ROW(x  int, y  bigint, Y double)))",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(query, "PARSE ERROR: Column [Y] specified multiple times.");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  @Test
  public void testChangeComplexStructOfStructDuplicateFields() throws Exception {
    final String newTblName = "changeTableStructOfStructDupFields";
    File file = new File(getDfsTestTmpSchemaLocation(), newTblName);
    try {
      String query =
          String.format("create table %s.%s (col1 ROW(x ROW(x  int)))", TEMP_SCHEMA, newTblName);
      test(query);
      query =
          String.format(
              "alter table %s.%s modify column col1 col1 ROW(x  ROW(x  int, y  bigint, Y double))",
              TEMP_SCHEMA, newTblName);
      errorMsgTestHelper(query, "PARSE ERROR: Column [Y] specified multiple times.");
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }
}
