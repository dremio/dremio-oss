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
package com.dremio.exec.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.test.DremioTest;

public class BatchSchemaFieldTest extends DremioTest {
  @Test
  public void testFromFieldWithPrimitiveTypes() {
    List<Field> fields = new ArrayList<>();
    List<String> expectedType = new ArrayList<>();
    fields.add(new Field("string_field", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    expectedType.add("string_field: VARCHAR");

    fields.add(new Field("int_field", FieldType.nullable(
      new ArrowType.Int(32, true)), null));
    expectedType.add("int_field: INTEGER");

    fields.add(new Field("bigint_field", FieldType.nullable(
      new ArrowType.Int(64, true)), null));
    expectedType.add("bigint_field: BIGINT");

    fields.add(new Field("float_field", FieldType.nullable(
      new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
    expectedType.add("float_field: FLOAT");

    fields.add(new Field("double_field", FieldType.nullable(
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
    expectedType.add("double_field: DOUBLE");

    fields.add(new Field("decimal_field", FieldType.nullable(
      new ArrowType.Decimal(10,5)), null));
    expectedType.add("decimal_field: DECIMAL");

    Assert.assertEquals(fields.size(), expectedType.size());
    for(int pos = 0; pos < fields.size(); ++pos) {
      Assert.assertEquals(expectedType.get(pos), BatchSchemaField.fromField(fields.get(pos)).toString());
    }
  }

  @Test
  public void testFromFieldWithListTypes() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("$data$", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    Field list_field = new Field("list_field", FieldType.nullable(
      new ArrowType.List()), fields);
    String expected = "list_field: LIST<$data$: VARCHAR>";
    Assert.assertEquals(expected, BatchSchemaField.fromField(list_field).toString());

    fields.clear();
    fields.add(new Field("$data$", FieldType.nullable(
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
    list_field = new Field("list_field", FieldType.nullable(
      new ArrowType.List()), fields);
    expected = "list_field: LIST<$data$: DOUBLE>";
    Assert.assertEquals(expected, BatchSchemaField.fromField(list_field).toString());
  }

  @Test
  public void testFromFieldWithStructTypes() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("string_field", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    fields.add(new Field("int_field", FieldType.nullable(
      new ArrowType.Int(32, true)), null));
    fields.add(new Field("bigint_field", FieldType.nullable(
      new ArrowType.Int(64, true)), null));
    fields.add(new Field("float_field", FieldType.nullable(
      new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
    fields.add(new Field("double_field", FieldType.nullable(
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
    fields.add(new Field("decimal_field", FieldType.nullable(
      new ArrowType.Decimal(10,5)), null));

    Field struct_field = new Field("struct_field", FieldType.nullable(
      new ArrowType.Struct()), fields);

    String expected = "struct_field: STRUCT<string_field: VARCHAR, " +
      "int_field: INTEGER, bigint_field: BIGINT, float_field: FLOAT, " +
      "double_field: DOUBLE, decimal_field: DECIMAL>";
    Assert.assertEquals(expected, BatchSchemaField.fromField(struct_field).toString());
  }

  @Test
  public void testFromFieldWithNestedTypes() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("string_field", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    fields.add(new Field("int_field", FieldType.nullable(
      new ArrowType.Int(32, true)), null));
    fields.add(new Field("bigint_field", FieldType.nullable(
      new ArrowType.Int(64, true)), null));
    fields.add(new Field("float_field", FieldType.nullable(
      new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
    fields.add(new Field("double_field", FieldType.nullable(
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
    fields.add(new Field("decimal_field", FieldType.nullable(
      new ArrowType.Decimal(10,5)), null));

    List<Field> list_struct_child = new ArrayList<>();
    list_struct_child.add(new Field("$data$", FieldType.nullable(
      new ArrowType.Struct()), fields));
    Field list_struct_field = new Field("list_struct_field", FieldType.nullable(
      new ArrowType.List()), list_struct_child);

    String list_struct_field_expected = "list_struct_field: LIST<" +
      "$data$: STRUCT<string_field: VARCHAR, " +
      "int_field: INTEGER, bigint_field: BIGINT, float_field: FLOAT, " +
      "double_field: DOUBLE, decimal_field: DECIMAL>>";
    Assert.assertEquals(list_struct_field_expected, BatchSchemaField.fromField(list_struct_field).toString());

    List<Field> struct_list_child = new ArrayList<>();
    struct_list_child.add(list_struct_field);
    Field struct_list_field = new Field("struct_list_field", FieldType.nullable(
      new ArrowType.Struct()), struct_list_child);
    String struct_list_field_expected = "struct_list_field: STRUCT<list_struct_field: LIST<" +
      "$data$: STRUCT<string_field: VARCHAR, " +
      "int_field: INTEGER, bigint_field: BIGINT, float_field: FLOAT, " +
      "double_field: DOUBLE, decimal_field: DECIMAL>>>";
    Assert.assertEquals(struct_list_field_expected, BatchSchemaField.fromField(struct_list_field).toString());
  }
}
