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
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SearchableBatchSchema;
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

  @Test
  public void testComparePrimitiveField() {
    Assert.assertTrue(BatchSchema.of(new Field("string_field", FieldType.nullable(ArrowType.Utf8.INSTANCE), null))
      .equalsTypesWithoutPositions(BatchSchema.of(new Field("string_field", FieldType.nullable(ArrowType.Utf8.INSTANCE), null))));

    Assert.assertFalse(BatchSchema.of(new Field("col1", FieldType.nullable(new ArrowType.Int(32, false)), null))
      .equalsTypesWithoutPositions(BatchSchema.of(new Field("col1", FieldType.nullable(ArrowType.Utf8.INSTANCE), null))));
  }

  @Test
  public void testCompareComplexNestedFields() {
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

    List<Field> list_struct_child_src = new ArrayList<>();
    List<Field> list_struct_child_tgt = new ArrayList<>();

    list_struct_child_src.add(new Field("$data$", FieldType.nullable(
      new ArrowType.Struct()), fields));
    Collections.shuffle(fields);
    list_struct_child_tgt.add(new Field("$data$", FieldType.nullable(
      new ArrowType.Struct()), fields));

    Field list_struct_field_src = new Field("list_struct_field", FieldType.nullable(
      new ArrowType.List()), list_struct_child_src);
    Field list_struct_field_tgt = new Field("list_struct_field", FieldType.nullable(
      new ArrowType.List()), list_struct_child_tgt);

    Assert.assertTrue(BatchSchema.of(list_struct_field_src).equalsTypesWithoutPositions(BatchSchema.of(list_struct_field_tgt)));

    List<Field> struct_list_child_src = new ArrayList<>();
    struct_list_child_src.add(list_struct_field_src);
    List<Field> struct_list_child_tgt = new ArrayList<>();
    struct_list_child_tgt.add(list_struct_field_tgt);
    Field struct_list_field_src = new Field("struct_list_field", FieldType.nullable(
      new ArrowType.Struct()), struct_list_child_src);
    Field struct_list_field_tgt = new Field("struct_list_field", FieldType.nullable(
      new ArrowType.Struct()), struct_list_child_tgt);

    Assert.assertTrue(BatchSchema.of(struct_list_field_src).equalsTypesWithoutPositions(BatchSchema.of(struct_list_field_tgt)));
  }


  @Test
  public void testCompareComplexNestedFieldsFailureTypeChange() {
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

    List<Field> list_struct_child_src = new ArrayList<>();
    List<Field> list_struct_child_tgt = new ArrayList<>();

    list_struct_child_src.add(new Field("$data$", FieldType.nullable(
      new ArrowType.Struct()), fields));
    // change type of field 0 from string to int. this should cause comparison to fail
    fields.set(0, new Field("string_field", FieldType.nullable(new ArrowType.Int(32, true)), null));
    Collections.shuffle(fields);
    list_struct_child_tgt.add(new Field("$data$", FieldType.nullable(
      new ArrowType.Struct()), fields));

    Field list_struct_field_src = new Field("list_struct_field", FieldType.nullable(
      new ArrowType.List()), list_struct_child_src);
    Field list_struct_field_tgt = new Field("list_struct_field", FieldType.nullable(
      new ArrowType.List()), list_struct_child_tgt);

    Assert.assertFalse(BatchSchema.of(list_struct_field_src).equalsTypesWithoutPositions(BatchSchema.of(list_struct_field_tgt)));

    List<Field> struct_list_child_src = new ArrayList<>();
    struct_list_child_src.add(list_struct_field_src);
    List<Field> struct_list_child_tgt = new ArrayList<>();
    struct_list_child_tgt.add(list_struct_field_tgt);
    Field struct_list_field_src = new Field("struct_list_field", FieldType.nullable(
      new ArrowType.Struct()), struct_list_child_src);
    Field struct_list_field_tgt = new Field("struct_list_field", FieldType.nullable(
      new ArrowType.Struct()), struct_list_child_tgt);

    Assert.assertFalse(BatchSchema.of(struct_list_field_src).equalsTypesWithoutPositions(BatchSchema.of(struct_list_field_tgt)));
  }

  @Test
  public void testCompareComplexNestedFieldsFailure() {
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

    List<Field> list_struct_child_src = new ArrayList<>();
    List<Field> list_struct_child_tgt = new ArrayList<>();

    list_struct_child_src.add(new Field("$data$", FieldType.nullable(
      new ArrowType.Struct()), fields));
    Collections.shuffle(fields);
    // add one extra field to the child list. this should cause comparison to fail
    fields.add(new Field("int_field_2", FieldType.nullable(
      new ArrowType.Int(32, true)), null));
    list_struct_child_tgt.add(new Field("$data$", FieldType.nullable(
      new ArrowType.Struct()), fields));

    Field list_struct_field_src = new Field("list_struct_field", FieldType.nullable(
      new ArrowType.List()), list_struct_child_src);
    Field list_struct_field_tgt = new Field("list_struct_field", FieldType.nullable(
      new ArrowType.List()), list_struct_child_tgt);

    Assert.assertFalse(BatchSchema.of(list_struct_field_src).equalsTypesWithoutPositions(BatchSchema.of(list_struct_field_tgt)));

    List<Field> struct_list_child_src = new ArrayList<>();
    struct_list_child_src.add(list_struct_field_src);
    List<Field> struct_list_child_tgt = new ArrayList<>();
    struct_list_child_tgt.add(list_struct_field_tgt);
    Field struct_list_field_src = new Field("struct_list_field", FieldType.nullable(
      new ArrowType.Struct()), struct_list_child_src);
    Field struct_list_field_tgt = new Field("struct_list_field", FieldType.nullable(
      new ArrowType.Struct()), struct_list_child_tgt);

    Assert.assertFalse(BatchSchema.of(struct_list_field_src).equalsTypesWithoutPositions(BatchSchema.of(struct_list_field_tgt)));
  }

  @Test
  public void testCompareUnionFields() {
    List<Field> union_children = new ArrayList<>();
    List<Field> rev_union_children = new ArrayList<>();

    union_children.add(new Field("string_field", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
    union_children.add(new Field("int_field", FieldType.nullable(new ArrowType.Int(32, false)), null));

    rev_union_children.add(new Field("int_field", FieldType.nullable(new ArrowType.Int(32, false)), null));
    rev_union_children.add(new Field("string_field", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));

    int[] typeIds = new int[2];
    typeIds[0] = Types.getMinorTypeForArrowType(union_children.get(0).getType()).ordinal();
    typeIds[1] = Types.getMinorTypeForArrowType(union_children.get(1).getType()).ordinal();

    int[] revTypeIds = new int[2];
    revTypeIds[0] = Types.getMinorTypeForArrowType(rev_union_children.get(0).getType()).ordinal();
    revTypeIds[1] = Types.getMinorTypeForArrowType(rev_union_children.get(1).getType()).ordinal();

    Assert.assertTrue(BatchSchema.of(new Field("union_field", FieldType.nullable(new ArrowType.Union(UnionMode.Sparse, typeIds)), union_children))
      .equalsTypesWithoutPositions(BatchSchema.of(new Field("union_field", FieldType.nullable(new ArrowType.Union(UnionMode.Sparse, revTypeIds)), rev_union_children))));
  }

  @Test
  public void testFindField() {
    int columns = 1000000;
    String fieldPrefix = "field_name_";
    List<Field> fields = new ArrayList<>();
    for (int c = 0; c < columns; ++c) {
      fields.add(new Field(fieldPrefix + c, FieldType.nullable(new ArrowType.Int(32, false)), null));
    }

    BatchSchema schema = BatchSchema.of(fields.toArray(new Field[0]));
    SearchableBatchSchema searchableBatchSchema = SearchableBatchSchema.of(schema);

    for (int c = 0; c < columns; ++c) {
      String fieldName = fieldPrefix + c;
      Optional<Field> field = searchableBatchSchema.findFieldIgnoreCase(fieldName);
      Assert.assertTrue(field.isPresent());
      Assert.assertNotNull(field.get());
    }

    String fieldName = fieldPrefix + "non_existent";
    Optional<Field> field = searchableBatchSchema.findFieldIgnoreCase(fieldName);
    Assert.assertFalse(field.isPresent());
  }
}
