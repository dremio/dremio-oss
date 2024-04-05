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

import static com.dremio.common.expression.CompleteType.LIST;
import static com.dremio.common.expression.CompleteType.STRUCT;
import static org.junit.Assert.assertEquals;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

public class BatchSchemaDifferTest {

  /**
   * schema1 -> { integerCol : int, doubleCol: double, bitField: bit, varCharField: varchar },
   *
   * <p>schema2 -> { integerCol : int, bit: varchar, varCharField: varchar }
   */
  @Test
  public void testBatchSchemaDifferSimpleColumns() {
    BatchSchema tableSchema1 =
        BatchSchema.of(
            CompleteType.INT.toField("integerCol"),
            CompleteType.DOUBLE.toField("doubleCol"),
            CompleteType.BIT.toField("bitField"),
            CompleteType.VARCHAR.toField("varCharField"));

    BatchSchema tableSchema2 =
        BatchSchema.of(
            CompleteType.INT.toField("integerCol"),
            CompleteType.VARCHAR.toField("bitField"),
            CompleteType.VARCHAR.toField("varCharField"));

    BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
    BatchSchemaDiff diff =
        batchSchemaDiffer.diff(tableSchema1.getFields(), tableSchema2.getFields());

    assertEquals(diff.getAddedFields().size(), 0);
    assertEquals(
        diff.getModifiedFields(), ImmutableList.of(CompleteType.VARCHAR.toField("bitField")));
    assertEquals(
        diff.getDroppedFields(), ImmutableList.of(CompleteType.DOUBLE.toField("doubleCol")));
  }

  /**
   * schema1 -> { struct a { integerCol : int, doubleCol: double }, integerCol: int }
   *
   * <p>schema2 -> { struct a { doubleCol: varchar }, integerCol: int }
   */
  @Test
  public void testBatchSchemaDifferStruct() {
    List<Field> childrenField1 =
        ImmutableList.of(
            CompleteType.INT.toField("integerCol"), CompleteType.DOUBLE.toField("doubleCol"));

    Field structField1 = new Field("a", FieldType.nullable(STRUCT.getType()), childrenField1);

    BatchSchema tableSchema1 = BatchSchema.of(CompleteType.INT.toField("integerCol"), structField1);

    List<Field> childrenField2 = ImmutableList.of(CompleteType.VARCHAR.toField("doubleCol"));

    Field structField2 = new Field("a", FieldType.nullable(STRUCT.getType()), childrenField2);

    BatchSchema tableSchema2 = BatchSchema.of(CompleteType.INT.toField("integerCol"), structField2);

    BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
    BatchSchemaDiff diff =
        batchSchemaDiffer.diff(tableSchema1.getFields(), tableSchema2.getFields());

    assertEquals(diff.getAddedFields().size(), 0);
    assertEquals(diff.getModifiedFields(), ImmutableList.of(structField2));

    Field assertField =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(CompleteType.INT.toField("integerCol")));
    assertEquals(diff.getDroppedFields(), ImmutableList.of(assertField));
  }

  /**
   * schema1 -> { struct a { struct b { id : int, col: double }, c: int }, integerCol: int }
   *
   * <p>schema2 -> { struct a { c: double }, integerCol: int }
   */
  @Test
  public void testBatchSchemaDifferStructInsideStructChange() {
    List<Field> childrenField1 =
        ImmutableList.of(CompleteType.INT.toField("id"), CompleteType.DOUBLE.toField("col"));

    Field structField1 = new Field("b", FieldType.nullable(STRUCT.getType()), childrenField1);

    Field structField2 =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(structField1, CompleteType.INT.toField("c")));

    BatchSchema tableSchema1 = BatchSchema.of(structField2, CompleteType.INT.toField("integerCol"));

    Field structFieldNew1 =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(CompleteType.DOUBLE.toField("c")));

    BatchSchema tableSchema2 =
        BatchSchema.of(structFieldNew1, CompleteType.INT.toField("integerCol"));

    BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
    BatchSchemaDiff diff =
        batchSchemaDiffer.diff(tableSchema1.getFields(), tableSchema2.getFields());

    assertEquals(diff.getAddedFields().size(), 0);

    Field assertField1 =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(CompleteType.DOUBLE.toField("c")));

    Field assertField2 =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(
                new Field(
                    "b",
                    FieldType.nullable(STRUCT.getType()),
                    ImmutableList.of(CompleteType.INT.toField("id")))));

    Field assertField3 =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(
                new Field(
                    "b",
                    FieldType.nullable(STRUCT.getType()),
                    ImmutableList.of(CompleteType.DOUBLE.toField("col")))));

    assertEquals(diff.getModifiedFields(), ImmutableList.of(assertField1));
    assertEquals(diff.getDroppedFields(), ImmutableList.of(assertField2, assertField3));
  }

  /**
   * schema1 -> { struct a { struct b { id : int, col: double }, c: int }, integerCol: int }
   *
   * <p>schema2 -> { struct a { struct b { id: int, col: varchar } }, }
   */
  @Test
  public void testBatchSchemaDifferStructInsideStruct() {
    List<Field> childrenField1 =
        ImmutableList.of(CompleteType.INT.toField("id"), CompleteType.DOUBLE.toField("col"));

    Field structField1 = new Field("b", FieldType.nullable(STRUCT.getType()), childrenField1);

    Field structField2 =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(structField1, CompleteType.INT.toField("c")));

    BatchSchema tableSchema1 = BatchSchema.of(structField2, CompleteType.INT.toField("integerCol"));

    List<Field> childrenField2 =
        ImmutableList.of(CompleteType.INT.toField("id"), CompleteType.VARCHAR.toField("col"));

    Field structFieldNew1 = new Field("b", FieldType.nullable(STRUCT.getType()), childrenField2);

    Field structFieldNew2 =
        new Field("a", FieldType.nullable(STRUCT.getType()), ImmutableList.of(structFieldNew1));

    BatchSchema tableSchema2 = BatchSchema.of(structFieldNew2);

    BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
    BatchSchemaDiff diff =
        batchSchemaDiffer.diff(tableSchema1.getFields(), tableSchema2.getFields());

    assertEquals(diff.getAddedFields().size(), 0);

    Field assertField1 =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(
                new Field(
                    "b",
                    FieldType.nullable(STRUCT.getType()),
                    ImmutableList.of(CompleteType.VARCHAR.toField("col")))));

    Field assertField2 =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(CompleteType.INT.toField("c")));

    Field assertField4 = CompleteType.INT.toField("integerCol");

    assertEquals(diff.getModifiedFields(), ImmutableList.of(assertField1));
    assertEquals(diff.getDroppedFields(), ImmutableList.of(assertField4, assertField2));
  }

  @Test
  public void testBatchSchemaDifferList() {
    List<Field> childrenField1 = ImmutableList.of(CompleteType.INT.toField("integerCol"));

    Field listField1 = new Field("listField", FieldType.nullable(LIST.getType()), childrenField1);

    BatchSchema tableSchema1 =
        BatchSchema.of(
            CompleteType.INT.toField("integerCol"),
            CompleteType.BIT.toField("bitField"),
            CompleteType.VARCHAR.toField("varCharField"),
            listField1);

    List<Field> childrenField2 = ImmutableList.of(CompleteType.DOUBLE.toField("integerCol"));
    Field listField2 = new Field("listField", FieldType.nullable(LIST.getType()), childrenField2);

    BatchSchema tableSchema2 =
        BatchSchema.of(
            CompleteType.INT.toField("integerCol"),
            CompleteType.BIT.toField("bitField"),
            CompleteType.VARCHAR.toField("varCharField"),
            listField2);

    BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
    BatchSchemaDiff diff =
        batchSchemaDiffer.diff(tableSchema1.getFields(), tableSchema2.getFields());

    assertEquals(diff.getAddedFields().size(), 0);
    assertEquals(diff.getModifiedFields(), ImmutableList.of(listField2));
    assertEquals(diff.getDroppedFields().size(), 0);
  }

  @Test
  public void testBatchSchemaDifferListOfStruct() {
    List<Field> childrenField1 =
        ImmutableList.of(
            CompleteType.INT.toField("integerCol"), CompleteType.DOUBLE.toField("doubleCol"));

    Field structField1 =
        new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField1);
    Field listStruct1 =
        new Field("listStruct", FieldType.nullable(LIST.getType()), ImmutableList.of(structField1));

    BatchSchema tableSchema1 = BatchSchema.of(listStruct1);

    List<Field> childrenField2 = ImmutableList.of(CompleteType.DOUBLE.toField("integerCol"));

    Field structField2 =
        new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField2);
    Field listStruct2 =
        new Field("listStruct", FieldType.nullable(LIST.getType()), ImmutableList.of(structField2));

    BatchSchema tableSchema2 = BatchSchema.of(listStruct2);

    BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
    BatchSchemaDiff diff =
        batchSchemaDiffer.diff(tableSchema1.getFields(), tableSchema2.getFields());

    assertEquals(diff.getAddedFields().size(), 0);

    Field assertField1 =
        new Field(
            "listStruct",
            FieldType.nullable(LIST.getType()),
            ImmutableList.of(
                new Field(
                    "structField",
                    FieldType.nullable(STRUCT.getType()),
                    ImmutableList.of(CompleteType.DOUBLE.toField("integerCol")))));

    Field assertField2 =
        new Field(
            "listStruct",
            FieldType.nullable(LIST.getType()),
            ImmutableList.of(
                new Field(
                    "structField",
                    FieldType.nullable(STRUCT.getType()),
                    ImmutableList.of(CompleteType.DOUBLE.toField("doubleCol")))));

    assertEquals(diff.getModifiedFields(), ImmutableList.of(assertField1));
    assertEquals(diff.getDroppedFields(), ImmutableList.of(assertField2));
  }

  @Test
  public void testBatchSchemaDifferDropTopLevelStruct() {
    List<Field> childrenField1 =
        ImmutableList.of(
            CompleteType.INT.toField("integerCol"), CompleteType.DOUBLE.toField("doubleCol"));

    Field structField1 =
        new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField1);

    BatchSchema tableSchema1 = BatchSchema.of(CompleteType.INT.toField("integerCol"), structField1);

    BatchSchema tableSchema2 = BatchSchema.of(CompleteType.INT.toField("integerCol"));

    BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
    BatchSchemaDiff diff =
        batchSchemaDiffer.diff(tableSchema1.getFields(), tableSchema2.getFields());

    assertEquals(diff.getAddedFields().size(), 0);
    assertEquals(diff.getModifiedFields().size(), 0);
    assertEquals(diff.getDroppedFields().size(), 2);

    Field assertField1 =
        new Field(
            "structField",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(CompleteType.INT.toField("integerCol")));

    Field assertField2 =
        new Field(
            "structField",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(CompleteType.DOUBLE.toField("doubleCol")));

    assertEquals(diff.getDroppedFields(), ImmutableList.of(assertField1, assertField2));
  }
}
