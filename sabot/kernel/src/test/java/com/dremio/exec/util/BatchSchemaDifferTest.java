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

    assertEquals(0, diff.getAddedFields().size());
    assertEquals(
        ImmutableList.of(CompleteType.VARCHAR.toField("bitField")), diff.getModifiedFields());
    assertEquals(
        ImmutableList.of(CompleteType.DOUBLE.toField("doubleCol")), diff.getDroppedFields());
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

    assertEquals(0, diff.getAddedFields().size());
    assertEquals(ImmutableList.of(structField2), diff.getModifiedFields());

    Field assertField =
        new Field(
            "a",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(CompleteType.INT.toField("integerCol")));
    assertEquals(ImmutableList.of(assertField), diff.getDroppedFields());
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

    assertEquals(0, diff.getAddedFields().size());

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

    assertEquals(ImmutableList.of(assertField1), diff.getModifiedFields());
    assertEquals(ImmutableList.of(assertField2, assertField3), diff.getDroppedFields());
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

    assertEquals(0, diff.getAddedFields().size());

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

    assertEquals(ImmutableList.of(assertField1), diff.getModifiedFields());
    assertEquals(ImmutableList.of(assertField4, assertField2), diff.getDroppedFields());
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

    assertEquals(0, diff.getAddedFields().size());
    assertEquals(ImmutableList.of(listField2), diff.getModifiedFields());
    assertEquals(0, diff.getDroppedFields().size());
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

    assertEquals(0, diff.getAddedFields().size());

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

    assertEquals(ImmutableList.of(assertField1), diff.getModifiedFields());
    assertEquals(ImmutableList.of(assertField2), diff.getDroppedFields());
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

    assertEquals(0, diff.getAddedFields().size());
    assertEquals(0, diff.getModifiedFields().size());
    assertEquals(2, diff.getDroppedFields().size());

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

    assertEquals(ImmutableList.of(assertField1, assertField2), diff.getDroppedFields());
  }

  /**
   * schema 1 -> {intCol, struct outerStruct{nestedInt, listOfStruct [struct innerStruct{nestInt2A,
   * nestInt2B}]}}
   *
   * <p>schema 2 -> {intCol}
   */
  @Test
  public void testBatchSchemaDifferStructOfListOfStruct() {
    List<Field> innerChildren =
        ImmutableList.of(
            CompleteType.INT.toField("nestInt2A"), CompleteType.INT.toField("nestInt2B"));

    Field innerStruct =
        new Field("innerStruct", FieldType.nullable(STRUCT.getType()), innerChildren);

    Field nestedInt = CompleteType.INT.toField("nestedInt");
    Field listField =
        new Field(
            "listOfStruct", FieldType.nullable(LIST.getType()), ImmutableList.of(innerStruct));
    Field outerStruct =
        new Field(
            "outerStruct",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(listField, nestedInt));
    Field outerInt = CompleteType.INT.toField("intCol");

    BatchSchema oldSchema = BatchSchema.of(outerInt, outerStruct);
    BatchSchema newSchema = BatchSchema.of(outerInt);

    BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
    BatchSchemaDiff diff = batchSchemaDiffer.diff(oldSchema.getFields(), newSchema.getFields());

    Field assertField1 =
        new Field(
            "outerStruct",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(
                new Field(
                    "listOfStruct",
                    FieldType.nullable(LIST.getType()),
                    ImmutableList.of(
                        new Field(
                            "innerStruct",
                            FieldType.nullable(STRUCT.getType()),
                            List.of(innerChildren.get(0)))))));

    Field assertField2 =
        new Field(
            "outerStruct",
            FieldType.nullable(STRUCT.getType()),
            ImmutableList.of(
                new Field(
                    "listOfStruct",
                    FieldType.nullable(LIST.getType()),
                    ImmutableList.of(
                        new Field(
                            "innerStruct",
                            FieldType.nullable(STRUCT.getType()),
                            List.of(innerChildren.get(1)))))));

    Field assertField3 =
        new Field("outerStruct", FieldType.nullable(STRUCT.getType()), ImmutableList.of(nestedInt));

    assertEquals(3, diff.getDroppedFields().size());
    assertEquals(
        ImmutableList.of(assertField1, assertField2, assertField3), diff.getDroppedFields());
  }
}
