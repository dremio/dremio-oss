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
package com.dremio.exec.record;

import static com.dremio.common.expression.CompleteType.LIST;
import static com.dremio.common.expression.CompleteType.STRUCT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

import com.dremio.common.expression.CompleteType;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link BatchSchema}
 */
public class TestBatchSchema {

  @Test
  public void testBatchSchemaDropColumnSimple() throws Exception {
    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField")
      );

    BatchSchema newSchema = tableSchema.dropFields(ImmutableList.of(
      ImmutableList.of("integerCol")));

    assertEquals(newSchema.getFields().size(), 3);
  }

  @Test
  public void testBatchSchemaDropColumnComplex() throws Exception {
    List<Field> childrenField = ImmutableList.of(CompleteType.INT.toField("integerCol"),
      CompleteType.DOUBLE.toField("doubleCol"));

    Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()),
      childrenField);


    Field listField = new Field("listField", FieldType.nullable(LIST.getType()), childrenField);

    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField"),
        structField,
        listField
      );

    BatchSchema newSchema = tableSchema.dropFields(ImmutableList.of(
      ImmutableList.of("structField"),
      ImmutableList.of("listField")));

    assertEquals(newSchema.getFields().size(), 4);
  }

  @Test
  public void testBatchSchemaDropColumnStruct() throws Exception {
    List<Field> childrenField = ImmutableList.of(CompleteType.INT.toField("integerCol"),
      CompleteType.DOUBLE.toField("doubleCol"));

    Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()),
      childrenField);

    BatchSchema tableSchema =
      BatchSchema.of(structField);

    BatchSchema newSchema = tableSchema.dropFields(ImmutableList.of(
      ImmutableList.of("structField", "integerCol")));

    assertEquals(newSchema.toJSONString(), "{\"name\":\"root\",\"children\":[{\"name\":\"structField\",\"type\":\"Struct\",\"children\":[{\"name\":\"doubleCol\",\"type\":\"FloatingPoint\"}]}]}");
  }

  @Test
  public void testBatchSchemaDropColumnStructInStruct() throws Exception {
    List<Field> childrenField = ImmutableList.of(CompleteType.INT.toField("integerCol"),
      CompleteType.DOUBLE.toField("doubleCol"));

    Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()),
      childrenField);

    Field structStructField = new Field("outerStructField", FieldType.nullable(STRUCT.getType()),
      ImmutableList.of(structField));

    BatchSchema tableSchema =
      BatchSchema.of(structStructField);

    BatchSchema newSchema = tableSchema.dropFields(ImmutableList.of(
      ImmutableList.of("outerStructField", "structField", "integerCol")));

    assertEquals(newSchema.toJSONString(), "{\"name\":\"root\",\"children\":[{\"name\":\"outerStructField\",\"type\":\"Struct\",\"children\":[{\"name\":\"structField\",\"type\":\"Struct\",\"children\":[{\"name\":\"doubleCol\",\"type\":\"FloatingPoint\"}]}]}]}");
  }

  @Test
  public void testBatchSchemaDropColumnStructInList() throws Exception {
    List<Field> childrenField = ImmutableList.of(CompleteType.INT.toField("integerCol"),
      CompleteType.DOUBLE.toField("doubleCol"));

    Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField);
    Field listStruct = new Field("listStruct", FieldType.nullable(LIST.getType()), ImmutableList.of(structField));

    BatchSchema tableSchema =
      BatchSchema.of(listStruct);

    //Drop one field from the struct inside the list
    BatchSchema newSchema1 = tableSchema.dropFields(ImmutableList.of(
      ImmutableList.of("listStruct", "structField", "integerCol")));

    assertEquals(newSchema1.toJSONString(), "{\"name\":\"root\",\"children\":[{\"name\":\"listStruct\",\"type\":\"List\",\"children\":[{\"name\":\"structField\",\"type\":\"Struct\",\"children\":[{\"name\":\"doubleCol\",\"type\":\"FloatingPoint\"}]}]}]}");
  }

  @Test
  public void testBatchSchemaUpdateTypeOfColumns() {
    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField")
      );
    BatchSchema newSchema = tableSchema.changeType(CompleteType.BIGINT.toField("integerCol"));
    assertEquals(newSchema.getFields().size(), 4);
    assertEquals(newSchema.getColumn(0), CompleteType.BIGINT.toField("integerCol"));
  }

  @Test
  public void testBatchSchemaUpdateTypeOfStructColumns() {
    List<Field> childrenField = ImmutableList.of(CompleteType.INT.toField("integerCol"),
      CompleteType.DOUBLE.toField("doubleCol"));

    Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField);

    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField"),
        structField
      );

    List<Field> newChildren = ImmutableList.of(CompleteType.BIGINT.toField("integerCol"),
      CompleteType.DOUBLE.toField("doubleCol"));

    Field newStructField = new Field("structField", FieldType.nullable(STRUCT.getType()), newChildren);

    BatchSchema newSchema = tableSchema.changeType(newStructField);
    assertEquals(newSchema.getFields().size(), 5);
    assertEquals(newSchema.getColumn(4).getChildren().get(0), CompleteType.BIGINT.toField("integerCol"));
  }

  @Test
  public void testBatchSchemaDropColumnsTypes() {
    List<Field> childrenField = ImmutableList.of(CompleteType.INT.toField("integerCol"),
      CompleteType.DOUBLE.toField("doubleCol"));

    Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField);
    Field listField = new Field("listField", FieldType.nullable(LIST.getType()), childrenField);

    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        structField,
        listField,
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField")
      );

    BatchSchema newSchema1 = tableSchema.dropField("integerCol");
    assertEquals(newSchema1.getFields().size(), 5);

    //First column should not be an integerCol
    assertTrue(newSchema1.getColumn(0).getName() != "integerCol");

    BatchSchema newSchema2 = tableSchema.dropField("structField");
    assertEquals(newSchema1.getFields().size(), 5);
    //second column should not be a struct column
    assertTrue(newSchema1.getColumn(1).getName() != "structField");


    BatchSchema newSchema3 = tableSchema.dropField("listField");
    assertEquals(newSchema3.getFields().size(), 5);
    //third column should not be a struct column
    assertTrue(newSchema1.getColumn(2).getName() != "listField");
  }

  @Test
  public void testBatchSchemaUpdateColumns() {
    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField")
      );

    BatchSchema newSchema1 = tableSchema.changeType(CompleteType.BIGINT.toField("integercol"));
    assertEquals(newSchema1.getFields().size(), 4);
    //First column should not be an integerCol
    assertEquals(newSchema1.getColumn(0), CompleteType.BIGINT.toField("integercol"));
  }

  @Test
  public void testBatchSchemaDropColumnsTypesIgnoreCase() {
    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField")
      );

    BatchSchema newSchema1 = tableSchema.dropField("integercol");
    assertEquals(newSchema1.getFields().size(), 3);
    //First column should not be an integerCol
    assertTrue(newSchema1.getColumn(0).getName() != "integerCol");
  }

  @Test
  public void testBatchSchemaDropNonExistingFields() {
    List<Field> childrenField = ImmutableList.of(CompleteType.INT.toField("integerCol"),
      CompleteType.DOUBLE.toField("doubleCol"));

    Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField);

    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField"),
        structField
      );

    Field primitiveField = CompleteType.INT.toField("tempCol");
    BatchSchema newSchema1 = tableSchema.dropField(primitiveField);
    assertEquals(newSchema1.getFields().size(), 5);

    Field structFieldDrop = new Field("structField", FieldType.nullable(STRUCT.getType()),  ImmutableList.of(CompleteType.INT.toField("lolCol")));
    BatchSchema newSchema2 = tableSchema.dropField(structFieldDrop);

    assertEquals(newSchema2.getFields().size(), 5);
    assertEquals(newSchema2.getFields().get(4), structField);
  }

  @Test
  public void testBatchSchemaDropField() throws Exception {
    List<Field> childrenField = ImmutableList.of(CompleteType.INT.toField("integerCol"),
      CompleteType.DOUBLE.toField("doubleCol"));

    Field structField = new Field("structField", FieldType.nullable(STRUCT.getType()), childrenField);

    BatchSchema tableSchema =
      BatchSchema.of(
        CompleteType.INT.toField("integerCol"),
        CompleteType.DOUBLE.toField("doubleCol"),
        CompleteType.BIT.toField("bitField"),
        CompleteType.VARCHAR.toField("varCharField"),
        structField
      );

    Field structFieldDrop = new Field("structField", FieldType.nullable(STRUCT.getType()),  ImmutableList.of(CompleteType.INT.toField("integerCol")));
    BatchSchema newSchema1 = tableSchema.dropField(structFieldDrop);

    assertEquals(newSchema1.getFields().size(), 5);
    assertEquals(newSchema1.toJSONString(), "{\"name\":\"root\",\"children\":[{\"name\":\"integerCol\",\"type\":\"Int\"},{\"name\":\"doubleCol\",\"type\":\"FloatingPoint\"},{\"name\":\"bitField\",\"type\":\"Bool\"},{\"name\":\"varCharField\",\"type\":\"Utf8\"},{\"name\":\"structField\",\"type\":\"Struct\",\"children\":[{\"name\":\"doubleCol\",\"type\":\"FloatingPoint\"}]}]}");
  }
}
