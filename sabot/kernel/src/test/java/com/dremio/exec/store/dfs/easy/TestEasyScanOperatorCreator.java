/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.dfs.easy;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.Lists;

public class TestEasyScanOperatorCreator {

  @Test
  public void trueWhenAllColumnsAreSelected() {
    BatchSchema schema = mock(BatchSchema.class);
    when(schema.iterator())
        .thenReturn(Lists.newArrayList(Field.nullable("a1", new ArrowType.Bool())).iterator());
    assertTrue(EasyScanOperatorCreator.selectsAllColumns(schema,
        Lists.<SchemaPath>newArrayList(SchemaPath.getSimplePath("a1"))));
  }

  @Test
  public void selectionIgnoresIncremental() {
    BatchSchema schema = mock(BatchSchema.class);
    when(schema.iterator())
        .thenReturn(Lists.newArrayList(Field.nullable("a1", new ArrowType.Bool()),
            Field.nullable(IncrementalUpdateUtils.UPDATE_COLUMN, new ArrowType.Bool())).iterator());
    assertTrue(EasyScanOperatorCreator.selectsAllColumns(schema,
      Lists.<SchemaPath>newArrayList(SchemaPath.getSimplePath("a1"))));
  }

  @Test
  public void falseWhenAllColumnsAreNotSelected() {
    BatchSchema schema = mock(BatchSchema.class);
    when(schema.iterator())
      .thenReturn(Lists.newArrayList(Field.nullable("a1", new ArrowType.Bool()),
          Field.nullable("a2", new ArrowType.Bool())).iterator());
    assertFalse(EasyScanOperatorCreator.selectsAllColumns(schema,
        Lists.<SchemaPath>newArrayList(SchemaPath.getSimplePath("a1"))));
  }

  @Test
  public void falseWhenChildrenAreSelected() {
    BatchSchema schema = mock(BatchSchema.class);
    when(schema.iterator())
      .thenReturn(Lists.newArrayList(
          new Field("a1", new FieldType(true, new ArrowType.Struct(), null),
              Lists.newArrayList(Field.nullable("a2", new ArrowType.Bool()))),
          Field.nullable("a3", new ArrowType.Bool())).iterator());
    assertFalse(EasyScanOperatorCreator.selectsAllColumns(schema,
      Lists.newArrayList(SchemaPath.getSimplePath("a1"), SchemaPath.getCompoundPath("a1", "a2"),
          SchemaPath.getSimplePath("a3"))));
  }

}
