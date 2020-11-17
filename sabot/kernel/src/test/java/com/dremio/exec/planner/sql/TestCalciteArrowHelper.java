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
package com.dremio.exec.planner.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.junit.Test;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableSet;

/**
 * Tests for methods in CalciteArrowHelper.
 */
public class TestCalciteArrowHelper {
  private static final Field UTF8_FIELD = Field.nullable("utf8_field", ArrowType.Utf8.INSTANCE);
  private static final Field NULL_FIELD = Field.nullable("null_field", ArrowType.Null.INSTANCE);

  private static final ImmutableSet<String> blackList = ImmutableSet.of("utf8_field");

  private final static BatchSchema schema = BatchSchema.newBuilder()
    .addField(UTF8_FIELD)
    .addField(NULL_FIELD)
    .build();

  @Test
  public void testToCalciteRecordRowTypeWithoutFiltering() {
    // Get actual results.
    RelDataType actual = CalciteArrowHelper.wrap(schema).toCalciteRecordType(
      SqlTypeFactoryImpl.INSTANCE, null, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

    // Create expected results.
    RelDataTypeFactory.FieldInfoBuilder expected =
      new RelDataTypeFactory.FieldInfoBuilder(SqlTypeFactoryImpl.INSTANCE);
    expected.add(UTF8_FIELD.getName(),
      CalciteArrowHelper.toCalciteType(UTF8_FIELD, SqlTypeFactoryImpl.INSTANCE, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal()));
    expected.add(NULL_FIELD.getName(),
      CalciteArrowHelper.toCalciteType(NULL_FIELD, SqlTypeFactoryImpl.INSTANCE, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal()));

    assertThat(actual, equalTo(expected.build()));
  }

  @Test
  public void testToCalciteRecordRowTypeWithTypeIdFiltering() {
    // Get actual results.
    RelDataType actual = CalciteArrowHelper.wrap(schema).toCalciteRecordType(
      SqlTypeFactoryImpl.INSTANCE,
      (Field f) -> (f.getType().getTypeID() != ArrowType.Null.TYPE_TYPE), PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

    // Create expected results.
    RelDataTypeFactory.FieldInfoBuilder expected =
      new RelDataTypeFactory.FieldInfoBuilder(SqlTypeFactoryImpl.INSTANCE);
    expected.add(UTF8_FIELD.getName(),
      CalciteArrowHelper.toCalciteType(UTF8_FIELD, SqlTypeFactoryImpl.INSTANCE, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal()));

    assertThat(actual, equalTo(expected.build()));
  }

  @Test
  public void testToCalciteRecordRowTypeWithNameFiltering() {
    // Get actual results.
    RelDataType actual = CalciteArrowHelper.wrap(schema).toCalciteRecordType(
      SqlTypeFactoryImpl.INSTANCE,
      (Field f) -> !(blackList.contains(f.getName())), PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

    // Create expected results.
    RelDataTypeFactory.FieldInfoBuilder expected =
      new RelDataTypeFactory.FieldInfoBuilder(SqlTypeFactoryImpl.INSTANCE);
    expected.add(NULL_FIELD.getName(),
      CalciteArrowHelper.toCalciteType(NULL_FIELD, SqlTypeFactoryImpl.INSTANCE, PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal()));

    assertThat(actual, equalTo(expected.build()));
  }
}
