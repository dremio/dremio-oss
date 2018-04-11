/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

package com.dremio.exec.planner.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Tests aspects of {@link ValuesRel}
 */
public class TestValuesRel {

  @Mock
  private RelOptCluster cluster;

  private RelDataTypeFactory typeFactory;

  // Mockito cannot mock RelTraitSet as it is final. Add the required LOGICAL convention.
  private static final RelTraitSet traits = RelTraitSet.createEmpty().plus(Rel.LOGICAL);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    // Mock cluster - Handle getter for type factory.
    typeFactory = new JavaTypeFactoryImpl();
    when(cluster.getTypeFactory()).thenReturn(typeFactory);
  }

  // Test the row type adjustment, modelling the tuples structure of an IN list.
  @Test
  public void testNumericValuesRelRowTypeAdjustment() {
    final int INListLength = 20;

    // Build RowType & Tuples
    RelDataTypeField relDataType = new RelDataTypeFieldImpl("ROW_VALUE", 0, new BasicSqlType(RelDataTypeSystemImpl.DEFAULT, SqlTypeName.ANY));
    RelDataType rowType = new RelRecordType(StructKind.FULLY_QUALIFIED, Arrays.asList(relDataType));
    ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = new ImmutableList.Builder<>();
    for (int i = 0; i < INListLength; i++) {
      tuples.add(new ImmutableList.Builder<RexLiteral>().add(new RexBuilder(typeFactory).makeExactLiteral(new BigDecimal(i))).build());
    }

    // Check original types.
    assertEquals(1, rowType.getFieldCount());
    assertEquals(SqlTypeName.ANY, rowType.getFieldList().get(0).getType().getSqlTypeName());

    // Construct ValuesRel
    final ValuesRel valuesRel = new ValuesRel(cluster, rowType, tuples.build(), traits);

    // Check the adjusted types.
    RelDataType adjustedRowType = valuesRel.getRowType();
    assertEquals(1, adjustedRowType.getFieldCount());
    assertEquals(SqlTypeName.INTEGER, adjustedRowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  // Test the row type adjustment, modelling the tuples structure of an IN list.
  @Test
  public void testCharValuesRelRowTypeAdjustment() {
    final int INListLength = 20;

    // Build RowType & Tuples
    RelDataTypeField relDataType = new RelDataTypeFieldImpl("ROW_VALUE", 0, new BasicSqlType(RelDataTypeSystemImpl.DEFAULT, SqlTypeName.ANY));
    RelDataType rowType = new RelRecordType(StructKind.FULLY_QUALIFIED, Arrays.asList(relDataType));
    ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = new ImmutableList.Builder<>();
    for (int i = 0; i < INListLength; ++i) {
      tuples.add(new ImmutableList.Builder<RexLiteral>().add(new RexBuilder(typeFactory).makeLiteral(charLiteralBuilder(i))).build());
    }

    // Check original types.
    assertEquals(1, rowType.getFieldCount());
    assertEquals(SqlTypeName.ANY, rowType.getFieldList().get(0).getType().getSqlTypeName());

    // Construct ValuesRel
    final ValuesRel valuesRel = new ValuesRel(cluster, rowType, tuples.build(), traits);

    // Check the adjusted types.
    RelDataType adjustedRowType = valuesRel.getRowType();
    assertEquals(1, adjustedRowType.getFieldCount());
    assertEquals(SqlTypeName.CHAR, adjustedRowType.getFieldList().get(0).getType().getSqlTypeName());
    assertEquals(INListLength - 1, adjustedRowType.getFieldList().get(0).getType().getPrecision());
  }

  private String charLiteralBuilder(int length) {
    StringBuilder sb = new StringBuilder(length);
    sb.append("");
    for (int i = 0; i < length; i++) {
      sb.append("a");
    }
    return sb.toString();
  }
}
