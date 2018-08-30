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
package com.dremio.exec.planner;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.mockito.Mock;

import com.dremio.exec.planner.types.SqlTypeFactoryImpl;

public class RexBuilderTestBase {
  protected final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  protected final RexBuilder builder = new DremioRexBuilder(typeFactory);
  protected RelDataType rowType;

  @Mock
  protected RelNode input = mock(RelNode.class);

  @Before
  public void setup() {
    rowType = typeFactory.createStructType(
        asList(
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            typeFactory.createSqlType(SqlTypeName.FLOAT),
            typeFactory.createSqlType(SqlTypeName.DOUBLE),
            typeFactory.createSqlType(SqlTypeName.DATE),
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.BOOLEAN)
        ),
        asList("intC", "bigIntC", "floatC", "doubleC", "dateC", "tsC", "varcharC", "boolC")
    );
    when(input.getRowType()).thenReturn(rowType);
  }

  //
  // Helper methods to create various Calcite RexNode expressions
  //

  protected RexInputRef input(int colIndex) {
    return builder.makeInputRef(input, colIndex);
  }

  protected RexLiteral intLit(int val) {
    return intLit(0, val);
  }

  protected RexLiteral intLit(int colIndex, int val) {
    return (RexLiteral) builder.makeLiteral(val, rowType.getFieldList().get(colIndex).getType(), false);
  }

  protected RexLiteral bigIntLit(long val) {
    return bigIntLit(1, val);
  }

  protected RexLiteral bigIntLit(int colIndex, long val) {
    return (RexLiteral) builder.makeLiteral(val, rowType.getFieldList().get(colIndex).getType(), false);
  }

  protected RexLiteral floatLit(float val) {
    return floatLit(2, val);
  }

  protected RexLiteral floatLit(int colIndex, float val) {
    return (RexLiteral) builder.makeLiteral(val, rowType.getFieldList().get(colIndex).getType(), false);
  }

  protected RexLiteral doubleLit(double val) {
    return doubleLit(3, val);
  }

  protected RexLiteral doubleLit(int colIndex, double val) {
    return (RexLiteral) builder.makeLiteral(val, rowType.getFieldList().get(colIndex).getType(), false);
  }

  protected RexLiteral dateLit(int millisSinceEpoch) {
    return dateLit(4, millisSinceEpoch);
  }

  protected RexLiteral dateLit(int colIndex, int millisSinceEpoch) {
    return (RexLiteral) builder.makeLiteral(millisSinceEpoch,
        rowType.getFieldList().get(colIndex).getType(), false);
  }

  protected RexLiteral tsLit(long millisSinceEpoch) {
    return tsLit(5, millisSinceEpoch);
  }

  protected RexLiteral tsLit(int colIndex, long millisSinceEpoch) {
    return (RexLiteral) builder.makeLiteral(millisSinceEpoch,
        rowType.getFieldList().get(colIndex).getType(), false);
  }

  protected RexLiteral varcharLit(String varchar) {
    return varcharLit(6, varchar);
  }

  protected RexLiteral varcharLit(int colIndex, String varchar) {
    return (RexLiteral) builder.makeLiteral(varchar, rowType.getFieldList().get(colIndex).getType(), false);
  }

  protected RexLiteral boolLit(boolean value) {
    return boolLit(7, value);
  }

  protected RexLiteral boolLit(int colIndex, boolean value) {
    return (RexLiteral) builder.makeLiteral(value, rowType.getFieldList().get(colIndex).getType(), false);
  }
}
