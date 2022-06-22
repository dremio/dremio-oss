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
package com.dremio.exec.planner.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.google.common.collect.Lists;

public class DremioRelDataTypeFactoryTest {
  private static final SqlTypeFactoryImpl TYPE_FACTORY = SqlTypeFactoryImpl.INSTANCE;

  @Test
  public void testDecimalTruncateReturnTypeInference() {
    RelDataType operand1 = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 28, 12);
    RelDataType operand2 = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType dataType = DremioSqlOperatorTable.TRUNCATE.inferReturnType(TYPE_FACTORY, Lists.newArrayList(new RelDataType[]{operand1, operand2}));
    Assert.assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    Assert.assertEquals(16, dataType.getPrecision());
    Assert.assertEquals(0, dataType.getScale());
  }

  @Test
  public void testDecimalRoundReturnTypeInference() {
    RelDataType operand1 = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 28, 12);
    RelDataType operand2 = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType dataType = DremioSqlOperatorTable.ROUND.inferReturnType(TYPE_FACTORY, Lists.newArrayList(new RelDataType[]{operand1, operand2}));
    Assert.assertEquals(SqlTypeName.DECIMAL, dataType.getSqlTypeName());
    // The precision is one above the value to handle if the significant figure changes.
    // For instance ROUND(9.9 as DECIMAL(2, 1)) should result in 10 AS DECIMAL(2, 0)
    Assert.assertEquals(17, dataType.getPrecision());
    Assert.assertEquals(0, dataType.getScale());
  }

}
