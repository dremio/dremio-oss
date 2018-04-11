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

package com.dremio.exec.planner.types;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.expression.CompleteType;

public class RelDataTypeSystemImpl extends org.apache.calcite.rel.type.RelDataTypeSystemImpl {

  public static final int DEFAULT_PRECISION = CompleteType.DEFAULT_VARCHAR_PRECISION;
  public static final int MAX_NUMERIC_SCALE = 38;
  public static final int MAX_NUMERIC_PRECISION = 38;

  public static final RelDataTypeSystem REL_DATATYPE_SYSTEM = new RelDataTypeSystemImpl();

  @Override
  public int getDefaultPrecision(SqlTypeName typeName) {
    switch (typeName) {
      case CHAR:
      case BINARY:
      case VARCHAR:
      case VARBINARY:
        return DEFAULT_PRECISION;
      default:
        return super.getDefaultPrecision(typeName);
    }
  }

  @Override
  public int getMaxNumericScale() {
    return MAX_NUMERIC_SCALE;
  }

  @Override
  public int getMaxNumericPrecision() {
    return MAX_NUMERIC_PRECISION;
  }

  @Override
  public RelDataType deriveSumType(
      RelDataTypeFactory typeFactory, RelDataType argumentType) {
    SqlTypeName sqlTypeName = argumentType.getSqlTypeName();
    switch (sqlTypeName) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), argumentType.isNullable());
      case FLOAT:
      case DOUBLE:
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), argumentType.isNullable());
    }
    return argumentType;
  }

  @Override
  public boolean isSchemaCaseSensitive() {
    // Dremio uses case-insensitive and case-preserve policy
    return false;
  }

}
