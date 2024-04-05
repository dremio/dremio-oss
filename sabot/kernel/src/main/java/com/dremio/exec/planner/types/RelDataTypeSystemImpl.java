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

package com.dremio.exec.planner.types;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.expr.fn.OutputDerivation;
import org.apache.arrow.gandiva.evaluator.DecimalTypeUtil;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

public class RelDataTypeSystemImpl extends org.apache.calcite.rel.type.RelDataTypeSystemImpl {

  public static final int DEFAULT_PRECISION = CompleteType.DEFAULT_VARCHAR_PRECISION;
  public static final int MAX_NUMERIC_SCALE = 38;
  public static final int MAX_NUMERIC_PRECISION = 38;

  public static final int SUPPORTED_DATETIME_PRECISION = 3;

  public static final RelDataTypeSystem REL_DATA_TYPE_SYSTEM = new RelDataTypeSystemImpl();

  private RelDataTypeSystemImpl() {}

  @Override
  public int getDefaultPrecision(SqlTypeName typeName) {
    switch (typeName) {
      case CHAR:
      case BINARY:
      case VARCHAR:
      case VARBINARY:
        return DEFAULT_PRECISION;
      case TIME:
      case TIMESTAMP:
        return SUPPORTED_DATETIME_PRECISION;
      case FLOAT:
        return 6;
      default:
        return super.getDefaultPrecision(typeName);
    }
  }

  @Override
  public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
      case TIME:
      case TIMESTAMP:
        return SUPPORTED_DATETIME_PRECISION;
      default:
        return super.getMaxPrecision(typeName);
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
  public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
    SqlTypeName sqlTypeName = argumentType.getSqlTypeName();
    switch (sqlTypeName) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BIGINT), argumentType.isNullable());
      case FLOAT:
      case DOUBLE:
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.DOUBLE), argumentType.isNullable());
      case DECIMAL:
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(
                SqlTypeName.DECIMAL, MAX_NUMERIC_PRECISION, argumentType.getScale()),
            argumentType.isNullable());
    }
    return argumentType;
  }

  @Override
  public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
  }

  @Override
  public RelDataType deriveFractionalRankType(RelDataTypeFactory typeFactory) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.DOUBLE), false);
  }

  @Override
  public RelDataType deriveRankType(RelDataTypeFactory typeFactory) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BIGINT), false);
  }

  @Override
  public RelDataType deriveCovarType(
      RelDataTypeFactory typeFactory, RelDataType arg0Type, RelDataType arg1Type) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
  }

  @Override
  public RelDataType deriveDecimalMultiplyType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    return getDecimalReturnType(typeFactory, type1, type2, DecimalTypeUtil.OperationType.MULTIPLY);
  }

  @Override
  public RelDataType deriveDecimalDivideType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    return getDecimalReturnType(typeFactory, type1, type2, DecimalTypeUtil.OperationType.DIVIDE);
  }

  @Override
  public RelDataType deriveDecimalPlusType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    return getDecimalReturnType(typeFactory, type1, type2, DecimalTypeUtil.OperationType.ADD);
  }

  @Override
  public RelDataType deriveDecimalModType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    return getDecimalReturnType(typeFactory, type1, type2, DecimalTypeUtil.OperationType.MOD);
  }

  public RelDataType deriveDecimalTruncateType(
      RelDataTypeFactory typeFactory, RelDataType type1, Integer scale2) {
    if (!SqlTypeUtil.isExactNumeric(type1) || !SqlTypeUtil.isDecimal(type1)) {
      return null;
    }

    ArrowType.Decimal finalPrecisionScale =
        OutputDerivation.getDecimalOutputTypeForTruncate(
            type1.getPrecision(), type1.getScale(), scale2);

    return typeFactory.createSqlType(
        SqlTypeName.DECIMAL, finalPrecisionScale.getPrecision(), finalPrecisionScale.getScale());
  }

  public RelDataType deriveDecimalRoundType(
      RelDataTypeFactory typeFactory, RelDataType type1, Integer scale2) {
    if (!SqlTypeUtil.isExactNumeric(type1) || !SqlTypeUtil.isDecimal(type1)) {
      return null;
    }

    ArrowType.Decimal finalPrecisionScale =
        OutputDerivation.getDecimalOutputTypeForRound(
            type1.getPrecision(), type1.getScale(), scale2);

    return typeFactory.createSqlType(
        SqlTypeName.DECIMAL, finalPrecisionScale.getPrecision(), finalPrecisionScale.getScale());
  }

  private RelDataType getDecimalReturnType(
      RelDataTypeFactory typeFactory,
      RelDataType type1,
      RelDataType type2,
      DecimalTypeUtil.OperationType operationType) {
    if (!SqlTypeUtil.isExactNumeric(type1)
        || !SqlTypeUtil.isExactNumeric(type2)
        || (!SqlTypeUtil.isDecimal(type1) && !SqlTypeUtil.isDecimal(type2))) {
      return null;
    } else {
      ArrowType.Decimal operand1 =
          new ArrowType.Decimal(type1.getPrecision(), type1.getScale(), 128);
      ArrowType.Decimal operand2 =
          new ArrowType.Decimal(type2.getPrecision(), type2.getScale(), 128);
      ArrowType.Decimal output =
          DecimalTypeUtil.getResultTypeForOperation(operationType, operand1, operand2);
      return typeFactory.createSqlType(
          SqlTypeName.DECIMAL, output.getPrecision(), output.getScale());
    }
  }

  @Override
  public boolean isSchemaCaseSensitive() {
    // Dremio uses case-insensitive and case-preserve policy
    return false;
  }
}
