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

import static org.apache.calcite.sql.type.ReturnTypes.ARG0_NULLABLE;

import com.dremio.exec.planner.types.RelDataTypeSystemImpl;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;

/** Extension to Calcite's ReturnTypes utility class. */
public class DremioReturnTypes {

  private DremioReturnTypes() {}

  public static final SqlReturnTypeInference DECIMAL_TRUNCATE =
      opBinding -> {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        RelDataType type1 = opBinding.getOperandType(0);
        Integer scale2 = 0;

        if (opBinding.getOperandCount() > 1 && opBinding instanceof SqlCallBinding) {
          SqlCallBinding callBinding = (SqlCallBinding) opBinding;
          SqlNode operand1 = callBinding.operand(1);

          if (!(operand1 instanceof SqlLiteral)) {
            return type1;
          } else {
            scale2 = opBinding.getIntLiteralOperand(1);
          }
        }

        RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();
        if (typeFactory.getTypeSystem() instanceof RelDataTypeSystemImpl) {
          return ((RelDataTypeSystemImpl) typeSystem)
              .deriveDecimalTruncateType(typeFactory, type1, scale2);
        } else {
          throw new RuntimeException("Unknown type factory");
        }
      };

  /**
   * Type-inference strategy whereby the result type of a call is the decimal truncate of two exact
   * numeric operands where at least one of the operands is a decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_TRUNCATE_NULLABLE =
      ReturnTypes.cascade(DECIMAL_TRUNCATE, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is {@link #DECIMAL_TRUNCATE_NULLABLE}
   * with a fallback to {@link #ARG0_NULLABLE} These rules are used for truncate.
   */
  public static final SqlReturnTypeInference NULLABLE_TRUNCATE =
      ReturnTypes.chain(DECIMAL_TRUNCATE_NULLABLE, ARG0_NULLABLE);

  public static final SqlReturnTypeInference DECIMAL_ROUND =
      opBinding -> {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        RelDataType type1 = opBinding.getOperandType(0);
        Integer scale2 = 0;

        if (opBinding.getOperandCount() > 1 && opBinding instanceof SqlCallBinding) {
          SqlCallBinding callBinding = (SqlCallBinding) opBinding;
          SqlNode operand1 = callBinding.operand(1);

          if (!(operand1 instanceof SqlLiteral)) {
            return type1;
          } else {
            scale2 = opBinding.getIntLiteralOperand(1);
          }
        }
        RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();

        if (typeFactory.getTypeSystem() instanceof RelDataTypeSystemImpl) {
          return ((RelDataTypeSystemImpl) typeSystem)
              .deriveDecimalRoundType(typeFactory, type1, scale2);
        } else {
          throw new RuntimeException("Unknown type factory");
        }
      };

  /**
   * Type-inference strategy whereby the result type of a call is the decimal round of two exact
   * numeric operands where at least one of the operands is a decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_ROUND_NULLABLE =
      ReturnTypes.cascade(DECIMAL_ROUND, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is {@link #DECIMAL_ROUND_NULLABLE}
   * with a fallback to {@link #ARG0_NULLABLE} These rules are used for round.
   */
  public static final SqlReturnTypeInference NULLABLE_ROUND =
      ReturnTypes.chain(DECIMAL_ROUND_NULLABLE, ARG0_NULLABLE);

  public static final SqlReturnTypeInference TO_MAP =
      new SqlReturnTypeInference() {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          RelDataType mapKeyType = opBinding.getOperandType(0);
          List<RelDataType> valuesTypes = new LinkedList<>();
          for (int i = 1; i < opBinding.getOperandCount(); i += 2) {
            valuesTypes.add(opBinding.getOperandType(i));
          }
          RelDataType mapValueType = opBinding.getTypeFactory().leastRestrictive(valuesTypes);
          RelDataType mapType = opBinding.getTypeFactory().createMapType(mapKeyType, mapValueType);
          return mapType;
        }
      };
  public static final SqlReturnTypeInference ARRAYS_TO_MAP =
      new SqlReturnTypeInference() {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          RelDataType mapKeyType = opBinding.getOperandType(0).getComponentType();
          RelDataType mapValueType = opBinding.getOperandType(1).getComponentType();
          RelDataType mapType = opBinding.getTypeFactory().createMapType(mapKeyType, mapValueType);
          return mapType;
        }
      };

  public static final SqlReturnTypeInference VARCHAR_MAX_PRECISION_NULLABLE =
      ReturnTypes.cascade(
          ReturnTypes.explicit(SqlTypeName.VARCHAR),
          DremioSqlTypeTransforms.MAX_PRECISION,
          SqlTypeTransforms.TO_NULLABLE);

  /**
   * Return type inference that just returns the element type of the array in the first argument.
   */
  public static final SqlReturnTypeInference ARG0_ARRAY_ELEMENT =
      opBinding -> {
        if (opBinding.getOperandCount() == 0) {
          throw new UnsupportedOperationException("Expected at least one argument.");
        }

        RelDataType type = opBinding.getOperandType(0);
        if (type.getSqlTypeName() != SqlTypeName.ARRAY) {
          throw new UnsupportedOperationException("Expected the first argument to an array.");
        }

        return type.getComponentType();
      };

  /**
   * Same as DECIMAL_QUOTIENT but with default DECIMAL, since the arguments aren't known until the
   * rewrite.
   */
  public static final SqlReturnTypeInference DECIMAL_QUOTIENT_DEFAULT_PRECISION_SCALE =
      opBinding -> {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        RelDataType defaultDecimal = typeFactory.createSqlType(SqlTypeName.DECIMAL);
        RelDataType defaultDecimalDivision =
            typeFactory.createDecimalQuotient(defaultDecimal, defaultDecimal);
        RelDataType withNullable =
            typeFactory.createTypeWithNullability(defaultDecimalDivision, true);
        return withNullable;
      };
}
