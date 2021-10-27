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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;

import com.dremio.exec.planner.types.RelDataTypeSystemImpl;

public class DremioReturnTypes {

  private DremioReturnTypes() {
  }

  public static final SqlReturnTypeInference DECIMAL_TRUNCATE = opBinding -> {
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
      return ((RelDataTypeSystemImpl)typeSystem).deriveDecimalTruncateType(typeFactory, type1, scale2);
    } else {
      throw new RuntimeException("Unknown type factory");
    }
  };

  /**
   * Type-inference strategy whereby the result type of a call is the decimal
   * truncate of two exact numeric operands where at least one of the operands is a
   * decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_TRUNCATE_NULLABLE =
      ReturnTypes.cascade(DECIMAL_TRUNCATE, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_TRUNCATE_NULLABLE} with a fallback to {@link #ARG0_NULLABLE}
   * These rules are used for truncate.
   */
  public static final SqlReturnTypeInference NULLABLE_TRUNCATE =
      ReturnTypes.chain(DECIMAL_TRUNCATE_NULLABLE, ARG0_NULLABLE);


  public static final SqlReturnTypeInference DECIMAL_ROUND = opBinding -> {
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
      return ((RelDataTypeSystemImpl)typeSystem).deriveDecimalRoundType(typeFactory, type1, scale2);
    } else {
      throw new RuntimeException("Unknown type factory");
    }
  };

  /**
   * Type-inference strategy whereby the result type of a call is the decimal
   * round of two exact numeric operands where at least one of the operands is a
   * decimal.
   */
  public static final SqlReturnTypeInference DECIMAL_ROUND_NULLABLE =
      ReturnTypes.cascade(DECIMAL_ROUND, SqlTypeTransforms.TO_NULLABLE);

  /**
   * Type-inference strategy whereby the result type of a call is
   * {@link #DECIMAL_ROUND_NULLABLE} with a fallback to {@link #ARG0_NULLABLE}
   * These rules are used for round.
   */
  public static final SqlReturnTypeInference NULLABLE_ROUND =
      ReturnTypes.chain(DECIMAL_ROUND_NULLABLE, ARG0_NULLABLE);
}
