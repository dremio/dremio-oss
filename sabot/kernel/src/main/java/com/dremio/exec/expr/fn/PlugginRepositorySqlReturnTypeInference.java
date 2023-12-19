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
package com.dremio.exec.expr.fn;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.planner.sql.TypeInferenceUtils;

public class PlugginRepositorySqlReturnTypeInference implements SqlReturnTypeInference {
  static final Logger logger = LoggerFactory.getLogger(PlugginRepositorySqlReturnTypeInference.class);

  private final PluggableFunctionRegistry registry;
  private final boolean isDecimalV2Enabled;

  // This is created per query, so safe to use decimal setting as a variable.
  public PlugginRepositorySqlReturnTypeInference(PluggableFunctionRegistry registry,
                                                 boolean isDecimalV2Enabled) {
    this.registry = registry;
    this.isDecimalV2Enabled = isDecimalV2Enabled;
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    for (RelDataType type : opBinding.collectOperandTypes()) {
      final TypeProtos.MinorType minorType = TypeInferenceUtils.getMinorTypeFromCalciteType(type);
      if(minorType == TypeProtos.MinorType.LATE) {
        return opBinding.getTypeFactory()
          .createTypeWithNullability(
            opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY),
            true);
      }
    }

    final FunctionCall functionCall = TypeInferenceUtils.convertSqlOperatorBindingToFunctionCall(opBinding);
    final AbstractFunctionHolder funcHolder = registry.findFunction(functionCall);
    if(funcHolder == null) {
      final StringBuilder operandTypes = new StringBuilder();
      for(int j = 0; j < opBinding.getOperandCount(); ++j) {
        operandTypes.append(opBinding.getOperandType(j).getSqlTypeName());
        if(j < opBinding.getOperandCount() - 1) {
          operandTypes.append(",");
        }
      }

      throw UserException
        .functionError()
        .message(String.format("%s does not support operand types (%s)",
          opBinding.getOperator().getName(),
          operandTypes))
        .build(logger);
    }

    return TypeInferenceUtils.createCalciteTypeWithNullability(
      opBinding.getTypeFactory(),
      TypeInferenceUtils.getCalciteTypeFromMinorType(funcHolder.getReturnType(null).toMinorType()),
      true,
      null);
  }
}
