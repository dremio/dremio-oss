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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Staged Builder Pattern for building SqlOperator.
 *
 * The builder works by asking the user to build the SqlOperator one stage at a time.
 * The stages are:
 *
 * 1) Name
 * 2) Return Type
 * 2) Operand Types
 * 3) Build
 *
 * Each one of these stages can be extended to allow the user to supply the information in different formats.
 *
 * For example some people might write:
 *
 * SqlOperatorBuilder
 *    .name("foo")
 *    .returnType(ReturnTypes.Numeric)
 *    .operandType(Checkers.INT_INT)
 *    .build()
 *
 * While others prefer to use:
 *
 * SqlOperatorBuilder
 *    .name("foo")
 *    .returnType(OperandType.NUMERIC)
 *    .operandType(SqlTypeName.INT, SqlTypeName.INT)
 *    .build()
 */
public final class SqlOperatorBuilder {
  private SqlOperatorBuilder() {}

  private static final class BaseBuilder {
    private final String name;
    private SqlReturnTypeInference sqlReturnTypeInference;
    private SqlOperandTypeChecker sqlOperandTypeChecker;

    private boolean isDeterministic;
    private boolean isDynamic;

    public BaseBuilder(String name) {
      Preconditions.checkNotNull(name);
      this.name = name;
      this.isDeterministic = true;
      this.isDynamic = false;
    }

    public BaseBuilder returnType(SqlReturnTypeInference sqlReturnTypeInference) {
      Preconditions.checkNotNull(sqlReturnTypeInference);
      this.sqlReturnTypeInference = sqlReturnTypeInference;
      return this;
    }

    public BaseBuilder operandTypes(SqlOperandTypeChecker sqlOperandTypeChecker) {
      Preconditions.checkNotNull(sqlOperandTypeChecker);
      this.sqlOperandTypeChecker = sqlOperandTypeChecker;
      return this;
    }

    public BaseBuilder withDeterminisim(boolean determinisim) {
      this.isDeterministic = determinisim;
      return this;
    }

    public BaseBuilder withDynamism(boolean dynamism) {
      this.isDynamic = dynamism;
      return this;
    }

    public SqlOperator build() {
      return new DremioSqlFunction(
        name.toUpperCase(),
        sqlReturnTypeInference,
        sqlOperandTypeChecker,
        isDeterministic,
        isDynamic);
    }
  }

  public static final class SqlOperatorBuilderReturnTypeStage {
    private final BaseBuilder baseBuilder;

    private SqlOperatorBuilderReturnTypeStage(BaseBuilder baseBuilder) {
      Preconditions.checkNotNull(baseBuilder);
      this.baseBuilder = baseBuilder;
    }

    public SqlOperatorBuilderOperandTypeStage returnType(SqlReturnTypeInference sqlReturnTypeInference) {
      baseBuilder.returnType(sqlReturnTypeInference);
      return new SqlOperatorBuilderOperandTypeStage(baseBuilder);
    }

    public SqlOperatorBuilderOperandTypeStage returnType(SqlTypeName returnType) {
      // This API doesn't support complex types.
      return returnType(ReturnTypes.explicit(returnType));
    }

    public SqlOperatorBuilderOperandTypeStage returnType(RelDataType relDataType) {
      // This API doesn't support complex types.
      return returnType(ReturnTypes.explicit(relDataType));
    }
  }

  public static final class SqlOperatorBuilderOperandTypeStage {
    private final BaseBuilder baseBuilder;

    private SqlOperatorBuilderOperandTypeStage(BaseBuilder baseBuilder) {
      Preconditions.checkNotNull(baseBuilder);
      this.baseBuilder = baseBuilder;
    }

    public SqlOperatorBuilderFinalStage noOperands() {
      baseBuilder.operandTypes(Checker.of(0));
      return new SqlOperatorBuilderFinalStage(baseBuilder);
    }

    public SqlOperatorBuilderFinalStage anyOperands() {
      baseBuilder.operandTypes(Checker.any());
      return new SqlOperatorBuilderFinalStage(baseBuilder);
    }

    public SqlOperatorBuilderFinalStage operandTypes(SqlTypeName ... operandTypes) {
      List<SqlOperand> sqlOperands = Arrays
        .stream(operandTypes)
        .map(sqlTypeName -> SqlOperand.regular(sqlTypeName))
        .collect(Collectors.toList());
      return operandTypes(sqlOperands);
    }

    public SqlOperatorBuilderFinalStage operandTypes(SqlOperand ... sqlOperands) {
      return operandTypes(Arrays.stream(sqlOperands).collect(Collectors.toList()));
    }

    public SqlOperatorBuilderFinalStage operandTypes(List<SqlOperand> sqlOperands) {
      SqlOperandTypeChecker sqlOperandTypeChecker = StaticSqlOperandTypeChecker.create(sqlOperands);
      return operandTypes(sqlOperandTypeChecker);
    }

    public SqlOperatorBuilderFinalStage operandTypes(SqlOperandTypeChecker sqlOperandTypeChecker) {
      baseBuilder.operandTypes(sqlOperandTypeChecker);
      return new SqlOperatorBuilderFinalStage(baseBuilder);
    }

    private static final class StaticSqlOperandTypeChecker implements SqlOperandTypeChecker {
      private static final ImmutableMap<SqlTypeName, ImmutableSet<SqlTypeName>> COERCION_MAP = new ImmutableMap.Builder<SqlTypeName, ImmutableSet<SqlTypeName>>()
        .put(SqlTypeName.ANY, new ImmutableSet.Builder<SqlTypeName>().add(SqlTypeName.ARRAY, SqlTypeName.MAP).addAll(SqlTypeName.ALL_TYPES).build())
        .put(SqlTypeName.DECIMAL, ImmutableSet.copyOf(SqlTypeName.EXACT_TYPES))
        .put(SqlTypeName.BIGINT, ImmutableSet.copyOf(SqlTypeName.INT_TYPES))
        .put(SqlTypeName.DOUBLE, ImmutableSet.copyOf(SqlTypeName.NUMERIC_TYPES))
        .put(SqlTypeName.VARCHAR, ImmutableSet.of(SqlTypeName.CHAR))
        .put(SqlTypeName.VARBINARY, ImmutableSet.of(SqlTypeName.BINARY))
        .put(SqlTypeName.DATE, ImmutableSet.of(SqlTypeName.VARCHAR))
        .put(SqlTypeName.TIME, ImmutableSet.of(SqlTypeName.VARCHAR))
        .put(SqlTypeName.TIMESTAMP, ImmutableSet.of(SqlTypeName.VARCHAR))
        .put(SqlTypeName.INTERVAL_DAY_SECOND, ImmutableSet.of(
          SqlTypeName.INTERVAL_DAY,
          SqlTypeName.INTERVAL_HOUR,
          SqlTypeName.INTERVAL_MINUTE,
          SqlTypeName.INTERVAL_SECOND))
        .put(SqlTypeName.INTERVAL_YEAR_MONTH, ImmutableSet.of(
          SqlTypeName.INTERVAL_YEAR,
          SqlTypeName.INTERVAL_MONTH))
        .build();

      private List<SqlOperand> operands;
      private SqlOperandCountRange sqlOperandCountRange;

      private StaticSqlOperandTypeChecker(List<SqlOperand> operands, SqlOperandCountRange sqlOperandCountRange) {
        this.operands = ImmutableList.copyOf(operands);
        this.sqlOperandCountRange = sqlOperandCountRange;
      }

      public static StaticSqlOperandTypeChecker create(List<SqlOperand> operands) {
        SqlOperandCountRange sqlOperandCountRange;
        if (operands.isEmpty()) {
          sqlOperandCountRange = SqlOperandCountRanges.of(0);
        } else {
          SqlOperand lastOperand = operands.get(operands.size() - 1);
          switch (lastOperand.type) {
          case REGULAR:
            sqlOperandCountRange = SqlOperandCountRanges.of(operands.size());
            break;

          case OPTIONAL:
            sqlOperandCountRange = SqlOperandCountRanges.between(operands.size() - 1, operands.size());
            break;

          case VARIADIC:
            sqlOperandCountRange = SqlOperandCountRanges.from(operands.size() - 1);
            break;

          default:
            throw new UnsupportedOperationException("UNKNOWN TYPE: " + lastOperand.type);
          }
        }

        return new StaticSqlOperandTypeChecker(operands, sqlOperandCountRange);
      }

      @Override
      public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        if (!sqlOperandCountRange.isValidCount(callBinding.getOperandCount())) {
          if (throwOnFailure) {
            throw UserException
              .validationError()
              .message(
                "'" + callBinding.getOperator().getName() + "'"
                  + " does not accept " + callBinding.getOperandCount() + " arguments.")
              .buildSilently();
          }

          return false;
        }

        for (int i = 0, j = 0; i < callBinding.getOperandCount(); i++) {
          RelDataType userOperand = callBinding.getOperandType(i);
          SqlTypeName userOperandType;
          // TODO: See if this can be replaced with typeFactory.leastRestrictiveType
          // This logic is needed because it is inconsistent when
          // the type is a Decimal with low precision and zero scale or an Integer
          if (canBeCoercedDownToInt32(userOperand)) {
            userOperandType = SqlTypeName.INTEGER;
          } else if (canBeCoercedDownToInt64(userOperand)) {
            userOperandType = SqlTypeName.BIGINT;
          } else {
            userOperandType = userOperand.getSqlTypeName();
          }

          SqlOperand specOperand = operands.get(j);
          if (!specOperand.typeRange.stream().anyMatch(acceptedType -> {
            if (acceptedType == userOperandType) {
              return true;
            }

            ImmutableSet<SqlTypeName> coercibleTypes = COERCION_MAP.get(acceptedType);
            if (coercibleTypes == null) {
              return false;
            }

            return coercibleTypes.contains(userOperandType);
          })) {
            if (throwOnFailure) {
              throw UserException
                .validationError()
                .message(
                  "'" + callBinding.getOperator().getName() + "'"
                    + " does not accept the supplied operand types: "
                    + callBinding
                      .collectOperandTypes()
                      .stream()
                      .map(relDataType -> relDataType.getSqlTypeName().toString())
                      .collect(Collectors.joining(", ")))
                .buildSilently();
            }

            return false;
          }

          if (specOperand.type != SqlOperand.Type.VARIADIC) {
            j++;
          }
        }

        return true;
      }

      @Override
      public SqlOperandCountRange getOperandCountRange() {
        return sqlOperandCountRange;
      }

      @Override
      public String getAllowedSignatures(SqlOperator op, String opName) {
        return null;
      }

      @Override
      public Consistency getConsistency() {
        return Consistency.NONE;
      }

      @Override
      public boolean isOptional(int i) {
        return operands.get(i).type == SqlOperand.Type.OPTIONAL;
      }
    }

    private static boolean canBeCoercedDownToInt32(RelDataType type) {
      if (type.getSqlTypeName() != SqlTypeName.DECIMAL) {
        return false;
      }

      if (type.getScale() != 0) {
        return false;
      }

      // 32 bits provides 10 digits of precision for positive numbers (excluding the sign).
      if (type.getPrecision() > 10) {
        return false;
      }

      return true;
    }

    private static boolean canBeCoercedDownToInt64(RelDataType type) {
      if (type.getSqlTypeName() != SqlTypeName.DECIMAL) {
        return false;
      }

      if (type.getScale() != 0) {
        return false;
      }

      // 64 bits provides 19 digits of precision for positive numbers (excluding the sign).
      if (type.getPrecision() > 19) {
        return false;
      }

      return true;
    }
  }

  public static final class SqlOperatorBuilderFinalStage {
    private final BaseBuilder baseBuilder;

    private SqlOperatorBuilderFinalStage(BaseBuilder baseBuilder) {
      Preconditions.checkNotNull(baseBuilder);
      this.baseBuilder = baseBuilder;
    }

    public SqlOperatorBuilderFinalStage withDeterminism(boolean determinism) {
      baseBuilder.withDeterminisim( determinism);
      return this;
    }

    public SqlOperatorBuilderFinalStage withDynanism(boolean dynanism) {
      baseBuilder.withDynamism(dynanism);
      return this;
    }

    public SqlOperator build() {
      return baseBuilder.build();
    }
  }

  public static SqlOperatorBuilderReturnTypeStage name(String name) {
    return new SqlOperatorBuilderReturnTypeStage(new BaseBuilder(name));
  }

  public static SqlOperator alias(String alias, SqlOperator sqlOperator) {
    // Operator might have chosen to override the method instead of passing in a strategy pattern.
    SqlReturnTypeInference sqlReturnTypeInference = sqlOperator.getReturnTypeInference();
    if (sqlReturnTypeInference == null) {
      sqlReturnTypeInference = new SqlReturnTypeInference() {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          return sqlOperator.inferReturnType(opBinding);
        }
      };
    }

    SqlOperandTypeChecker sqlOperandTypeChecker = sqlOperator.getOperandTypeChecker();
    if (sqlOperandTypeChecker == null) {
      sqlOperandTypeChecker = new SqlOperandTypeChecker() {
        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
          return sqlOperator.checkOperandTypes(callBinding, throwOnFailure);
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
          return sqlOperator.getOperandCountRange();
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
          return sqlOperator.getAllowedSignatures(opName);
        }

        @Override
        public Consistency getConsistency() {
          return null;
        }

        @Override
        public boolean isOptional(int i) {
          return false;
        }
      };
    }

    return name(alias)
      .returnType(sqlReturnTypeInference)
      .operandTypes(sqlOperandTypeChecker)
      .withDeterminism(sqlOperator.isDeterministic())
      .withDynanism(sqlOperator.isDynamicFunction())
      .build();
  }

  private static final class DremioSqlFunction extends SqlFunction {
    /**
     * This means that the function will return the same output given the same input.
     */
    private final boolean isDeterministic;

    /**
     * These functions will return different results based on the environment, but computed only once per query.
     */
    private final boolean isDynamic;

    public DremioSqlFunction(
      String name,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      boolean isDeterministic,
      boolean isDynamic) {
      super(
        name,
        SqlKind.OTHER_FUNCTION,
        returnTypeInference,
        null,
        operandTypeChecker,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
      this.isDeterministic = isDeterministic;
      this.isDynamic = isDynamic;
    }

    @Override
    public boolean isDeterministic() {
      return isDeterministic;
    }

    @Override
    public boolean isDynamicFunction() { return isDynamic; }
  }
}
