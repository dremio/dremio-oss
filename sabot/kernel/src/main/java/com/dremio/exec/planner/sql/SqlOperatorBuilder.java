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

import static com.dremio.exec.planner.sql.SqlOperand.Type.REGULAR;

import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Pair;

/**
 * Staged Builder Pattern for building SqlOperator.
 *
 * <p>The builder works by asking the user to build the SqlOperator one stage at a time. The stages
 * are:
 *
 * <p>1) Name 2) Return Type 2) Operand Types 3) Build
 *
 * <p>Each one of these stages can be extended to allow the user to supply the information in
 * different formats.
 *
 * <p>For example some people might write:
 *
 * <p>SqlOperatorBuilder .name("foo") .returnType(ReturnTypes.Numeric)
 * .operandType(Checkers.INT_INT) .build()
 *
 * <p>While others prefer to use:
 *
 * <p>SqlOperatorBuilder .name("foo") .returnType(OperandType.NUMERIC) .operandType(SqlTypeName.INT,
 * SqlTypeName.INT) .build()
 */
public final class SqlOperatorBuilder {
  private SqlOperatorBuilder() {}

  private static final class BaseBuilder {
    private final String name;
    private SqlReturnTypeInference sqlReturnTypeInference;
    private SqlOperandTypeChecker sqlOperandTypeChecker;

    private ImplicitCoercionStrategy implicitCoercionStrategy;
    private boolean isDeterministic;
    private boolean isDynamic;
    private SqlSyntax sqlSyntax;

    public BaseBuilder(String name) {
      Preconditions.checkNotNull(name);
      this.name = name;
      this.isDeterministic = true;
      this.isDynamic = false;
      this.sqlSyntax = SqlSyntax.FUNCTION;
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

    public BaseBuilder withImplicitCoercionStrategy(
        ImplicitCoercionStrategy implicitCoercionStrategy) {
      this.implicitCoercionStrategy = implicitCoercionStrategy;
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

    public BaseBuilder withSqlSyntax(SqlSyntax sqlSyntax) {
      this.sqlSyntax = sqlSyntax;
      return this;
    }

    public SqlOperator build() {
      return new DremioSqlFunction(
          name.toUpperCase(),
          sqlReturnTypeInference,
          sqlOperandTypeChecker,
          implicitCoercionStrategy,
          isDeterministic,
          isDynamic,
          sqlSyntax);
    }
  }

  public static final class SqlOperatorBuilderReturnTypeStage {
    private final BaseBuilder baseBuilder;

    private SqlOperatorBuilderReturnTypeStage(BaseBuilder baseBuilder) {
      Preconditions.checkNotNull(baseBuilder);
      this.baseBuilder = baseBuilder;
    }

    public SqlOperatorBuilderOperandTypeStage returnType(
        SqlReturnTypeInference sqlReturnTypeInference) {
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

    public SqlOperatorBuilderFinalStage operandTypes(SqlTypeName... operandTypes) {
      List<SqlOperand> sqlOperands =
          Arrays.stream(operandTypes)
              .map(sqlTypeName -> SqlOperand.regular(sqlTypeName))
              .collect(Collectors.toList());
      return operandTypes(sqlOperands);
    }

    public SqlOperatorBuilderFinalStage operandTypes(SqlOperand... sqlOperands) {
      return operandTypes(Arrays.stream(sqlOperands).collect(Collectors.toList()));
    }

    public SqlOperatorBuilderFinalStage operandTypes(List<SqlOperand> sqlOperands) {
      SqlOperandTypeChecker sqlOperandTypeChecker = StaticSqlOperandTypeChecker.create(sqlOperands);
      return operandTypes(sqlOperandTypeChecker)
          .withImplicitCoercionStrategy(
              new StaticSqlOperandImplicitCoercionStrategy(
                  sqlOperands)); // default implicit coercion strategy here
    }

    public SqlOperatorBuilderFinalStage operandTypes(SqlOperandTypeChecker sqlOperandTypeChecker) {
      baseBuilder.operandTypes(sqlOperandTypeChecker);
      return new SqlOperatorBuilderFinalStage(baseBuilder);
    }

    public static final class StaticSqlOperandImplicitCoercionStrategy
        implements ImplicitCoercionStrategy {
      private static final List<CoercionRule> RULES =
          ImmutableList.of(
              new CoercionRule() {
                @Override
                public boolean isValidSource(RelDataType userType) {
                  return userType.getSqlTypeName() == SqlTypeName.DECIMAL
                      // int64 has no fractional digits by definition of being an integer.
                      && userType.getScale() == 0
                      // 64 bits provides 19 digits of precision for positive numbers (excluding the
                      // sign).
                      && userType.getPrecision() <= 19;
                }

                @Override
                public SqlTypeName destinationType() {
                  return SqlTypeName.BIGINT;
                }
              },
              new CoercionRule() {
                @Override
                public boolean isValidSource(RelDataType userType) {
                  return userType.getSqlTypeName() == SqlTypeName.DECIMAL
                      // int32 has no fractional digits by definition of being an integer.
                      && userType.getScale() == 0
                      // 32 bits provides 10 digits of precision for positive numbers (excluding the
                      // sign).
                      && userType.getPrecision() <= 10;
                }

                @Override
                public SqlTypeName destinationType() {
                  return SqlTypeName.INTEGER;
                }
              },
              new CoercionRule() {
                @Override
                public boolean isValidSource(RelDataType userType) {
                  return userType.getSqlTypeName() == SqlTypeName.DECIMAL
                      // Doubles have between 15 and 17 digits of precision shared between both
                      // the integer and fractional part of the number.
                      // To be conservative we just cap it to 15
                      && userType.getPrecision() <= 15;
                }

                @Override
                public SqlTypeName destinationType() {
                  return SqlTypeName.DOUBLE;
                }
              },
              new CoercionRule() {
                @Override
                public boolean isValidSource(RelDataType userType) {
                  return userType.getSqlTypeName() == SqlTypeName.VARCHAR;
                }

                @Override
                public SqlTypeName destinationType() {
                  return SqlTypeName.TIMESTAMP;
                }
              },
              new CoercionRule() {
                @Override
                public boolean isValidSource(RelDataType userType) {
                  return userType.getSqlTypeName() == SqlTypeName.VARCHAR;
                }

                @Override
                public SqlTypeName destinationType() {
                  return SqlTypeName.DATE;
                }
              },
              new CoercionRule() {
                @Override
                public boolean isValidSource(RelDataType userType) {
                  return userType.getSqlTypeName() == SqlTypeName.VARCHAR;
                }

                @Override
                public SqlTypeName destinationType() {
                  return SqlTypeName.TIME;
                }
              });

      private final List<SqlOperand> operands;

      public StaticSqlOperandImplicitCoercionStrategy(List<SqlOperand> operands) {
        this.operands = operands;
      }

      @Override
      public Map<Integer, RelDataType> coerce(SqlCallBinding sqlCallBinding) {
        Map<Integer, RelDataType> coercions = new HashMap<>();
        List<RelDataType> argumentTypes = sqlCallBinding.collectOperandTypes();
        RelDataTypeFactory relDataTypeFactory = sqlCallBinding.getTypeFactory();
        List<Pair<SqlOperand, RelDataType>> zipping = zipOperandWithTypes(operands, argumentTypes);
        for (int i = 0; i < zipping.size(); i++) {
          Pair<SqlOperand, RelDataType> pair = zipping.get(i);
          SqlOperand sqlOperand = pair.left;
          RelDataType userSuppliedType = pair.right;

          int finalI = i;
          RULES.stream()
              .filter(
                  rule ->
                      rule.isValidSource(userSuppliedType)
                          && sqlOperand.typeRange.contains(rule.destinationType()))
              .findFirst()
              .ifPresent(
                  rule ->
                      coercions.put(
                          finalI, relDataTypeFactory.createSqlType(rule.destinationType())));
        }

        return coercions;
      }

      private interface CoercionRule {
        boolean isValidSource(RelDataType userType);

        SqlTypeName destinationType();
      }
    }

    public static final class StaticSqlOperandTypeChecker implements SqlOperandTypeChecker {
      private final List<SqlOperand> operands;
      private final SqlOperandCountRange sqlOperandCountRange;

      private StaticSqlOperandTypeChecker(
          List<SqlOperand> operands, SqlOperandCountRange sqlOperandCountRange) {
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
              sqlOperandCountRange =
                  SqlOperandCountRanges.between(operands.size() - 1, operands.size());
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
        TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
        RelDataTypeFactory relDataTypeFactory = callBinding.getValidator().getTypeFactory();
        List<Pair<SqlOperand, RelDataType>> zipping =
            zipOperandWithTypes(operands, callBinding.collectOperandTypes());
        for (Pair<SqlOperand, RelDataType> pair : zipping) {
          SqlOperand specOperand = pair.left;
          RelDataType userType = pair.right;

          boolean anyMatch = specOperand.typeRange.contains(userType.getSqlTypeName());
          if (!anyMatch) {
            // Check to see if any of the types are "wider"
            anyMatch =
                specOperand.typeRange.stream()
                    .anyMatch(
                        acceptedType -> {
                          if (acceptedType == SqlTypeName.ANY) {
                            // getTightestCommonType doesn't work for ANY for whatever reason ...
                            return true;
                          }

                          // For non sql types we can't call createSqlType, so we need to add manual
                          // checks
                          if (acceptedType == SqlTypeName.ARRAY) {
                            return userType.getSqlTypeName() == SqlTypeName.ARRAY;
                          }

                          if (acceptedType == SqlTypeName.MAP) {
                            return userType.getSqlTypeName() == SqlTypeName.MAP;
                          }

                          // Technically this should only support going up a bigger interval type
                          if (SqlTypeName.INTERVAL_TYPES.contains(acceptedType)) {
                            return SqlTypeName.INTERVAL_TYPES.contains(userType.getSqlTypeName());
                          }

                          RelDataType acceptedRelDataType =
                              relDataTypeFactory.createSqlType(acceptedType);
                          RelDataType commonType =
                              typeCoercion.getTightestCommonType(acceptedRelDataType, userType);
                          if (commonType == null) {
                            return false;
                          }

                          return commonType.getSqlTypeName() == acceptedType;
                        });
          }

          if (!anyMatch) {
            if (throwOnFailure) {
              throw UserException.validationError()
                  .message(
                      "'"
                          + callBinding.getOperator().getName()
                          + "'"
                          + " does not accept the supplied operand types: "
                          + callBinding.collectOperandTypes().stream()
                              .map(relDataType -> relDataType.getSqlTypeName().toString())
                              .collect(Collectors.joining(", ")))
                  .buildSilently();
            }
            return false;
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

    /**
     * Zips a list of SQL operands with their corresponding relational data types into pairs.
     *
     * @param sqlOperands The list of SQL operands.
     * @param relDataTypes The list of relational data types. This list may be shorter than the list
     *     of SQL operands if variadic arguments are present.
     * @return A list of pairs where each pair contains a SQL operand and its corresponding data
     *     type.
     * @throws IllegalStateException if an unknown enum type is encountered.
     */
    private static List<Pair<SqlOperand, RelDataType>> zipOperandWithTypes(
        List<SqlOperand> sqlOperands, List<RelDataType> relDataTypes) {
      // In the simple scenario we will have something like this:
      // List<SqlOperand> sqlOperands = [A(regular), B(regular), C(regular)]
      // List<RelDataType> relDataTypes = [X, Y, Z]
      // And the output be one to one: [(A, X), (B, Y), (C, Z)]
      // Why this gets tricky is that functions have optional or variadic arguments:
      // List<SqlOperand> sqlOperands = [A(regular), B(regular), C(variadic)]
      // List<RelDataType> relDataTypes = [X, Y, Z, T, W]
      // So the zipping will be: [(A, X), (B, Y), (C, Z), (C, T), (C, W)]
      // Note that optional and variadic have to be the last arguments to a function.
      // Basically we are trying to create a mapping from sqlOperands to relDataTypes,
      // but the mapping isn't always 1 to 1 (and we need to keep track of the ordering)
      List<Pair<SqlOperand, RelDataType>> zipping = new ArrayList<>();
      int operandIndex = 0;
      for (RelDataType dataType : relDataTypes) {
        SqlOperand sqlOperand = sqlOperands.get(operandIndex);
        switch (sqlOperand.type) {
          case REGULAR:
            operandIndex++;
            break;
          case OPTIONAL:
          case VARIADIC:
            // Do nothing for optional and variadic arguments, they don't necessarily consume a data
            // type.
            break;
          default:
            throw new IllegalStateException("Unknown enum type: " + sqlOperand.type);
        }

        Pair<SqlOperand, RelDataType> pair = Pair.of(sqlOperand, dataType);
        zipping.add(pair);
      }

      return zipping;
    }
  }

  public static final class SqlOperatorBuilderFinalStage {
    private final BaseBuilder baseBuilder;

    private SqlOperatorBuilderFinalStage(BaseBuilder baseBuilder) {
      Preconditions.checkNotNull(baseBuilder);
      this.baseBuilder = baseBuilder;
    }

    public SqlOperatorBuilderFinalStage withImplicitCoercionStrategy(
        ImplicitCoercionStrategy implicitCoercionStrategy) {
      baseBuilder.withImplicitCoercionStrategy(implicitCoercionStrategy);
      return this;
    }

    public SqlOperatorBuilderFinalStage withDeterminism(boolean determinism) {
      baseBuilder.withDeterminisim(determinism);
      return this;
    }

    public SqlOperatorBuilderFinalStage withDynanism(boolean dynanism) {
      baseBuilder.withDynamism(dynanism);
      return this;
    }

    public SqlOperatorBuilderFinalStage withSqlSyntax(SqlSyntax sqlSyntax) {
      baseBuilder.withSqlSyntax(sqlSyntax);
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
      sqlReturnTypeInference =
          new SqlReturnTypeInference() {
            @Override
            public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
              return sqlOperator.inferReturnType(opBinding);
            }
          };
    }

    SqlOperandTypeChecker sqlOperandTypeChecker = sqlOperator.getOperandTypeChecker();
    if (sqlOperandTypeChecker == null) {
      sqlOperandTypeChecker =
          new SqlOperandTypeChecker() {
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
        .withSqlSyntax(sqlOperator.getSyntax())
        .build();
  }

  public static final class DremioSqlFunction extends SqlFunction {
    private final ImplicitCoercionStrategy implicitCoercionStrategy;

    /** This means that the function will return the same output given the same input. */
    private final boolean isDeterministic;

    /**
     * These functions will return different results based on the environment, but computed only
     * once per query.
     */
    private final boolean isDynamic;

    private final SqlSyntax sqlSyntax;

    public DremioSqlFunction(
        String name,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        ImplicitCoercionStrategy implicitCoercionStrategy,
        boolean isDeterministic,
        boolean isDynamic,
        SqlSyntax sqlSyntax) {
      super(
          name,
          SqlKind.OTHER_FUNCTION,
          returnTypeInference,
          null,
          operandTypeChecker,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
      this.implicitCoercionStrategy = implicitCoercionStrategy;
      this.isDeterministic = isDeterministic;
      this.isDynamic = isDynamic;
      this.sqlSyntax = sqlSyntax;
    }

    public ImplicitCoercionStrategy getImplicitCoercionStrategy() {
      return implicitCoercionStrategy;
    }

    @Override
    public boolean isDeterministic() {
      return isDeterministic;
    }

    @Override
    public boolean isDynamicFunction() {
      return isDynamic;
    }

    @Override
    public SqlSyntax getSyntax() {
      return sqlSyntax;
    }

    @Override
    public void preValidateCall(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
      if (implicitCoercionStrategy == null) {
        return;
      }

      // Check the number of operands
      checkOperandCount(validator, getOperandTypeChecker(), call);
      SqlCallBinding opBinding = new SqlCallBinding(validator, scope, call);
      if (checkOperandTypes(opBinding, false)) {
        return;
      }

      Map<Integer, RelDataType> implicitCoercions = implicitCoercionStrategy.coerce(opBinding);
      for (Map.Entry<Integer, RelDataType> entry : implicitCoercions.entrySet()) {
        coerceOperandType(validator, call, entry.getKey(), entry.getValue());
      }
    }

    private void coerceOperandType(
        SqlValidator validator, SqlCall call, int index, RelDataType targetType) {
      SqlNode operand = call.getOperandList().get(index);
      SqlNode desired = castTo(operand, targetType);
      call.setOperand(index, desired);
      updateInferredType(validator, desired, targetType);
    }

    private SqlNode castTo(SqlNode node, RelDataType type) {
      return SqlStdOperatorTable.CAST.createCall(
          SqlParserPos.ZERO, node, new CustomSqlDataTypeSpec(type));
    }

    private final class CustomSqlDataTypeSpec extends SqlDataTypeSpec {
      private final RelDataType castType;

      public CustomSqlDataTypeSpec(RelDataType castType) {
        super(
            new SqlBasicTypeNameSpec(SqlTypeName.NULL, -1, -1, null, SqlParserPos.ZERO),
            SqlParserPos.ZERO);
        this.castType = castType;
      }

      @Override
      public RelDataType deriveType(SqlValidator validator) {
        return castType;
      }
    }

    private void updateInferredType(SqlValidator validator, SqlNode node, RelDataType type) {
      validator.setValidatedNodeType(node, type);
      final SqlValidatorNamespace namespace = validator.getNamespace(node);
      if (namespace != null) {
        namespace.setType(type);
      }
    }
  }
}
