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

import java.util.List;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.CompleteTypeInLogicalExpression;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.resolver.FunctionResolver;
import com.dremio.exec.resolver.FunctionResolverFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public final class TypeInferenceUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeInferenceUtils.class);

  private static final ImmutableMap<TypeProtos.MinorType, SqlTypeName> MINOR_TO_CALCITE_TYPE_MAPPING = ImmutableMap.<TypeProtos.MinorType, SqlTypeName> builder()
      .put(TypeProtos.MinorType.INT, SqlTypeName.INTEGER)
      .put(TypeProtos.MinorType.BIGINT, SqlTypeName.BIGINT)
      .put(TypeProtos.MinorType.FLOAT4, SqlTypeName.FLOAT)
      .put(TypeProtos.MinorType.FLOAT8, SqlTypeName.DOUBLE)
      .put(TypeProtos.MinorType.VARCHAR, SqlTypeName.VARCHAR)
      .put(TypeProtos.MinorType.BIT, SqlTypeName.BOOLEAN)
      .put(TypeProtos.MinorType.DATE, SqlTypeName.DATE)
      .put(TypeProtos.MinorType.DECIMAL, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL9, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL18, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL28SPARSE, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL38SPARSE, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.TIME, SqlTypeName.TIME)
      .put(TypeProtos.MinorType.TIMESTAMP, SqlTypeName.TIMESTAMP)
      .put(TypeProtos.MinorType.VARBINARY, SqlTypeName.VARBINARY)
      .put(TypeProtos.MinorType.INTERVALYEAR, SqlTypeName.INTERVAL_YEAR_MONTH)
      .put(TypeProtos.MinorType.INTERVALDAY, SqlTypeName.INTERVAL_DAY_SECOND)
      .put(TypeProtos.MinorType.STRUCT, SqlTypeName.MAP)
      .put(TypeProtos.MinorType.LIST, SqlTypeName.ARRAY)
      .put(TypeProtos.MinorType.LATE, SqlTypeName.ANY)

      // These are defined in the Dremio type system but have been turned off for now
      // .put(TypeProtos.MinorType.TINYINT, SqlTypeName.TINYINT)
      // .put(TypeProtos.MinorType.SMALLINT, SqlTypeName.SMALLINT)
      // Calcite types currently not supported by Dremio, nor defined in the Dremio type list:
      //      - CHAR, SYMBOL, MULTISET, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST
      .build();

  private static final ImmutableMap<SqlTypeName, TypeProtos.MinorType> CALCITE_TO_MINOR_MAPPING
      = ImmutableMap.<SqlTypeName, TypeProtos.MinorType> builder()
      .put(SqlTypeName.INTEGER, TypeProtos.MinorType.INT)
      .put(SqlTypeName.BIGINT, TypeProtos.MinorType.BIGINT)
      .put(SqlTypeName.FLOAT, TypeProtos.MinorType.FLOAT4)
      .put(SqlTypeName.DOUBLE, TypeProtos.MinorType.FLOAT8)
      .put(SqlTypeName.VARCHAR, TypeProtos.MinorType.VARCHAR)
      .put(SqlTypeName.BOOLEAN, TypeProtos.MinorType.BIT)
      .put(SqlTypeName.DATE, TypeProtos.MinorType.DATE)
      .put(SqlTypeName.DECIMAL, TypeProtos.MinorType.DECIMAL)
      .put(SqlTypeName.TIME, TypeProtos.MinorType.TIME)
      .put(SqlTypeName.TIMESTAMP, TypeProtos.MinorType.TIMESTAMP)
      .put(SqlTypeName.VARBINARY, TypeProtos.MinorType.VARBINARY)
      .put(SqlTypeName.INTERVAL_YEAR, TypeProtos.MinorType.INTERVALYEAR)
      .put(SqlTypeName.INTERVAL_YEAR_MONTH, TypeProtos.MinorType.INTERVALYEAR)
      .put(SqlTypeName.INTERVAL_MONTH, TypeProtos.MinorType.INTERVALYEAR)
      .put(SqlTypeName.INTERVAL_DAY, TypeProtos.MinorType.INTERVALDAY)
      .put(SqlTypeName.INTERVAL_DAY_HOUR, TypeProtos.MinorType.INTERVALDAY)
      .put(SqlTypeName.INTERVAL_DAY_MINUTE, TypeProtos.MinorType.INTERVALDAY)
      .put(SqlTypeName.INTERVAL_DAY_SECOND, TypeProtos.MinorType.INTERVALDAY)
      .put(SqlTypeName.INTERVAL_HOUR, TypeProtos.MinorType.INTERVALDAY)
      .put(SqlTypeName.INTERVAL_HOUR_MINUTE, TypeProtos.MinorType.INTERVALDAY)
      .put(SqlTypeName.INTERVAL_HOUR_SECOND, TypeProtos.MinorType.INTERVALDAY)
      .put(SqlTypeName.INTERVAL_MINUTE, TypeProtos.MinorType.INTERVALDAY)
      .put(SqlTypeName.INTERVAL_MINUTE_SECOND, TypeProtos.MinorType.INTERVALDAY)
      .put(SqlTypeName.INTERVAL_SECOND, TypeProtos.MinorType.INTERVALDAY)
      // SqlTypeName.CHAR is the type for Literals in Calcite, Dremio treats Literals as VARCHAR also
      .put(SqlTypeName.CHAR, TypeProtos.MinorType.VARCHAR)

      // (2) These 2 types are defined in the Dremio type system but have been turned off for now
      // .put(SqlTypeName.TINYINT, TypeProtos.MinorType.TINYINT)
      // .put(SqlTypeName.SMALLINT, TypeProtos.MinorType.SMALLINT)

      // (3) Calcite types currently not supported by Dremio, nor defined in the Dremio type list:
      //      - SYMBOL, MULTISET, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST
     .put(SqlTypeName.MAP, TypeProtos.MinorType.MAP)
     .put(SqlTypeName.ARRAY, TypeProtos.MinorType.LIST)
     .put(SqlTypeName.ROW, TypeProtos.MinorType.STRUCT)
      .build();

  private TypeInferenceUtils() {
    // utility class
  }

  /**
   * Given a Dremio's TypeProtos.MinorType, return a Calcite's corresponding SqlTypeName
   */
  public static SqlTypeName getCalciteTypeFromMinorType(final TypeProtos.MinorType type) {
    return CalciteArrowHelper.getCalciteTypeFromMinorType(type);
  }

  /**
   * Given a Calcite's RelDataType, return a Dremio's corresponding TypeProtos.MinorType
   */
  public static TypeProtos.MinorType getMinorTypeFromCalciteType(final RelDataType relDataType) {
    final SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    return getMinorTypeFromCalciteType(sqlTypeName);
  }

  /**
   * Given a Calcite's SqlTypeName, return a Dremio's corresponding TypeProtos.MinorType
   */
  public static TypeProtos.MinorType getMinorTypeFromCalciteType(final SqlTypeName sqlTypeName) {
    if(!CALCITE_TO_MINOR_MAPPING.containsKey(sqlTypeName)) {
      return TypeProtos.MinorType.LATE;
    }

    return CALCITE_TO_MINOR_MAPPING.get(sqlTypeName);
  }

  /**
   * Give the name and BaseFunctionHolder list, return the inference mechanism.
   */
  public static SqlReturnTypeInference getSqlReturnTypeInference(
    final List<AbstractFunctionHolder> functions) {
    return new DefaultSqlReturnTypeInference(functions);
  }

  private static class DefaultSqlReturnTypeInference implements SqlReturnTypeInference {
    private final List<AbstractFunctionHolder> functions;

    // This is created per query, so safe to use decimal setting as a variable.
    public DefaultSqlReturnTypeInference(List<AbstractFunctionHolder> functions) {
      this.functions = functions;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      if (functions.isEmpty()) {
        return factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.ANY),
            true);
      }

      // The following logic is just a safe play:
      // Even if any of the input arguments has ANY type,
      // it "might" still be possible to determine the return type based on other non-ANY types
      for (RelDataType type : opBinding.collectOperandTypes()) {
        if (getMinorTypeFromCalciteType(type) == TypeProtos.MinorType.LATE) {
          // This code for boolean output type is added for addressing DRILL-1729
          // In summary, if we have a boolean output function in the WHERE-CLAUSE,
          // this logic can validate and execute user queries seamlessly
          boolean allBooleanOutput = true;
          for (AbstractFunctionHolder function : functions) {
            if (!function.isReturnTypeIndependent() || function.getReturnType(null).toMinorType() != TypeProtos.MinorType.BIT) {
              allBooleanOutput = false;
              break;
            }
          }

          if(allBooleanOutput) {
            return factory.createTypeWithNullability(
                factory.createSqlType(SqlTypeName.BOOLEAN), true);
          } else {
            return factory.createTypeWithNullability(
                factory.createSqlType(SqlTypeName.ANY),
                true);
          }
        }
      }

      final AbstractFunctionHolder func = resolveFunctionHolder(opBinding, functions);
      return getReturnType(opBinding, func);
    }

    private static RelDataType getReturnType(final SqlOperatorBinding opBinding, final AbstractFunctionHolder func) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();

      // least restrictive type (nullable ANY type)
      final RelDataType nullableAnyType = factory.createTypeWithNullability(
              factory.createSqlType(SqlTypeName.ANY),
              true);

      if(!func.isReturnTypeIndependent()){
        return nullableAnyType;
      }

      CompleteType returnType = func.getReturnType(null);
      if(returnType.isList() || returnType.isComplex() || returnType.isUnion()  || returnType.isLate()){
        return nullableAnyType;
      }

      final TypeProtos.MinorType minorType = returnType.toMinorType();
      final SqlTypeName sqlTypeName = getCalciteTypeFromMinorType(minorType);
      if (sqlTypeName == null) {
        return nullableAnyType;
      }

      return createCalciteTypeWithNullability(factory, sqlTypeName, true, returnType.getPrecision());
    }
  }

  private static AbstractFunctionHolder resolveFunctionHolder(
    final SqlOperatorBinding opBinding,
    final List<AbstractFunctionHolder>  functions) {
    final FunctionCall functionCall = convertSqlOperatorBindingToFunctionCall(opBinding);
    final FunctionResolver functionResolver = FunctionResolverFactory.getResolver(functionCall);
    final AbstractFunctionHolder func = functionResolver.getBestMatch(functions, functionCall);

    // Throw an exception
    // if no BaseFunctionHolder matched for the given list of operand types
    if (func == null) {
      String operandTypes = "";
      for(int i = 0; i < opBinding.getOperandCount(); ++i) {
        operandTypes += opBinding.getOperandType(i).getSqlTypeName();
        if(i < opBinding.getOperandCount() - 1) {
          operandTypes += ",";
        }
      }

      throw UserException
          .functionError()
          .message(String.format("%s does not support operand types (%s)",
              opBinding.getOperator().getName(),
              operandTypes))
          .buildSilently();
    }
    return func;
  }


  /**
   * Given a {@link SqlTypeName} and nullability, create a RelDataType from the RelDataTypeFactory
   *
   * @param typeFactory RelDataTypeFactory used to create the RelDataType
   * @param sqlTypeName the given SqlTypeName
   * @param isNullable  the nullability of the created RelDataType
   * @param precision precision, null if the type does not support precision, or to fallback to default precision
   * @return RelDataType Type of call
   */
  public static RelDataType createCalciteTypeWithNullability(RelDataTypeFactory typeFactory,
                                                             SqlTypeName sqlTypeName,
                                                             boolean isNullable,
                                                             Integer precision) {
    RelDataType type;
    switch (sqlTypeName) {
      case TIMESTAMP:
        type = precision == null
            ? typeFactory.createSqlType(sqlTypeName)
            : typeFactory.createSqlType(sqlTypeName, precision);
          break;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        type = typeFactory.createSqlIntervalType(
          new SqlIntervalQualifier(
            TimeUnit.YEAR,
            TimeUnit.MONTH,
            SqlParserPos.ZERO));
        break;
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        type = typeFactory.createSqlIntervalType(
          new SqlIntervalQualifier(
            TimeUnit.DAY,
            TimeUnit.MINUTE,
            SqlParserPos.ZERO));
        break;
      case VARCHAR:
        type = typeFactory.createSqlType(sqlTypeName, TypeHelper.VARCHAR_DEFAULT_CAST_LEN);
        break;
      default:
        type = typeFactory.createSqlType(sqlTypeName);
        break;
    }
    return typeFactory.createTypeWithNullability(type, isNullable);
  }

  /**
   * Given a SqlOperatorBinding, convert it to FunctionCall
   * @param  opBinding    the given SqlOperatorBinding
   * @return FunctionCall the converted FunctionCall
   */
  public static FunctionCall convertSqlOperatorBindingToFunctionCall(final SqlOperatorBinding opBinding) {
    final List<LogicalExpression> args = Lists.newArrayList();

    for (int i = 0; i < opBinding.getOperandCount(); ++i) {
      final RelDataType type = opBinding.getOperandType(i);
      final TypeProtos.MinorType minorType = getMinorTypeFromCalciteType(type);
      CompleteType completeType = CompleteType.fromMinorType(minorType);
      args.add(new CompleteTypeInLogicalExpression(completeType));
    }

    final String funcName = FunctionCallFactory.replaceOpWithFuncName(opBinding.getOperator().getName());
    final FunctionCall functionCall = new FunctionCall(funcName, args);
    return functionCall;
  }
}
