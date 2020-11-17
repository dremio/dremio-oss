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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

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
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class TypeInferenceUtils {
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
      // .put(SqlTypeName.MAP, TypeProtos.MinorType.STRUCT)
       .put(SqlTypeName.ARRAY, TypeProtos.MinorType.LIST)
       .put(SqlTypeName.ROW, TypeProtos.MinorType.STRUCT)
      .build();

  private static final ImmutableMap<String, SqlReturnTypeInference> funcNameToInference = ImmutableMap.<String, SqlReturnTypeInference> builder()
      .put("CONCAT", ConcatSqlReturnTypeInference.INSTANCE)
      .put("LENGTH", LengthSqlReturnTypeInference.INSTANCE)
      .put("REPLACE", ReplaceSqlReturnTypeInference.INSTANCE)
      .put("LPAD", PadTrimSqlReturnTypeInference.INSTANCE)
      .put("RPAD", PadTrimSqlReturnTypeInference.INSTANCE)
      .put("LTRIM", PadTrimSqlReturnTypeInference.INSTANCE)
      .put("RTRIM", PadTrimSqlReturnTypeInference.INSTANCE)
      .put("BTRIM", PadTrimSqlReturnTypeInference.INSTANCE)
      .put("TRIM", PadTrimSqlReturnTypeInference.INSTANCE)
      .put("CONVERT_TO", ConvertToSqlReturnTypeInference.INSTANCE)
      .put("FLATTEN", FlattenReturnTypeInference.INSTANCE)
      .put("KVGEN", DeferToExecSqlReturnTypeInference.INSTANCE)
      .put("CONVERT_FROM", ConvertFromReturnTypeInference.INSTANCE)
      .put("IS DISTINCT FROM", IsDistinctFromSqlReturnTypeInference.INSTANCE)
      .put("IS NOT DISTINCT FROM", IsDistinctFromSqlReturnTypeInference.INSTANCE)
      .put("TRUNC", SqlStdOperatorTable.TRUNCATE.getReturnTypeInference())
      .build();

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
    final String name,
    final List<AbstractFunctionHolder> functions,
    final boolean isDecimalV2Enabled) {

    final String nameCap = name.toUpperCase();
    if(funcNameToInference.containsKey(nameCap)) {
      return funcNameToInference.get(nameCap);
    } else {
      return new DefaultSqlReturnTypeInference(functions, isDecimalV2Enabled);
    }
  }

  private static class DefaultSqlReturnTypeInference implements SqlReturnTypeInference {
    private final List<AbstractFunctionHolder> functions;
    @FieldSerializer.Optional("ignored")
    private final boolean isDecimalV2Enabled;

    // This is created per query, so safe to use decimal setting as a variable.
    public DefaultSqlReturnTypeInference(List<AbstractFunctionHolder> functions, boolean isDecimalV2Enabled) {
      this.functions = functions;
      this.isDecimalV2Enabled = isDecimalV2Enabled;
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

      final AbstractFunctionHolder func = resolveFunctionHolder(opBinding, functions, isDecimalV2Enabled);
      final RelDataType returnType = getReturnType(opBinding, func);
      return returnType.getSqlTypeName() == SqlTypeName.VARBINARY
          ? createCalciteTypeWithNullability(factory, SqlTypeName.ANY, returnType.isNullable(), null)
              : returnType;
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

  private static class ConvertFromReturnTypeInference implements SqlReturnTypeInference {
    private static final ConvertFromReturnTypeInference INSTANCE = new ConvertFromReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      SqlTypeName typeToCastTo = null;
      if (opBinding instanceof SqlCallBinding) {
        SqlCallBinding sqlCallBinding = (SqlCallBinding) opBinding;
        if (sqlCallBinding.operand(1).getKind() == SqlKind.LITERAL) {
          String type = null;
          try {
            SqlLiteral sqlLiteral = (SqlLiteral) sqlCallBinding.operand(1);
            type = ((NlsString) sqlLiteral.getValue()).getValue();
            switch(type) {
              case "JSON":
                typeToCastTo = SqlTypeName.ANY;
                break;
              case "UTF8":
              case "UTF16":
                typeToCastTo = SqlTypeName.VARCHAR;
                break;
              case "BOOLEAN_BYTE":
                typeToCastTo = SqlTypeName.BOOLEAN;
                break;
              case "TINYINT_BE":
              case "TINYINT":
                typeToCastTo = SqlTypeName.TINYINT;
                break;
              case "SMALLINT_BE":
              case "SMALLINT":
                typeToCastTo = SqlTypeName.SMALLINT;
                break;
              case "INT_BE":
              case "INT":
              case "INT_HADOOPV":
                typeToCastTo = SqlTypeName.INTEGER;
                break;
              case "BIGINT_BE":
              case "BIGINT":
              case "BIGINT_HADOOPV":
                typeToCastTo = SqlTypeName.BIGINT;
                break;
              case "FLOAT":
                typeToCastTo = SqlTypeName.FLOAT;
                break;
              case "DOUBLE":
                typeToCastTo = SqlTypeName.DOUBLE;
                break;
              case "DATE_EPOCH_BE":
              case "DATE_EPOCH":
                typeToCastTo = SqlTypeName.DATE;
                break;
              case "TIME_EPOCH_BE":
              case "TIME_EPOCH":
                typeToCastTo = SqlTypeName.TIME;
                break;
              case "TIMESTAMP_EPOCH":
              case "TIMESTAMP_IMPALA":
                typeToCastTo = SqlTypeName.TIMESTAMP;
                break;
              default:
                typeToCastTo = SqlTypeName.ANY;
                break;
            }
          } catch (final ClassCastException e) {
            logger.debug("Failed to parse string for convert_from()");
          }
        }
      }

      if (typeToCastTo == null) {
        typeToCastTo = SqlTypeName.ANY;
      }
      return factory.createTypeWithNullability(
          factory.createSqlType(typeToCastTo),
          true);
    }
  }

  private static class DeferToExecSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DeferToExecSqlReturnTypeInference INSTANCE = new DeferToExecSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      return factory.createTypeWithNullability(
        factory.createSqlType(SqlTypeName.ANY),
        true);
    }
  }

  private static class ConcatSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final ConcatSqlReturnTypeInference INSTANCE = new ConcatSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();

      boolean isNullable = true;
      int precision = 0;
      for(RelDataType relDataType : opBinding.collectOperandTypes()) {
        if(!relDataType.isNullable()) {
          isNullable = false;
        }

        // If the underlying columns cannot offer information regarding the precision (i.e., the length) of the VarChar,
        // Dremio uses the largest to represent it
        if(relDataType.getPrecision() == TypeHelper.VARCHAR_DEFAULT_CAST_LEN
            || relDataType.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
          precision = TypeHelper.VARCHAR_DEFAULT_CAST_LEN;
        } else {
          precision += relDataType.getPrecision();
        }
      }

      return factory.createTypeWithNullability(
          factory.createSqlType(SqlTypeName.VARCHAR, precision),
          isNullable);
    }
  }

  private static class LengthSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final LengthSqlReturnTypeInference INSTANCE = new LengthSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final SqlTypeName sqlTypeName = SqlTypeName.INTEGER;

      // We need to check only the first argument because
      // the second one is used to represent encoding type
      final boolean isNullable = opBinding.getOperandType(0).isNullable();
      return createCalciteTypeWithNullability(factory, sqlTypeName, isNullable, null);
    }
  }

  private static class ReplaceSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final ReplaceSqlReturnTypeInference INSTANCE = new ReplaceSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final SqlTypeName sqlTypeName = SqlTypeName.VARCHAR;

      for(int i = 0; i < opBinding.getOperandCount(); ++i) {
        if(opBinding.getOperandType(i).isNullable()) {
          return createCalciteTypeWithNullability(factory, sqlTypeName, true, null);
        }
      }

      return createCalciteTypeWithNullability(factory, sqlTypeName, false, 65536);
    }
  }

  private static class PadTrimSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final PadTrimSqlReturnTypeInference INSTANCE = new PadTrimSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final SqlTypeName sqlTypeName = SqlTypeName.VARCHAR;

      for(int i = 0; i < opBinding.getOperandCount(); ++i) {
        if(opBinding.getOperandType(i).isNullable()) {
          return createCalciteTypeWithNullability(factory, sqlTypeName, true, null);
        }
      }

      return createCalciteTypeWithNullability(factory, sqlTypeName, false, null);
    }
  }

  private static class ConvertToSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final ConvertToSqlReturnTypeInference INSTANCE = new ConvertToSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final SqlTypeName type = SqlTypeName.VARBINARY;

      return createCalciteTypeWithNullability(factory, type, opBinding.getOperandType(0).isNullable(), null);
    }
  }
  private static class FlattenReturnTypeInference implements SqlReturnTypeInference {
    private static final FlattenReturnTypeInference INSTANCE = new FlattenReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataType operandType = opBinding.getOperandType(0);
      if (operandType instanceof ArraySqlType) {
        return ((ArraySqlType) operandType).getComponentType();
      } else {
        return DynamicReturnType.INSTANCE.inferReturnType(opBinding);
      }
    }
  }
  private static class IsDistinctFromSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final IsDistinctFromSqlReturnTypeInference INSTANCE = new IsDistinctFromSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      // The result of IS [NOT] DISTINCT FROM is NOT NULL because it can only return TRUE or FALSE.
      return opBinding.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    }
  }

  private static AbstractFunctionHolder resolveFunctionHolder(final SqlOperatorBinding opBinding,
                                                          final List<AbstractFunctionHolder>  functions,
                                                          boolean isDecimalV2On) {
    final FunctionCall functionCall = convertSqlOperatorBindingToFunctionCall(opBinding);
    final FunctionResolver functionResolver = FunctionResolverFactory.getResolver(functionCall);
    final AbstractFunctionHolder func = functionResolver.getBestMatch(functions, functionCall);

    // Throw an exception
    // if no BaseFunctionHolder matched for the given list of operand types
    if(func == null) {
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
          .build(logger);
    }
    return func;
  }

  /**
   * For Extract and date_part functions, infer the return types based on timeUnit
   */
  public static SqlTypeName getSqlTypeNameForTimeUnit(String timeUnit) {
    switch (timeUnit.toUpperCase()){
      case "YEAR":
      case "MONTH":
      case "DAY":
      case "HOUR":
      case "MINUTE":
      case "SECOND":
      case "CENTURY":
      case "DECADE":
      case "DOW":
      case "DOY":
      case "MILLENNIUM":
      case "QUARTER":
      case "WEEK":
      case "EPOCH":
        return SqlTypeName.BIGINT;
      default:
        throw UserException
            .functionError()
            .message("extract function supports the following time units: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND")
            .build(logger);
    }
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
      default:
        type = typeFactory.createSqlType(sqlTypeName);
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

  /**
   * This class is not intended to be instantiated
   */
  private TypeInferenceUtils() {

  }
}
