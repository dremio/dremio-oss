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
package com.dremio.service.autocomplete.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

/**
 * Factory to create a function from a SqlFunction.
 */
public final class FunctionFactory {
  private static final Map<ImmutableList<SqlTypeName>, SqlNodeList> cachedTypesToNodeList = new ConcurrentHashMap<>();
  private static final Map<String, Function> cache = new ConcurrentHashMap<>();
  private static final class LiteralSqlNodes {
    public static final SqlNode EXACT = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
    public static final SqlNode APPROX = SqlLiteral.createApproxNumeric("123.0", SqlParserPos.ZERO);
    public static final SqlNode VARCHAR = SqlLiteral.createCharString("hello", SqlParserPos.ZERO);
    public static final SqlNode BINARY = SqlLiteral.createBinaryString(new byte[] {}, SqlParserPos.ZERO);
    public static final SqlNode BOOLEAN = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    public static final SqlNode DATE = SqlLiteral.createDate(new DateString(2000, 1, 1), SqlParserPos.ZERO);
    public static final SqlNode TIME = SqlLiteral.createTime(new TimeString(1, 1, 1), 1, SqlParserPos.ZERO);
    public static final SqlNode TIMESTAMP = SqlLiteral.createTimestamp(new TimestampString(1, 1, 1, 1, 1, 1), 1, SqlParserPos.ZERO);
    public static final SqlNode UNKNOWN = SqlLiteral.createUnknown(SqlParserPos.ZERO);
    private LiteralSqlNodes() {}
  }

  private FunctionFactory() {}

  public static Function createFromSqlFunction(
    SqlFunction sqlFunction,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope,
    boolean enableSignatures) {
    Preconditions.checkNotNull(sqlFunction);
    Optional<Function> optionalFunction = FunctionDictionary.INSTANCE.tryGetValue(sqlFunction.getName());
    if (optionalFunction.isPresent()) {
      return optionalFunction.get();
    }

    if (!enableSignatures) {
      return new Function(
        sqlFunction.getName(),
        ImmutableList.of(),
        FunctionDescriptions.tryGetDescription(sqlFunction.getName()).orElse(null),
        FunctionSyntaxes.tryGetSyntax(sqlFunction.getName()).orElse(null));
    }

    Function function = cache.get(sqlFunction.getName().toUpperCase());
    if (function == null) {
      function = createFromSqlFunctionImpl(
        sqlFunction,
        sqlValidator,
        sqlValidatorScope);
      cache.put(sqlFunction.getName().toUpperCase(), function);
    }

    return function;
  }

  private static Function createFromSqlFunctionImpl(
    SqlFunction sqlFunction,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope) {
    Optional<ImmutableList<FunctionSignature>> optionalFunctionSignatures = tryGetSignatures(
      sqlFunction,
      sqlValidator,
      sqlValidatorScope);

    Optional<ImmutableList<FunctionSignature>> functionSignatures = optionalFunctionSignatures;
    Optional<String> description = FunctionDescriptions.tryGetDescription(sqlFunction.getName());
    Optional<String> syntax = FunctionSyntaxes.tryGetSyntax(sqlFunction.getName());

    return new Function(
      sqlFunction.getName(),
      functionSignatures.orElse(ImmutableList.of()),
      description.orElse(null),
      syntax.orElse(null));
  }

  private static Optional<ImmutableList<FunctionSignature>> tryGetSignatures(
    SqlFunction sqlFunction,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope) {
    return firstSuccess(
      () -> tryGetSignaturesFromGetParamTypes(sqlFunction),
      () -> tryGetSignaturesFromGetAllowedSignatures(
        sqlFunction,
        sqlValidator,
        sqlValidatorScope),
      () -> tryGetSignaturesFromCheckOperandTypes(
        sqlFunction,
        sqlValidator,
        sqlValidatorScope));
  }

  private static <T> Optional<T> firstSuccess(Supplier<Optional<T>> ... suppliers) {
    for (Supplier<Optional<T>> supplier : suppliers) {
      Optional<T> optional = supplier.get();
      if (optional.isPresent()) {
        return optional;
      }
    }

    return Optional.empty();
  }

  private static Optional<ImmutableList<FunctionSignature>> tryGetSignaturesFromGetParamTypes(
    SqlFunction sqlFunction) {
    try {
      List<RelDataType> paramTypes = sqlFunction.getParamTypes();
      if (paramTypes == null) {
        return Optional.empty();
      }

      SqlTypeName returnType = sqlFunction.inferReturnType(JavaTypeFactoryImpl.INSTANCE, paramTypes).getSqlTypeName();
      ImmutableList<SqlTypeName> operandTypes = ImmutableList.copyOf(paramTypes.stream().map(RelDataType::getSqlTypeName).collect(Collectors.toList()));
      String template = sqlFunction.getSignatureTemplate(paramTypes.size());
      FunctionSignature functionSignature = new FunctionSignature(
        returnType,
        operandTypes,
        template);

      return Optional.of(ImmutableList.of(functionSignature));
    } catch (Exception ex) {
      return Optional.empty();
    }
  }

  private static Optional<ImmutableList<FunctionSignature>> tryGetSignaturesFromGetAllowedSignatures(
    SqlFunction sqlFunction,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope) {
    try {
      String allowedSignaturesString = sqlFunction.getAllowedSignatures();
      if (allowedSignaturesString.contains("Dremio") || allowedSignaturesString.contains("*")) {
        return Optional.empty();
      }

      String[] allowedSignatures = allowedSignaturesString.split("\\r?\\n");
      // FUNCTION_NAME(<PARAM1_TYPE> <PARAM2_TYPE>)\nFUNCTION_NAME(<PARAM1_TYPE> <PARAM2_TYPE> <PARAM3_TYPE>)
      ImmutableList.Builder<FunctionSignature> functionSignatureBuilder = new ImmutableList.Builder<>();
      for (String allowedSignature : allowedSignatures) {
        ImmutableList.Builder<SqlTypeName> operandTypesBuilder = new ImmutableList.Builder<>();

        int startIndex = 0;
        for (int i = 0; i < allowedSignature.length(); i++) {
          char character = allowedSignature.charAt(i);
          switch (character) {
          case '<':
            startIndex = i;
            break;

          case '>':
            int endIndex = i;
            String operandTypeString = allowedSignature.substring(startIndex + 1, endIndex);
            SqlTypeName sqlTypeName = SqlTypeName.valueOf(operandTypeString);
            operandTypesBuilder.add(sqlTypeName);
            break;

          default:
            // Do Nothing
            break;
          }
        }

        ImmutableList<SqlTypeName> operandTypes = operandTypesBuilder.build();
        SqlNodeList sqlNodeList = createSqlNodeList(operandTypes);
        SqlCall sqlCall = sqlFunction.createCall(sqlNodeList);
        RelDataType returnType = sqlFunction.deriveType(sqlValidator, sqlValidatorScope, sqlCall);

        String template = sqlFunction.getSignatureTemplate(operandTypes.size());

        FunctionSignature functionSignature = new FunctionSignature(
          returnType.getSqlTypeName(),
          operandTypes,
          template);
        functionSignatureBuilder.add(functionSignature);
      }

      return Optional.of(functionSignatureBuilder.build());
    } catch (Error | Exception e) {
      // Not every function implements this shortcut
      return Optional.empty();
    }
  }

  private static Optional<ImmutableList<FunctionSignature>> tryGetSignaturesFromCheckOperandTypes(
    SqlFunction sqlFunction,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope) {

    SqlOperandCountRange sqlOperandCountRange;
    try {
      sqlOperandCountRange = sqlFunction.getOperandCountRange();
    } catch (Exception ex) {
      return Optional.empty();
    }

    int maxOperands = sqlOperandCountRange.getMax() < 0 ? sqlOperandCountRange.getMin() + 1 : sqlOperandCountRange.getMax();
    if (maxOperands > 3) {
      // It's going to take too long, so just give up
      return Optional.empty();
    }

    int minOperands = sqlOperandCountRange.getMin();
    ImmutableList.Builder<FunctionSignature> functionSignatures = new ImmutableList.Builder<>();
    for (int operandCount = minOperands; operandCount <= maxOperands; operandCount++) {
      ParameterTypeCombinations parameterTypeCombinations = ParameterTypeCombinationDictionary.INSTANCE.get(operandCount);
      for (ParameterTypes parameterTypes : parameterTypeCombinations.getValues()) {
        SqlNodeList sqlNodeList = createSqlNodeList(parameterTypes.getSqlTypeNames());
        SqlCall sqlCall;
        try {
          sqlCall = sqlFunction.createCall(sqlNodeList);
        } catch (Error | Exception e) {
          // Some functions like SqlTrimFunction don't implement createCall
          continue;
        }
        SqlCallBinding sqlCallBinding = new SqlCallBinding(
          sqlValidator,
          sqlValidatorScope,
          sqlCall);

        if (sqlFunction.checkOperandTypes(sqlCallBinding, false)) {
          RelDataType returnType;
          try {
            returnType = sqlFunction.deriveType(sqlValidator, sqlValidatorScope, sqlCall);
          } catch (Error | Exception e) {
            // For whatever reason some functions always return true for checkOperandTypes, but throw an exception for deriveType
            continue;
          }

          String template = sqlFunction.getSignatureTemplate(operandCount);

          FunctionSignature functionSignature = new FunctionSignature(
            returnType.getSqlTypeName(),
            parameterTypes.getSqlTypeNames(),
            template);
          functionSignatures.add(functionSignature);
        }
      }
    }

    return Optional.of(functionSignatures.build());
  }

  private static SqlNodeList createSqlNodeList(ImmutableList<SqlTypeName> sqlTypeNames) {
    SqlNodeList sqlNodeList = cachedTypesToNodeList.get(sqlTypeNames);
    if (sqlNodeList != null) {
      return sqlNodeList;
    }
    List<SqlNode> sqlNodes = new ArrayList<>();
    for (SqlTypeName sqlTypeName : sqlTypeNames) {
      SqlNode sqlNode = convertTypeToNode(sqlTypeName);
      sqlNodes.add(sqlNode);
    }
    sqlNodeList = new SqlNodeList(sqlNodes, SqlParserPos.ZERO);
    cachedTypesToNodeList.put(sqlTypeNames, sqlNodeList);
    return sqlNodeList;
  }

  private static SqlNode convertTypeToNode(SqlTypeName sqlTypeName) {
    switch (sqlTypeName) {
    case DATE:
      return LiteralSqlNodes.DATE;
    case VARCHAR:
      return LiteralSqlNodes.VARCHAR;
    case BINARY:
      return LiteralSqlNodes.BINARY;
    case BOOLEAN:
      return LiteralSqlNodes.BOOLEAN;
    case TIME:
      return LiteralSqlNodes.TIME;
    case TIMESTAMP:
      return LiteralSqlNodes.TIMESTAMP;
    default:
      if (SqlTypeName.EXACT_TYPES.contains(sqlTypeName)) {
        return LiteralSqlNodes.EXACT;
      }
      if (SqlTypeName.APPROX_TYPES.contains(sqlTypeName)) {
        return LiteralSqlNodes.APPROX;
      }
      return LiteralSqlNodes.UNKNOWN;
    }
  }

  private static final class ParameterTypeCombinationDictionary {
    public static final ParameterTypeCombinationDictionary INSTANCE = new ParameterTypeCombinationDictionary();

    private final ImmutableList<ParameterTypeCombinations> parameterTypeCombinationsList;

    private ParameterTypeCombinationDictionary() {
      ImmutableList<SqlTypeName> possibleTypes = ImmutableList.<SqlTypeName>builder()
        .add(SqlTypeName.INTEGER)
        .add(SqlTypeName.BOOLEAN)
        .add(SqlTypeName.BINARY)
        .add(SqlTypeName.DATE)
        .add(SqlTypeName.DOUBLE)
        .add(SqlTypeName.FLOAT)
        .add(SqlTypeName.TIME)
        .add(SqlTypeName.VARCHAR)
        .add(SqlTypeName.DECIMAL)
        .build();

      List<ParameterTypeCombinations> combinationsOfLength = new ArrayList<>(
        ImmutableList.of(
          new ParameterTypeCombinations(
            ImmutableList.of(
              ParameterTypes.create(
                ImmutableList.of())))));

      for (int length = 1; length <= 3; length++) {
        ParameterTypeCombinations combinationsOfOneLessLength = combinationsOfLength.get(length - 1);
        ImmutableList.Builder<ParameterTypes> parameterTypesList = new ImmutableList.Builder<>();
        for (SqlTypeName type : possibleTypes) {
          for (ParameterTypes parameterTypesOfOneLessLength : combinationsOfOneLessLength.getValues()) {
            ImmutableList<SqlTypeName> parameterTypeValues = new ImmutableList.Builder<SqlTypeName>()
              .add(type)
              .addAll(parameterTypesOfOneLessLength.getSqlTypeNames())
              .build();

            ParameterTypes parameterTypes = ParameterTypes.create(parameterTypeValues);
            parameterTypesList.add(parameterTypes);
          }
        }

        ParameterTypeCombinations parameterTypeCombinations = new ParameterTypeCombinations(parameterTypesList.build());
        combinationsOfLength.add(parameterTypeCombinations);
      }

      this.parameterTypeCombinationsList = ImmutableList.copyOf(combinationsOfLength);
    }

    public ParameterTypeCombinations get(int length) {
      return parameterTypeCombinationsList.get(length);
    }
  }

  private static final class ParameterTypeCombinations {
    private final ImmutableList<ParameterTypes> values;

    private ParameterTypeCombinations(ImmutableList<ParameterTypes> values) {
      Preconditions.checkNotNull(values);

      this.values = values;
    }

    public ImmutableList<ParameterTypes> getValues() {
      return values;
    }
  }

  private static final class ParameterTypes {
    private final ImmutableList<SqlTypeName> sqlTypeNames;
    private final ImmutableList<RelDataType> relDataTypes;

    private ParameterTypes(ImmutableList<SqlTypeName> sqlTypeNames, ImmutableList<RelDataType> relDataTypes) {
      Preconditions.checkNotNull(sqlTypeNames);
      Preconditions.checkNotNull(relDataTypes);

      this.sqlTypeNames = sqlTypeNames;
      this.relDataTypes = relDataTypes;
    }

    public ImmutableList<SqlTypeName> getSqlTypeNames() {
      return sqlTypeNames;
    }
    public ImmutableList<RelDataType> getRelDataType() {
      return relDataTypes;
    }

    public static ParameterTypes create(ImmutableList<SqlTypeName> sqlTypeNames) {
      ImmutableList<RelDataType> relDataTypes = sqlTypeNames
        .stream()
        .map(JavaTypeFactoryImpl.INSTANCE::createSqlType)
        .collect(ImmutableList.toImmutableList());

      return new ParameterTypes(sqlTypeNames, relDataTypes);
    }
  }
}
