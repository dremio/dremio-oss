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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
import com.google.common.collect.ImmutableList;

/**
 * Factory to create a function from a SqlFunction.
 */
public final class FunctionFactory {
  private static final Map<String, Function> cachedFunctions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  private static final Map<ParameterTypes, SqlNodeList> cachedTypesToNodeList = new HashMap<>();

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
    public static final SqlNode NULL = SqlLiteral.createNull(SqlParserPos.ZERO);

    private LiteralSqlNodes() {}
  }

  private FunctionFactory() {}

  public static Function createFromSqlFunction(
    SqlFunction sqlFunction,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope) {
    Preconditions.checkNotNull(sqlFunction);

    Function function = cachedFunctions.get(sqlFunction.getName());
    if (function == null) {
      function = createFromSqlFunctionImpl(sqlFunction, sqlValidator, sqlValidatorScope);
      cachedFunctions.put(sqlFunction.getName(), function);
    }

    return function;
  }

  private static Function createFromSqlFunctionImpl(
    SqlFunction sqlFunction,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope) {
    SqlOperandCountRange sqlOperandCountRange = sqlFunction.getOperandCountRange();
    int maxOperands = Math.min(sqlOperandCountRange.getMax() < 0 ? sqlOperandCountRange.getMin() + 1 : sqlOperandCountRange.getMax(), 10);
    int minOperands = sqlOperandCountRange.getMin();

    ImmutableList.Builder<FunctionSignature> functionSignatures = new ImmutableList.Builder<>();
    for (int operandCount = minOperands; operandCount <= maxOperands; operandCount++) {
      ParameterTypeCombinations parameterTypeCombinations = ParameterTypeCombinationDictionary.getCombinationsOfLength(operandCount);
      for (ParameterTypes parameterTypes : parameterTypeCombinations.getValues()) {
        SqlNodeList sqlNodeList = createSqlNodeList(parameterTypes);
        SqlCall sqlCall = sqlFunction.createCall(sqlNodeList);
        SqlCallBinding sqlCallBinding = new SqlCallBinding(
          sqlValidator,
          sqlValidatorScope,
          sqlCall);
        if (sqlFunction.checkOperandTypes(sqlCallBinding, false)) {
          // Replace this with derive type once the other PR gets merged in
          List<RelDataType> operandTypes = new ArrayList<>();
          for (SqlTypeName sqlTypeName : parameterTypes.getValues()) {
            RelDataType operandType = JavaTypeFactoryImpl.INSTANCE.createSqlType(sqlTypeName);
            operandTypes.add(operandType);
          }

          RelDataType returnType = sqlFunction.inferReturnType(
            JavaTypeFactoryImpl.INSTANCE,
            operandTypes);
          FunctionSignature functionSignature = new FunctionSignature(
            returnType.getSqlTypeName(),
            parameterTypes.getValues());
          functionSignatures.add(functionSignature);
        }
      }
    }

    Function function = new Function(sqlFunction.getName(), functionSignatures.build());
    return function;
  }

  private static SqlNodeList createSqlNodeList(ParameterTypes parameterTypes) {
    SqlNodeList sqlNodeList = cachedTypesToNodeList.get(parameterTypes);
    if (sqlNodeList != null) {
      return sqlNodeList;
    }

    List<SqlNode> sqlNodes = new ArrayList<>();
    for (SqlTypeName sqlTypeName : parameterTypes.getValues()) {
      SqlNode sqlNode = convertTypeToNode(sqlTypeName);
      sqlNodes.add(sqlNode);
    }

    sqlNodeList = new SqlNodeList(sqlNodes, SqlParserPos.ZERO);
    cachedTypesToNodeList.put(parameterTypes, sqlNodeList);
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
    case NULL:
      return LiteralSqlNodes.NULL;
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
    private static final ImmutableList<SqlTypeName> possibleTypes = ImmutableList.<SqlTypeName>builder()
      .add(SqlTypeName.INTEGER)
      .add(SqlTypeName.BOOLEAN)
      .add(SqlTypeName.BINARY)
      .add(SqlTypeName.NULL)
      .add(SqlTypeName.DATE)
      .add(SqlTypeName.DOUBLE)
      .add(SqlTypeName.FLOAT)
      .add(SqlTypeName.TIME)
      .add(SqlTypeName.VARCHAR)
      .add(SqlTypeName.DECIMAL)
      .build();

    private static final List<ParameterTypeCombinations> combinationsOfLength = new ArrayList<>(
      ImmutableList.of(
        new ParameterTypeCombinations(
          ImmutableList.of(
            new ParameterTypes(
              ImmutableList.of())))));

    public static ParameterTypeCombinations getCombinationsOfLength(int length) {
      if (combinationsOfLength.size() > length) {
        return combinationsOfLength.get(length);
      }

      ParameterTypeCombinations combinationsOfOneLessLength = getCombinationsOfLength(length - 1);
      ImmutableList.Builder<ParameterTypes> parameterTypesList = new ImmutableList.Builder<>();
      for (SqlTypeName type : possibleTypes) {
        for (ParameterTypes parameterTypesOfOneLessLength : combinationsOfOneLessLength.getValues()) {
          ImmutableList<SqlTypeName> parameterTypeValues = new ImmutableList.Builder<SqlTypeName>()
            .add(type)
            .addAll(parameterTypesOfOneLessLength.values)
            .build();

          ParameterTypes parameterTypes = new ParameterTypes(parameterTypeValues);
          parameterTypesList.add(parameterTypes);
        }
      }

      ParameterTypeCombinations parameterTypeCombinations = new ParameterTypeCombinations(parameterTypesList.build());
      combinationsOfLength.add(parameterTypeCombinations);
      return parameterTypeCombinations;
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
    private final ImmutableList<SqlTypeName> values;

    public ParameterTypes(ImmutableList<SqlTypeName> values) {
      Preconditions.checkNotNull(values);

      this.values = values;
    }

    public ImmutableList<SqlTypeName> getValues() {
      return values;
    }
  }
}
