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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.DremioToken;
import com.dremio.service.autocomplete.SqlQueryUntokenizer;
import com.dremio.service.autocomplete.columns.Column;
import com.dremio.service.autocomplete.columns.DremioQueryParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Resolves all the possible columns and functions that could be used as parameters to a function being built.
 */
public final class ParameterResolver {
  private static final class DremioTokens {
    public static final DremioToken COMMA = new DremioToken(ParserImplConstants.COMMA, ",");
    public static final DremioToken SELECT = new DremioToken(ParserImplConstants.SELECT, "SELECT");

    private DremioTokens() {
    }
  }

  private final SqlFunctionDictionary sqlFunctionDictionary;
  private final SqlValidator sqlValidator;
  private final SqlValidatorScope sqlValidatorScope;
  private final DremioQueryParser queryParser;

  public ParameterResolver(
    SqlFunctionDictionary sqlFunctionDictionary,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope,
    DremioQueryParser queryParser) {
    Preconditions.checkNotNull(sqlFunctionDictionary);
    Preconditions.checkNotNull(sqlValidator);
    Preconditions.checkNotNull(sqlValidatorScope);
    Preconditions.checkNotNull(queryParser);

    this.sqlValidator = sqlValidator;
    this.sqlValidatorScope = sqlValidatorScope;
    this.sqlFunctionDictionary = sqlFunctionDictionary;
    this.queryParser = queryParser;
  }

  public SqlFunctionDictionary getSqlFunctionDictionary() {
    return sqlFunctionDictionary;
  }

  public Optional<Resolutions> resolve(
    ImmutableList<DremioToken> tokens,
    ImmutableSet<Column> columns,
    ImmutableList<DremioToken> fromClause) {
    Optional<Pair<Resolutions, DebugInfo>> resolutionsDebugInfoPair = resolveWithDebugInfo(
      tokens,
      columns,
      fromClause);
    if (!resolutionsDebugInfoPair.isPresent()) {
      return Optional.empty();
    }

    return Optional.of(resolutionsDebugInfoPair.get().left);
  }

  public Optional<Pair<Resolutions, DebugInfo>> resolveWithDebugInfo(
    ImmutableList<DremioToken> tokens,
    ImmutableSet<Column> columns,
    ImmutableList<DremioToken> fromClause) {
    Preconditions.checkNotNull(tokens);
    Preconditions.checkNotNull(columns);
    Preconditions.checkNotNull(fromClause);

    Optional<Pair<SqlFunction, Integer>> functionSpecAndIndex = getSqlFunctionAndIndex(tokens);
    if (!functionSpecAndIndex.isPresent()) {
      return Optional.empty();
    }

    SqlFunction sqlFunction = functionSpecAndIndex.get().left;
    int index = functionSpecAndIndex.get().right;

    ImmutableList<DremioToken> functionTokens = tokens.subList(index, tokens.size());

    List<SqlTypeName> parameterTypesUsedSoFar = getParameterTypesUsedSoFar(
      functionTokens,
      columns,
      fromClause);

    DebugInfo debugInfo = new DebugInfo(sqlFunction.getName(), parameterTypesUsedSoFar);

    Resolutions resolutions = resolveImplementation(
      sqlFunction,
      ImmutableList.copyOf(parameterTypesUsedSoFar),
      columns);

    return Optional.of(new Pair<>(resolutions, debugInfo));
  }

  private Optional<Pair<SqlFunction, Integer>> getSqlFunctionAndIndex(ImmutableList<DremioToken> tokens) {
    int parensCounter = 1;
    for (int i = tokens.size() - 1; i >= 0; i--) {
      DremioToken token = tokens.get(i);
      switch (token.getKind()) {
        case ParserImplConstants.LPAREN:
          parensCounter--;
          break;
        case ParserImplConstants.RPAREN:
          parensCounter++;
          break;
        default:
          // Do Nothing
      }

      if (parensCounter != 0) {
        continue;
      }

      Optional<SqlFunction> optionalSqlFunction = sqlFunctionDictionary.tryGetValue(token.getImage());
      if (optionalSqlFunction.isPresent()) {
        return Optional.of(new Pair<>(optionalSqlFunction.get(), i));
      }
    }

    return Optional.empty();
  }

  private List<SqlTypeName> getParameterTypesUsedSoFar(
    ImmutableList<DremioToken> tokens,
    Set<Column> columns,
    ImmutableList<DremioToken> fromClause) {
    if (tokens.size() == 2) {
      return Collections.EMPTY_LIST;
    }

    // We are going to get something of the form:
    // FUNCTION(A, ^, C)
    // We need to parse out the tokens for A and C and get their types
    // Start at 2, since we want to skip the function name and '('
    if (tokens.get(tokens.size() - 1).getKind() != ParserImplConstants.COMMA) {
      tokens = new ImmutableList.Builder<DremioToken>().addAll(tokens).add(DremioTokens.COMMA).build();
    }

    List<SqlTypeName> types = new ArrayList<>();
    int tokenIndex = 2;
    int lastCommaIndex = tokenIndex;
    int parensCount = 0;
    while (tokenIndex != tokens.size()) {
      DremioToken token = tokens.get(tokenIndex);
      switch (token.getKind()) {
        case ParserImplConstants.LPAREN:
          parensCount++;
          break;

        case ParserImplConstants.RPAREN:
          parensCount--;
          break;

        case ParserImplConstants.COMMA:
          if (parensCount == 0) {
            ImmutableList<DremioToken> parameterTokens = tokens.subList(lastCommaIndex, tokenIndex);
            lastCommaIndex = tokenIndex + 1;

            SqlNode parameterNode = getSqlNode(parameterTokens, fromClause);

            SqlTypeName type = deriveSqlNodeType(parameterNode, columns);
            types.add(type);
          }
          break;

        default:
          // DO NOTHING
          break;
      }

      tokenIndex++;
    }

    return types;
  }

  private SqlNode getSqlNode(ImmutableList<DremioToken> tokens, ImmutableList<DremioToken> fromClause) {
    ImmutableList<DremioToken> modifiedQueryTokens = new ImmutableList.Builder<DremioToken>()
      .add(DremioTokens.SELECT)
      .addAll(tokens)
      .addAll(fromClause)
      .build();

    String text = SqlQueryUntokenizer.untokenize(modifiedQueryTokens);
    SqlNode sqlNode = queryParser.parse(text);
    return ((SqlSelect) sqlNode).getSelectList().get(0);
  }

  private SqlTypeName deriveSqlNodeType(SqlNode sqlNode, Set<Column> columns) {
    SqlTypeName type;
    if (sqlNode instanceof SqlLiteral) {
      SqlLiteral sqlLiteral = (SqlLiteral) sqlNode;
      type = sqlLiteral.getTypeName();
    } else if (sqlNode instanceof SqlCall) {
      SqlCall sqlCall = (SqlCall) sqlNode;
      type = sqlCall
        .getOperator()
        .deriveType(
          sqlValidator,
          sqlValidatorScope,
          sqlCall)
        .getSqlTypeName();
    } else if (sqlNode instanceof SqlIdentifier) {
      SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
      String name = sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
      Optional<Column> optionalColumn = columns
        .stream()
        .filter(column -> column.getName().equalsIgnoreCase(name))
        .findAny();

      type = optionalColumn.get().getType().getSqlTypeName();
    } else {
      throw new RuntimeException("Cannot derive SqlNode with kind: " + sqlNode.getKind());
    }

    return type;
  }

  private Resolutions resolveImplementation(
    SqlFunction sqlFunction,
    ImmutableList<SqlTypeName> typePrefix,
    ImmutableSet<Column> columns) {
    Set<Column> resolvedColumns = columns
      .stream()
      .filter(column -> hasMatchingSignatures(
        sqlFunction,
        typePrefix,
        column.getType().getSqlTypeName()))
      .collect(Collectors.toSet());

    Set<SqlFunction> resolvedFunctions = sqlFunctionDictionary
      .getValues()
      .stream()
      .filter(subFunction -> getPossibleReturnTypes(
        subFunction,
        ImmutableSet.<SqlTypeName>builder()
          .addAll(columns
            .stream()
            .map(column -> column.getType().getSqlTypeName())
            .collect(Collectors.toSet()))
          .build())
        .stream()
        .filter(possibleReturnType -> hasMatchingSignatures(
          sqlFunction,
          typePrefix,
          possibleReturnType))
        .findAny()
        .isPresent())
      .collect(Collectors.toSet());

    return new Resolutions(resolvedColumns, resolvedFunctions);
  }

  private boolean hasMatchingSignatures(
    SqlFunction sqlFunction,
    ImmutableList<SqlTypeName> typesPrefix,
    SqlTypeName candidateType) {
    Function function = FunctionFactory.createFromSqlFunction(
      sqlFunction,
      this.sqlValidator,
      this.sqlValidatorScope);
    for (FunctionSignature functionSignature : function.getSignatures()) {
      // This could be better with a trie
      ImmutableList<SqlTypeName> operandTypes = functionSignature.getOperandTypes();
      boolean functionSignatureIsLonger = operandTypes.size() > typesPrefix.size();
      if (functionSignatureIsLonger) {
        boolean prefixesMatch = operandTypes
          .subList(0, typesPrefix.size())
          .equals(typesPrefix);
        if (prefixesMatch) {
          boolean nextOperandMatchesColumn = operandTypes.get(typesPrefix.size()) == candidateType;
          if (nextOperandMatchesColumn) {
            return true;
          }
        }
      }
    }

    return false;
  }

  private Set<SqlTypeName> getPossibleReturnTypes(
    SqlFunction sqlFunction,
    ImmutableSet<SqlTypeName> possibleColumnTypes) {
    Set<SqlTypeName> possibleReturnTypes = new HashSet<>();
    Function function = FunctionFactory.createFromSqlFunction(
      sqlFunction,
      this.sqlValidator,
      this.sqlValidatorScope);
    for (FunctionSignature functionSignature : function.getSignatures()) {
      ImmutableList<SqlTypeName> operandTypes = functionSignature.getOperandTypes();
      boolean allOperandsArePossible = operandTypes.stream().allMatch(operandType -> possibleColumnTypes.contains(operandType));
      if (allOperandsArePossible) {
        possibleReturnTypes.add(functionSignature.getReturnType());
      }
    }

    return possibleReturnTypes;
  }

  /**
   * The resolutions from the ParameterResolver.
   */
  public static final class Resolutions {
    private final Set<Column> columns;
    private final Set<SqlFunction> functions;

    public Resolutions(
      Set<Column> columns,
      Set<SqlFunction> functions) {
      Preconditions.checkNotNull(columns);
      Preconditions.checkNotNull(functions);

      this.columns = columns;
      this.functions = functions;
    }

    public Set<Column> getColumns() {
      return columns;
    }

    public Set<SqlFunction> getFunctions() {
      return functions;
    }
  }

  /**
   * Debug information for how we resolved the parameters.
   */
  public static final class DebugInfo {
    private final String functionName;
    private final List<SqlTypeName> typesUsedSoFar;

    private DebugInfo(
      String functionName,
      List<SqlTypeName> typesUsedSoFar) {
      this.functionName = functionName;
      this.typesUsedSoFar = typesUsedSoFar;
    }

    public String getFunctionName() {
      return functionName;
    }

    public List<SqlTypeName> getTypesUsedSoFar() {
      return typesUsedSoFar;
    }
  }
}
