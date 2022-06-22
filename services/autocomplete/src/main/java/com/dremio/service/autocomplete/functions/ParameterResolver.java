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
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.parsing.SqlNodeParser;
import com.dremio.service.autocomplete.parsing.ValidatingParser;
import com.dremio.service.autocomplete.statements.grammar.FromClause;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Resolves all the possible columns and functions that could be used as parameters to a function being built.
 */
public final class ParameterResolver {
  private static final DremioToken COMMA_TOKEN = DremioToken.createFromParserKind(ParserImplConstants.COMMA);

  private final FunctionDictionary functionDictionary;
  private final SqlValidator sqlValidator;
  private final SqlValidatorScope sqlValidatorScope;
  private final SqlNodeParser queryParser;

  public ParameterResolver(
    FunctionDictionary functionDictionary,
    SqlValidator sqlValidator,
    SqlValidatorScope sqlValidatorScope,
    SqlNodeParser queryParser) {
    Preconditions.checkNotNull(functionDictionary);
    Preconditions.checkNotNull(sqlValidator);
    Preconditions.checkNotNull(sqlValidatorScope);
    Preconditions.checkNotNull(queryParser);

    this.functionDictionary = functionDictionary;
    this.sqlValidator = sqlValidator;
    this.sqlValidatorScope = sqlValidatorScope;
    this.queryParser = queryParser;
  }

  public Optional<Result> resolve(
    ImmutableList<DremioToken> tokens,
    ImmutableSet<ColumnAndTableAlias> columns,
    FromClause fromClause) {
    Preconditions.checkNotNull(tokens);
    Preconditions.checkNotNull(columns);
    Preconditions.checkNotNull(fromClause);

    Optional<Pair<Function, Integer>> functionSpecAndIndex = getFunctionAndIndex(tokens);
    if (!functionSpecAndIndex.isPresent()) {
      return Optional.empty();
    }

    Function function = functionSpecAndIndex.get().left;
    int index = functionSpecAndIndex.get().right;

    ImmutableList<DremioToken> functionTokens = tokens.subList(index, tokens.size());

    List<SqlTypeName> parameterTypesUsedSoFar = getParameterTypesUsedSoFar(
      functionTokens,
      columns,
      fromClause);

    Result result = resolveImplementation(
      function,
      ImmutableList.copyOf(parameterTypesUsedSoFar),
      columns);

    return Optional.of(result);
  }

  public static ParameterResolver create(
    final SqlOperatorTable operatorTable,
    final SimpleCatalog<?> catalog) {
    final DremioCatalogReader catalogReader = new DremioCatalogReader(
      catalog,
      JavaTypeFactoryImpl.INSTANCE);
    final SqlOperatorTable chainedOperatorTable = ChainedSqlOperatorTable.of(
      operatorTable,
      catalogReader);
    SqlValidatorAndScopeFactory.Result validatorAndScope = SqlValidatorAndScopeFactory.create(
      operatorTable,
      catalog);
    List<SqlFunction> sqlFunctions = chainedOperatorTable
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
      .map(function -> (SqlFunction) function).collect(Collectors.toList());
    FunctionDictionary functionDictionary = FunctionDictionary.create(
      sqlFunctions,
      validatorAndScope.getSqlValidator(),
      validatorAndScope.getScope(),
      false);
    SqlNodeParser parser = ValidatingParser.create(
      chainedOperatorTable,
      catalog);
    return new ParameterResolver(
      functionDictionary,
      validatorAndScope.getSqlValidator(),
      validatorAndScope.getScope(),
      parser);
  }

  public static ParameterResolver create(
    AutocompleteEngineContext context) {
    return create(
      context.getOperatorTable(),
      context.getCatalog());
  }

  private Optional<Pair<Function, Integer>> getFunctionAndIndex(ImmutableList<DremioToken> tokens) {
    // Look for a function with an open parens without a matching close parens
    int i = tokens.size() - 1;
    int parensCounter = 1;
    for (; (i > 0) && (parensCounter > 0); i--) {
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
    }

    if (parensCounter > 0) {
      return Optional.empty();
    }

    DremioToken functionToken = tokens.get(i);
    final int finalI = i;
    return functionDictionary
      .tryGetValue(functionToken.getImage())
      .map(function -> new Pair<>(function, finalI));
  }

  private List<SqlTypeName> getParameterTypesUsedSoFar(
    ImmutableList<DremioToken> tokens,
    Set<ColumnAndTableAlias> columns,
    FromClause fromClause) {
    if (tokens.size() == 2) {
      return Collections.emptyList();
    }

    // We are going to get something of the form:
    // FUNCTION(A, ^, C)
    // We need to parse out the tokens for A and C and get their types
    // Start at 2, since we want to skip the function name and '('
    if (tokens.get(tokens.size() - 1).getKind() != ParserImplConstants.COMMA) {
      tokens = new ImmutableList.Builder<DremioToken>().addAll(tokens).add(COMMA_TOKEN).build();
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

  private SqlNode getSqlNode(ImmutableList<DremioToken> tokens, FromClause fromClause) {
    String queryText = QueryBuilder.build(tokens, fromClause);
    SqlNode sqlNode = queryParser.toSqlNode(queryText);
    return ((SqlSelect) sqlNode).getSelectList().get(0);
  }

  private SqlTypeName deriveSqlNodeType(SqlNode sqlNode, Set<ColumnAndTableAlias> columns) {
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
      Optional<ColumnAndTableAlias> optionalColumn = columns
        .stream()
        .filter(column -> column.getColumn().getName().equalsIgnoreCase(name))
        .findAny();
      if (!optionalColumn.isPresent()) {
        throw new RuntimeException("FAILED TO FIND COLUMN WITH NAME: " + name);
      }

      type = optionalColumn.get().getColumn().getType();
    } else {
      throw new RuntimeException("Cannot derive SqlNode with kind: " + sqlNode.getKind());
    }

    return type;
  }

  private Result resolveImplementation(
    Function function,
    ImmutableList<SqlTypeName> typePrefix,
    ImmutableSet<ColumnAndTableAlias> columns) {
    Set<ColumnAndTableAlias> resolvedColumns = columns
      .stream()
      .filter(column -> {
        if (function.getSignatures().isEmpty()) {
          return true;
        }

        return hasMatchingSignatures(
          function,
          typePrefix,
          column.getColumn().getType());
      })
      .collect(Collectors.toSet());

    Set<Function> resolvedFunctions = functionDictionary
      .getValues()
      .stream()
      .filter(subFunction -> {
        if (function.getSignatures().isEmpty()) {
          return true;
        }

        return getPossibleReturnTypes(
          subFunction,
          ImmutableSet.<SqlTypeName>builder()
            .addAll(columns
              .stream()
              .map(column -> column.getColumn().getType())
              .collect(Collectors.toSet()))
            .build())
          .stream()
          .anyMatch(possibleReturnType -> hasMatchingSignatures(
            function,
            typePrefix,
            possibleReturnType));
      })
      .collect(Collectors.toSet());

    Result.Resolutions resolutions = new Result.Resolutions(resolvedColumns, resolvedFunctions);

    ImmutableList<FunctionSignature> signaturesMatched = getSignaturesMatched(function, typePrefix);
    FunctionContext functionContext = new FunctionContext(
      function,
      typePrefix,
      signaturesMatched);

    return new Result(resolutions, functionContext);
  }

  private static ImmutableList<FunctionSignature> getSignaturesMatched(
    Function function,
    ImmutableList<SqlTypeName> typesPrefix) {
    ImmutableList.Builder<FunctionSignature> functionSignatureBuilder = new ImmutableList.Builder<>();
    for (FunctionSignature functionSignature : function.getSignatures()) {
      // This could be better with a trie
      ImmutableList<SqlTypeName> operandTypes = functionSignature.getOperandTypes();
      boolean functionSignatureIsLonger = operandTypes.size() > typesPrefix.size();
      if (functionSignatureIsLonger) {
        boolean prefixesMatch = operandTypes
          .subList(0, typesPrefix.size())
          .equals(typesPrefix);
        if (prefixesMatch) {
          functionSignatureBuilder.add(functionSignature);
        }
      }
    }

    return functionSignatureBuilder.build();
  }

  private static boolean hasMatchingSignatures(
    Function function,
    ImmutableList<SqlTypeName> typesPrefix,
    SqlTypeName candidateType) {
    for (FunctionSignature functionSignature : function.getSignatures()) {
      // This could be better with a trie
      ImmutableList<SqlTypeName> operandTypes = functionSignature.getOperandTypes();
      boolean functionSignatureIsLonger = operandTypes.size() > typesPrefix.size();
      if (functionSignatureIsLonger) {
        boolean prefixesMatch = operandTypes
          .subList(0, typesPrefix.size())
          .equals(typesPrefix);
        if (prefixesMatch) {
          boolean nextOperandMatchesColumn = candidateType == SqlTypeName.ANY || operandTypes.get(typesPrefix.size()) == candidateType;
          if (nextOperandMatchesColumn) {
            return true;
          }
        }
      }
    }

    return false;
  }

  private static Set<SqlTypeName> getPossibleReturnTypes(
    Function function,
    ImmutableSet<SqlTypeName> possibleColumnTypes) {
    Set<SqlTypeName> possibleReturnTypes = new HashSet<>();
    for (FunctionSignature functionSignature : function.getSignatures()) {
      ImmutableList<SqlTypeName> operandTypes = functionSignature.getOperandTypes();
      boolean allOperandsArePossible = possibleColumnTypes.containsAll(operandTypes);
      if (allOperandsArePossible) {
        possibleReturnTypes.add(functionSignature.getReturnType());
      }
    }

    return possibleReturnTypes;
  }

  /**
   * Result of parameter resolution.
   */
  public static final class Result {
    private final Resolutions resolutions;
    private final FunctionContext functionContext;

    public Result(
      Resolutions resolutions,
      FunctionContext functionContext) {
      Preconditions.checkNotNull(resolutions);
      this.resolutions = resolutions;
      this.functionContext = functionContext;
    }

    public Resolutions getResolutions() {
      return resolutions;
    }

    public FunctionContext getFunctionContext() {
      return functionContext;
    }

    /**
     * The resolutions from the ParameterResolver.
     */
    public static final class Resolutions {
      private final Set<ColumnAndTableAlias> columns;
      private final Set<Function> functions;

      public Resolutions(
        Set<ColumnAndTableAlias> columns,
        Set<Function> functions) {
        Preconditions.checkNotNull(columns);
        Preconditions.checkNotNull(functions);

        this.columns = columns;
        this.functions = functions;
      }

      public Set<ColumnAndTableAlias> getColumns() {
        return columns;
      }

      public Set<Function> getFunctions() {
        return functions;
      }
    }
  }
}
