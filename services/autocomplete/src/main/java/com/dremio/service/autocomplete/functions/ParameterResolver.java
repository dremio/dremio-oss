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
import java.util.Optional;
import java.util.Set;

import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.util.Pair;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.parsing.SqlNodeParser;
import com.dremio.service.autocomplete.parsing.ValidatingParser;
import com.dremio.service.autocomplete.statements.grammar.TableReference;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.functions.model.Function;
import com.dremio.service.functions.model.FunctionSignature;
import com.dremio.service.functions.model.Parameter;
import com.dremio.service.functions.model.ParameterType;
import com.dremio.service.functions.model.ParameterTypeHierarchy;
import com.dremio.service.functions.model.SqlTypeNameToParameterType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Resolves all the possible columns and functions that could be used as parameters to a function being built.
 */
public final class ParameterResolver {
  private final FunctionDictionary functionDictionary;
  private final ParameterTypeExtractor parameterTypeExtractor;

  public ParameterResolver(
    FunctionDictionary functionDictionary,
    ParameterTypeExtractor parameterTypeExtractor) {
    Preconditions.checkNotNull(functionDictionary);
    Preconditions.checkNotNull(parameterTypeExtractor);

    this.functionDictionary = functionDictionary;
    this.parameterTypeExtractor = parameterTypeExtractor;
  }

  public Optional<Result> resolve(
    ImmutableList<DremioToken> tokens,
    ImmutableSet<ColumnAndTableAlias> columns,
    ImmutableList<TableReference> tableReferences) {
    Preconditions.checkNotNull(tokens);
    Preconditions.checkNotNull(columns);
    Preconditions.checkNotNull(tableReferences);

    Optional<FunctionInformation> optionalFunctionInformation = getFunctionInformation(tokens);
    if (!optionalFunctionInformation.isPresent()) {
      return Optional.empty();
    }

    Function specification = optionalFunctionInformation.get().getSpecification();
    ParsedFunction parsedFunction = optionalFunctionInformation.get().getParseTree();

    ParameterTypeExtractor.Result suppliedParameterTypes = parameterTypeExtractor.extract(
      parsedFunction,
      columns,
      tableReferences);

    Result result = resolveResult(
      specification,
      suppliedParameterTypes,
      columns);

    return Optional.of(result);
  }

  public static ParameterResolver create(
    final SqlOperatorTable operatorTable,
    final SimpleCatalog<?> catalog,
    boolean generateSignatures) {
    final DremioCatalogReader catalogReader = new DremioCatalogReader(
      catalog,
      JavaTypeFactoryImpl.INSTANCE);
    final SqlOperatorTable chainedOperatorTable = ChainedSqlOperatorTable.of(
      operatorTable,
      catalogReader);
    SqlValidatorAndScopeFactory.Result validatorAndScope = SqlValidatorAndScopeFactory.create(
      operatorTable,
      catalog);
    SqlNodeParser parser = ValidatingParser.create(
      chainedOperatorTable,
      catalog);

    FunctionDictionary functionDictionary = FunctionDictionary.create(
      operatorTable,
      catalog,
      generateSignatures);

    ParameterTypeExtractor parameterTypeExtractor = new ParameterTypeExtractor(
      validatorAndScope.getSqlValidator(),
      validatorAndScope.getScope(),
      parser);

    return new ParameterResolver(
      functionDictionary,
      parameterTypeExtractor);
  }

  public static ParameterResolver create(
    AutocompleteEngineContext context) {
    return create(
      context.getOperatorTable(),
      context.getCatalog(),
      false);
  }

  private Optional<FunctionInformation> getFunctionInformation(ImmutableList<DremioToken> tokens) {
    // Find the function with a cursor inside of it
    int cursorIndex = Cursor.indexOfTokenWithCursor(tokens);
    int parensCounter = 1;
    int functionStartIndex;
    for (functionStartIndex = cursorIndex; (functionStartIndex >= 0) && (parensCounter > 0); functionStartIndex--) {
      // Scan backwards for the matching open parens
      DremioToken token = tokens.get(functionStartIndex);
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

    if (functionStartIndex < 0) {
      return Optional.empty();
    }

    DremioToken functionToken = tokens.get(functionStartIndex);
    Optional<Function> function = functionDictionary.tryGetValue(functionToken.getImage());
    if (!function.isPresent()) {
      return Optional.empty();
    }

    Optional<ParsedFunction> parsedFunction = ParsedFunction.tryParse(tokens.subList(functionStartIndex, tokens.size()));
    if (!parsedFunction.isPresent()) {
      return Optional.empty();
    }

    FunctionInformation functionInformation = new FunctionInformation(function.get(), parsedFunction.get());
    return Optional.of(functionInformation);
  }

  private Result resolveResult(
    Function function,
    ParameterTypeExtractor.Result suppliedParameterTypes,
    ImmutableSet<ColumnAndTableAlias> columnAndTableAliases) {
    FunctionContext functionContext = getFunctionContext(function, suppliedParameterTypes);
    ImmutableSet<ColumnAndTableAlias> resolvedColumns = resolveColumns(functionContext.getMissingTypes(), columnAndTableAliases);
    ImmutableSet<Function> resolvedFunctions = resolveFunctions(functionContext.getMissingTypes(), columnAndTableAliases);
    Result.Resolutions resolutions = new Result.Resolutions(resolvedColumns, resolvedFunctions);
    return new Result(resolutions, functionContext);
  }

  private static FunctionContext getFunctionContext(
    Function function,
    ParameterTypeExtractor.Result suppliedParameterTypes) {
    List<Pair<FunctionSignature, ParameterType>> matchingSignatureAndMissingType = new ArrayList<>();
    ImmutableList<FunctionSignature> functionSignatures = function.getSignatures() != null ? function.getSignatures() : ImmutableList.of();
    for (FunctionSignature functionSignature : functionSignatures) {
      ImmutableList<Parameter> parameters = functionSignature.getParameters();
      PrefixMatchResult prefixMatchResult = getPrefixMatchResult(
        ImmutableList.of(),
        suppliedParameterTypes.getBeforeCursor(),
        parameters);
      if (!prefixMatchResult.matched) {
        continue;
      }

      if (!prefixMatchResult.missingType.isPresent()) {
        continue;
      }

      if (!suppliedParameterTypes.getAfterCursor().isEmpty()) {
        ImmutableList<Parameter> parametersAfterPrefixMatch = parameters.subList(
          prefixMatchResult.parametersMatched.size() + 1,
          parameters.size());
        PrefixMatchResult suffixMatchResult = getPrefixMatchResult(
          ImmutableList.of(),
          suppliedParameterTypes.getAfterCursor(),
          parametersAfterPrefixMatch);

        if (!suffixMatchResult.matched) {
          continue;
        }

        if (suffixMatchResult.missingType.isPresent()) {
          continue;
        }
      }

      Pair<FunctionSignature, ParameterType> fullMatchResult = Pair.of(
        functionSignature,
        prefixMatchResult.missingType.get());

      matchingSignatureAndMissingType.add(fullMatchResult);
    }

    return new FunctionContext(
      function,
      suppliedParameterTypes,
      matchingSignatureAndMissingType.stream().map(pair -> pair.left).collect(ImmutableList.toImmutableList()),
      matchingSignatureAndMissingType.stream().map(pair -> pair.right).collect(ImmutableSet.toImmutableSet()));
  }

  private static PrefixMatchResult getPrefixMatchResult(
    ImmutableList<Parameter> parametersMatchedSoFar,
    ImmutableList<ParameterType> types,
    ImmutableList<Parameter> parameters) {
    // This is a standard regex match algorithm
    if (types.isEmpty() && parameters.isEmpty()) {
      return PrefixMatchResult.createMatch(parametersMatchedSoFar);
    }

    if (types.isEmpty()) {
      return PrefixMatchResult.createMatch(parametersMatchedSoFar,parameters.get(0).getType());
    }

    if (parameters.isEmpty()) {
      return PrefixMatchResult.createNoMatch();
    }

    ParameterType firstType = types.get(0);
    Parameter firstParameter = parameters.get(0);
    if (!doTypesMatch(firstParameter.getType(), firstType)) {
      return PrefixMatchResult.createNoMatch();
    }

    ImmutableList<ParameterType> subTypes = types.subList(1, types.size());

    ImmutableList<Parameter> subParameters;
    ImmutableList<Parameter> subParametersMatchedSoFar;
    switch (firstParameter.getKind()) {
    case REGULAR:
    case OPTIONAL:
      subParameters = parameters.subList(1, parameters.size());
      subParametersMatchedSoFar = ImmutableList.<Parameter>builder().addAll(parametersMatchedSoFar).add(firstParameter).build();
      break;

    case VARARG:
      subParameters = parameters;
      subParametersMatchedSoFar = parametersMatchedSoFar;
      break;

    default:
      throw new UnsupportedOperationException("UNKNOWN KIND: " + firstParameter.getKind());
    }

    return getPrefixMatchResult(subParametersMatchedSoFar, subTypes, subParameters);
  }

  private static ImmutableSet<ColumnAndTableAlias> resolveColumns(
    ImmutableSet<ParameterType> allowedTypes,
    ImmutableSet<ColumnAndTableAlias> columnAndTableAliases) {
    // Get the columns whose types are possible
    return columnAndTableAliases
      .stream()
      .filter(columnAndTableAlias -> {
        ParameterType columnType = SqlTypeNameToParameterType.convert(columnAndTableAlias.getColumn().getType());
        return allowedTypes.stream().anyMatch(allowedType -> doTypesMatch(allowedType, columnType));
      })
      .collect(ImmutableSet.toImmutableSet());
  }

  private ImmutableSet<Function> resolveFunctions(
    ImmutableSet<ParameterType> allowedTypes,
    ImmutableSet<ColumnAndTableAlias> columnAndTableAliases) {
    ImmutableSet<ParameterType> possibleColumnTypes = columnAndTableAliases
      .stream()
      .map(columnAndTableAlias -> SqlTypeNameToParameterType.convert(columnAndTableAlias.getColumn().getType()))
      .collect(ImmutableSet.toImmutableSet());
    return functionDictionary
      .getValues()
      .stream()
      .filter(subFunction -> getPossibleReturnTypes(subFunction, possibleColumnTypes)
        .stream()
        .anyMatch(returnType -> allowedTypes
          .stream()
          .anyMatch(allowedType -> doTypesMatch(allowedType, returnType))))
      .collect(ImmutableSet.toImmutableSet());
  }

  private static boolean doTypesMatch(ParameterType a, ParameterType b) {
    return (b == ParameterType.ANY) || ParameterTypeHierarchy.isDescendantOf(a, b);
  }

  private static ImmutableSet<ParameterType> getPossibleReturnTypes(
    Function function,
    ImmutableSet<ParameterType> possibleColumnTypes) {
    if (function.getSignatures() == null) {
      return ImmutableSet.of();
    }

    return function
      .getSignatures()
      .stream()
      .filter(functionSignature -> functionSignature
        .getParameters()
        .stream()
        .allMatch(parameter -> possibleColumnTypes
          .stream()
          .anyMatch(possibleColumnType -> doTypesMatch(parameter.getType(), possibleColumnType))))
      .map(functionSignature -> functionSignature.getReturnType())
      .collect(ImmutableSet.toImmutableSet());
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

  private static final class PrefixMatchResult {
    private final boolean matched;
    private final ImmutableList<Parameter> parametersMatched;
    private final Optional<ParameterType> missingType;

    private PrefixMatchResult(
      boolean matched,
      ImmutableList<Parameter> parametersMatched,
      Optional<ParameterType> missingType) {
      this.matched = matched;
      this.parametersMatched = parametersMatched;
      this.missingType = missingType;
    }

    public static PrefixMatchResult createMatch(ImmutableList<Parameter> parametersMatched, ParameterType parameterType) {
      return new PrefixMatchResult(true, parametersMatched, Optional.of(parameterType));
    }

    public static PrefixMatchResult createMatch(ImmutableList<Parameter> parametersMatched) {
      return new PrefixMatchResult(true, parametersMatched, Optional.empty());
    }

    public static PrefixMatchResult createNoMatch() {
      return new PrefixMatchResult(false, null, Optional.empty());
    }
  }

  private static final class FunctionInformation {
    private final Function specification;
    private final ParsedFunction parseTree;

    public FunctionInformation(Function specification, ParsedFunction parseTree) {
      this.specification = specification;
      this.parseTree = parseTree;
    }

    public Function getSpecification() {
      return specification;
    }

    public ParsedFunction getParseTree() {
      return parseTree;
    }
  }
}
