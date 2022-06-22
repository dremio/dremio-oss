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
package com.dremio.service.autocomplete.statements.visitors;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.util.Preconditions;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.columns.ColumnResolver;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.functions.FunctionDictionary;
import com.dremio.service.autocomplete.functions.ParameterResolver;
import com.dremio.service.autocomplete.nessie.NessieElementResolver;
import com.dremio.service.autocomplete.nessie.NessieElementType;
import com.dremio.service.autocomplete.statements.grammar.CatalogPath;
import com.dremio.service.autocomplete.statements.grammar.FromClause;
import com.dremio.service.autocomplete.statements.grammar.JoinCondition;
import com.dremio.service.autocomplete.statements.grammar.QueryStatement;
import com.dremio.service.autocomplete.statements.grammar.SelectQueryStatement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public final class SelectQueryStatementCompletionProvider {
  private SelectQueryStatementCompletionProvider() {
  }

  public static Completions getCompletionsForIdentifier(
    SelectQueryStatement queryStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    Preconditions.checkNotNull(autocompleteEngineContext);
    FromClause fromClause = queryStatement.getFromClause();
    if (fromClause == null) {
      return getCompletionsForFunctions(
        queryStatement,
        autocompleteEngineContext);
    }

    boolean cursorInsideFromClause = Cursor.tokensHasCursor(fromClause.getTokens());
    if (!cursorInsideFromClause) {
      return getCompletionsForColumns(
        queryStatement,
        autocompleteEngineContext);
    }

    // We are inside a FROM clause but we need to know if we are inside a JOIN condition or not,
    // since that determines if we should return a a column or catalog entry
    ImmutableList<JoinCondition> joinConditions = queryStatement.getFromClause().getJoinConditions();
    for (JoinCondition joinCondition : joinConditions) {
      if (Cursor.tokensHasCursor(joinCondition.getTokens())) {
        return getCompletionsForColumns(
          queryStatement,
          autocompleteEngineContext);
      }
    }

    DremioToken lastTokenBeforeCursor = Iterables.getLast(Cursor.getTokensUntilCursor(fromClause.getTokens()));
    Optional<NessieElementType> optionalNessieElementType = NessieElementType.tryConvertFromDremioToken(lastTokenBeforeCursor);
    if (optionalNessieElementType.isPresent()) {
      return getCompletionsForNessieElements(
        queryStatement,
        autocompleteEngineContext);
    }

    return getCompletionsForCatalogEntries(
      queryStatement,
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForCatalogEntries(
    SelectQueryStatement queryStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    CatalogPath incompleteCatalogPath = queryStatement
      .getFromClause()
      .getCatalogPaths()
      .stream()
      .filter(catalogPath -> Cursor.tokensHasCursor(catalogPath.getTokens()))
      .findFirst()
      .get();
    return Completions
      .builder()
      .addCatalogNodes(autocompleteEngineContext
        .getAutocompleteSchemaProvider()
        .getChildrenInScope(incompleteCatalogPath.getPathTokens()))
      .build();
  }

  private static Completions getCompletionsForColumns(
    SelectQueryStatement queryStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    ImmutableList<DremioToken> contextTokens = Cursor.getTokensUntilCursor(queryStatement.getTokens());

    Set<ColumnAndTableAlias> columnAndTableAliases = ColumnResolver
      .create(autocompleteEngineContext)
      .resolve(autocompleteEngineContext.getCurrentStatementPath());

    // We need to determine if we are inside a function,
    // so we only recommend the columns and functions that are relevant.
    ParameterResolver.Result result = ParameterResolver
      .create(autocompleteEngineContext)
      .resolve(
        contextTokens,
        ImmutableSet.copyOf(columnAndTableAliases),
        queryStatement.getFromClause())
      .orElseGet(() -> new ParameterResolver.Result(
        new ParameterResolver.Result.Resolutions(
          columnAndTableAliases,
          new HashSet<>(FunctionDictionary.INSTANCE.getValues())),
        null));

    return Completions
      .builder()
      .addColumnAndTableAliases(ImmutableList.copyOf(result.getResolutions().getColumns()), getTableScope(contextTokens))
      .addFunctions(ImmutableList.copyOf(result.getResolutions().getFunctions()), contextTokens)
      .addFunctionContext(result.getFunctionContext())
      .build();
  }

  private static String getTableScope(ImmutableList<DremioToken> contextTokens) {
    if (contextTokens.size() < 2) {
      return null;
    }
    if (Iterables.getLast(contextTokens).getKind() == ParserImplConstants.DOT) {
      DremioToken scope = contextTokens.get(contextTokens.size() - 2);
      if (scope.getKind() == ParserImplConstants.IDENTIFIER) {
        return scope.getImage();
      }
    }
    return null;
  }

  private static Completions getCompletionsForFunctions(
    QueryStatement queryStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    ImmutableList<DremioToken> contextTokens = Cursor.getTokensUntilCursor(queryStatement.getTokens());
    return Completions
      .builder()
      .addFunctions(
        ImmutableList.copyOf(FunctionDictionary.INSTANCE.getValues()),
        contextTokens)
      .build();
  }

  private static Completions getCompletionsForNessieElements(
    QueryStatement queryStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Completions
      .builder()
      .addNessieElements(ImmutableList.copyOf(new NessieElementResolver(autocompleteEngineContext
        .getNessieElementReaderSupplier()
        .get())
        .resolve(Iterables.getLast(Cursor.getTokensUntilCursor(queryStatement.getTokens())))))
      .build();
  }
}
