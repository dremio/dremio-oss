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
package com.dremio.service.autocomplete.statements.grammar;

import java.util.HashSet;
import java.util.Set;

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.columns.ColumnResolver;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.functions.FunctionDictionary;
import com.dremio.service.autocomplete.functions.ParameterResolver;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public final class Expression extends LeafStatement {
  private final ImmutableList<TableReference> tableReferences;

  private Expression(
    ImmutableList<DremioToken> tokens,
    ImmutableList<TableReference> tableReferences) {
    super(tokens);
    Preconditions.checkNotNull(tableReferences);
    this.tableReferences = tableReferences;
  }

  public ImmutableList<TableReference> getTableReferences() {
    return tableReferences;
  }

  @Override
  public Completions getCompletions(AutocompleteEngineContext autocompleteEngineContext) {
    ImmutableList<DremioToken> contextTokens = Cursor.getTokensUntilCursor(getTokens());

    Set<ColumnAndTableAlias> columnAndTableAliases = ColumnResolver
      .create(autocompleteEngineContext)
      .resolve(tableReferences);

    // We need to determine if we are inside a function,
    // so we only recommend the columns and functions that are relevant.
    ParameterResolver.Result result = ParameterResolver
      .create(autocompleteEngineContext)
      .resolve(
        getTokens(),
        ImmutableSet.copyOf(columnAndTableAliases),
        tableReferences)
      .orElseGet(() -> new ParameterResolver.Result(
        new ParameterResolver.Result.Resolutions(
          columnAndTableAliases,
          new HashSet<>(FunctionDictionary
            .create(
              autocompleteEngineContext.getOperatorTable(),
              autocompleteEngineContext.getCatalog())
            .getValues())),
        null));

    return Completions
      .builder()
      .addColumnAndTableAliases(ImmutableList.copyOf(result.getResolutions().getColumns()))
      .addFunctions(
        ImmutableList.copyOf(result.getResolutions().getFunctions()),
        contextTokens)
      .addFunctionContext(result.getFunctionContext())
      .build();
  }

  static Expression parse(
    ImmutableList<DremioToken> tokens,
    FromClause fromClause) {
    return new Expression(tokens, fromClause.getTableReferences());
  }

  static Expression parse(
    ImmutableList<DremioToken> tokens,
    ImmutableList<TableReference> tableReferences) {
    return new Expression(tokens, tableReferences);
  }
}
