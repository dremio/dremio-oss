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

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.columns.ColumnResolver;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public final class Column extends LeafStatement {
  private final TableReference tableReference;

  private Column(ImmutableList<DremioToken> tokens, TableReference tableReference) {
    super(tokens);
    Preconditions.checkNotNull(tableReference);
    this.tableReference = tableReference;
  }

  public TableReference getTableReference() {
    return tableReference;
  }

  static Column parse(
    ImmutableList<DremioToken> tokens,
    TableReference tableReferences) {
    return new Column(tokens, tableReferences);
  }

  @Override
  public Completions getCompletions(AutocompleteEngineContext autocompleteEngineContext) {
    ImmutableList<com.dremio.service.autocomplete.columns.Column> columns = ColumnResolver
      .create(autocompleteEngineContext)
      .resolve(ImmutableList.of(tableReference))
      .stream()
      .map(ColumnAndTableAlias::getColumn)
      .collect(ImmutableList.toImmutableList());
    return Completions
      .builder()
      .addColumns(columns, tableReference)
      .build();
  }
}
