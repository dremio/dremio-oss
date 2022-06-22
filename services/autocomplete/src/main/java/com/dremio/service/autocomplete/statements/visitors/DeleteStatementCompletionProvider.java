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

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.statements.grammar.DeleteStatement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.google.common.base.Preconditions;

public final class DeleteStatementCompletionProvider {
  private DeleteStatementCompletionProvider() {
  }

  public static Completions getCompletionsForIdentifier(
    DeleteStatement deleteStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    Preconditions.checkNotNull(deleteStatement);
    Preconditions.checkNotNull(autocompleteEngineContext);

    if ((deleteStatement.getCatalogPath() != null) && Cursor.tokensHasCursor(deleteStatement.getCatalogPath().getTokens())) {
      return getCompletionsForCatalogPath(
        deleteStatement,
        autocompleteEngineContext);
    }

    if ((deleteStatement.getCondition() != null && Cursor.tokensHasCursor(deleteStatement.getCondition()))) {
      return getCompletionsForCondition(
        deleteStatement,
        autocompleteEngineContext);
    }

    throw new UnsupportedOperationException("Cursor was not in any of the expected places for a DELETE statement.");
  }

  private static Completions getCompletionsForCatalogPath(
    DeleteStatement deleteStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getCatalogEntries(
      deleteStatement.getCatalogPath(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForCondition(
    DeleteStatement deleteStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getColumns(
      deleteStatement.getCatalogPath(),
      autocompleteEngineContext);
  }
}
