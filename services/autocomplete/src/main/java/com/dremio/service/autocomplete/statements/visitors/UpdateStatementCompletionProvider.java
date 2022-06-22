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
import com.dremio.service.autocomplete.statements.grammar.UpdateStatement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.google.common.base.Preconditions;

public final class UpdateStatementCompletionProvider {
  private UpdateStatementCompletionProvider() {
  }

  public static Completions getCompletionsForIdentifier(
    UpdateStatement updateStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    Preconditions.checkNotNull(updateStatement);
    Preconditions.checkNotNull(autocompleteEngineContext);
    if ((updateStatement.getTablePrimary() != null) && Cursor.tokensHasCursor(updateStatement.getTablePrimary().getTokens())){
      return getCompletionsForTablePrimary(updateStatement, autocompleteEngineContext);
    }

    if ((updateStatement.getAssignTokens() != null) && Cursor.tokensHasCursor(updateStatement.getAssignTokens())) {
      return getCompletionsForAssign(updateStatement, autocompleteEngineContext);
    }

    if ((updateStatement.getCondition() != null) && Cursor.tokensHasCursor(updateStatement.getCondition())) {
      return getCompletionsForCondition(updateStatement, autocompleteEngineContext);
    }

    throw new UnsupportedOperationException("Cursor was not in any of the expected places for a UPDATE statement.");
  }

  private static Completions getCompletionsForTablePrimary(
    UpdateStatement updateStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getCatalogEntries(
      updateStatement.getTablePrimary(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForAssign(
    UpdateStatement updateStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getColumns(
      updateStatement.getTablePrimary(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForCondition(
    UpdateStatement updateStatement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getColumns(
      updateStatement.getTablePrimary(),
      autocompleteEngineContext);
  }
}
