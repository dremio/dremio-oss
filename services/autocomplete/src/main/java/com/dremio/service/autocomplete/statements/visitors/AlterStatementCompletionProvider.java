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
import com.dremio.service.autocomplete.statements.grammar.AlterStatement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.google.common.base.Preconditions;

public final class AlterStatementCompletionProvider {
  private AlterStatementCompletionProvider() {
  }

  public static Completions getCompletionsForIdentifier(
    AlterStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    Preconditions.checkNotNull(statement);
    Preconditions.checkNotNull(autocompleteEngineContext);
    if ((statement.getCatalogPath() != null) && Cursor.tokensHasCursor(statement.getCatalogPath().getTokens())){
      return getCompletionsForCatalogPath(statement, autocompleteEngineContext);
    }

    throw new UnsupportedOperationException("Cursor was not in any of the expected places for a UPDATE statement.");
  }

  private static Completions getCompletionsForCatalogPath(
    AlterStatement statement,
    AutocompleteEngineContext context) {
    // In the future we need to filter by the alter catalog type.
    return Utils.getCatalogEntries(statement.getCatalogPath(), context);
  }
}
