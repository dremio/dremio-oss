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
package com.dremio.service.autocomplete;

import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.statements.grammar.Statement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenResolver;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Completion engine for SQL queries.
 */
public final class AutocompleteEngine {
  private AutocompleteEngine() {
  }

  public static Completions generateCompletions(
    AutocompleteEngineContext context,
    String corpus,
    int cursorPosition) {
    Preconditions.checkNotNull(corpus);
    Preconditions.checkArgument(cursorPosition >= 0);
    SqlStateMachine.State state = SqlStateMachine.getState(corpus, cursorPosition);
    if (state != SqlStateMachine.State.IN_CODE && state != SqlStateMachine.State.IN_DOUBLE_QUOTE_STRING_LITERAL) {
      return Completions.EMPTY;
    }

    ImmutableList<DremioToken> tokens = Cursor.tokenizeWithCursor(corpus, cursorPosition);
    if (tokens == null) {
      // User gave an invalid query that didn't even tokenize correctly.
      return Completions.EMPTY;
    }

    Completions.Builder completionsBuilder = Completions.builder();
    TokenResolver.Predictions predictions = TokenResolver.getNextPossibleTokens(tokens);
    if (predictions.isIdentifierPossible()) {
      completionsBuilder.merge(Cursor
        .extractElementWithCursor(Statement.parse(corpus, cursorPosition))
        .getCompletions(context));
    }

    completionsBuilder.addKeywords(predictions.getKeywords());

    DremioToken cursorToken = tokens
      .stream()
      .filter(Cursor::tokenEndsWithCursor)
      .findFirst()
      .get();
    String prefix = cursorToken
      .getImage()
      .substring(0, cursorToken.getImage().indexOf(Cursor.CURSOR_CHARACTER));
    if (!prefix.isEmpty()) {
      completionsBuilder.addPrefixFilter(prefix);
    }

    return completionsBuilder.build();
  }
}
