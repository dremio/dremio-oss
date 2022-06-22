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

import java.util.function.Predicate;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlOperatorTable;

import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.catalog.AutocompleteSchemaProvider;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.nessie.NessieElementReader;
import com.dremio.service.autocomplete.statements.grammar.Statement;
import com.dremio.service.autocomplete.statements.visitors.StatementCompletionProvider;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenResolver;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Completion engine for SQL queries.
 */
public final class AutocompleteEngine {
  private final AutocompleteSchemaProvider autocompleteSchemaProvider;
  private final Supplier<NessieElementReader> nessieElementReaderSupplier;
  private final SqlOperatorTable operatorTable;
  private final SimpleCatalog<?> catalog;

  public AutocompleteEngine(
    final AutocompleteSchemaProvider autocompleteSchemaProvider,
    final Supplier<NessieElementReader> nessieElementReaderSupplier,
    final SqlOperatorTable operatorTable,
    final SimpleCatalog<?> catalog) {
    Preconditions.checkNotNull(autocompleteSchemaProvider);
    Preconditions.checkNotNull(nessieElementReaderSupplier);
    Preconditions.checkNotNull(operatorTable);
    Preconditions.checkNotNull(catalog);
    this.autocompleteSchemaProvider = autocompleteSchemaProvider;
    this.nessieElementReaderSupplier = nessieElementReaderSupplier;
    this.operatorTable = operatorTable;
    this.catalog = catalog;
  }

  public Completions generateCompletions(
    String corpus,
    int cursorPosition) {
    Preconditions.checkNotNull(corpus);
    Preconditions.checkArgument(cursorPosition >= 0);

    SqlStateMachine.State state = SqlStateMachine.getState(corpus, cursorPosition);
    if (state != SqlStateMachine.State.IN_CODE) {
      return Completions.EMPTY;
    }

    ImmutableList<DremioToken> tokens = Cursor.tokenizeWithCursor(corpus, cursorPosition);
    ImmutableList<DremioToken> tokensForPrediction = getTokensForPrediction(tokens);

    Completions.Builder completionsBuilder = Completions.builder();
    TokenResolver.Predictions predictions = TokenResolver.getNextPossibleTokens(tokensForPrediction);
    if (predictions.isIdentifierPossible()) {
      ImmutableList<Statement> currentStatementPath = Statement.extractCurrentStatementPath(corpus, cursorPosition);
      Statement currentStatement = Iterables.getLast(currentStatementPath);
      AutocompleteEngineContext autocompleteEngineContext = new AutocompleteEngineContext(
        autocompleteSchemaProvider,
        nessieElementReaderSupplier,
        operatorTable,
        catalog,
        currentStatementPath);
      Completions completions = currentStatement.accept(
        StatementCompletionProvider.INSTANCE,
        autocompleteEngineContext);
      completionsBuilder.merge(completions);
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

  private static ImmutableList<DremioToken> getTokensForPrediction(ImmutableList<DremioToken> tokens) {
    ImmutableList<DremioToken> tokensForPrediction = Cursor.getTokensUntilCursor(tokens);
    int semiColonIndex = lastIndexOf(
      tokensForPrediction,
      token -> token.getKind() == ParserImplConstants.SEMICOLON);
    return tokensForPrediction.subList(semiColonIndex + 1, tokensForPrediction.size());
  }

  private static <T> int lastIndexOf (
    ImmutableList<T> tokens,
    Predicate<T> predicate) {
    for (int i = tokens.size() - 1; i >= 0; i--) {
      T currentToken = tokens.get(i);
      if (predicate.test(currentToken)) {
        return i;
      }
    }

    return -1;
  }
}
