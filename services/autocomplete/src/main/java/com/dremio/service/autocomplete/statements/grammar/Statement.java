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

import java.util.Optional;

import com.dremio.service.autocomplete.statements.visitors.StatementInputOutputVisitor;
import com.dremio.service.autocomplete.statements.visitors.StatementVisitor;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.SqlQueryTokenizer;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;

/**
 * Base class for all statements
 */
public abstract class Statement {

  private final ImmutableList<DremioToken> tokens;
  private final ImmutableList<? extends Statement> children;

  protected Statement(ImmutableList<DremioToken> tokens, ImmutableList<? extends Statement> children) {
    this.tokens = tokens;
    this.children = children;
  }

  public ImmutableList<DremioToken> getTokens() {
    return tokens;
  }

  public ImmutableList<? extends Statement> getChildren() {
    return children;
  }

  public abstract void accept(StatementVisitor visitor);

  public abstract <I, O> O accept(StatementInputOutputVisitor<I, O> visitor, I input);

  /**
   * This is somewhat similar to what a parser does, but much more limited in scope.
   * We also don't check correctness of the query in any way
   * We rely on specific keywords to infer scopes.
   * The result is a list of statements in a given query. Statement is something that
   */
  public static ImmutableList<Statement> extractCurrentStatementPath(String corpus, int cursorPosition) {
    // Add the bell character which is never used in valid sql to represent the cursor.
    String corpusWithCursor = new StringBuilder(corpus)
      .insert(cursorPosition, Cursor.CURSOR_CHARACTER)
      .toString();

    ImmutableList<DremioToken> tokens = SqlQueryTokenizer.tokenize(corpusWithCursor);

    Statement statement = StatementList.parse(new TokenBuffer(tokens));
    return extractCurrentStatementPath(statement);
  }

  private static ImmutableList<Statement> extractCurrentStatementPath(Statement statement) {
    ImmutableList.Builder<Statement> currentStatementPathBuilder = new ImmutableList.Builder<>();
    extractCurrentStatementPathImplementation(statement, currentStatementPathBuilder);
    return currentStatementPathBuilder.build();
  }

  private static void extractCurrentStatementPathImplementation(
    Statement statement,
    ImmutableList.Builder<Statement> pathSoFar) {
    // Look for the statement that has the token marked as the cursor token
    // And that does not have any children with the same property.
    if (!Cursor.tokensHasCursor(statement.getTokens())) {
      return;
    }

    pathSoFar.add(statement);

    if (statement.getChildren().isEmpty()) {
      return;
    }

    Optional<? extends Statement> childWithCursor = statement
      .getChildren()
      .stream()
      .filter(child -> Cursor.tokensHasCursor(child.getTokens()))
      .findFirst();
    if (childWithCursor.isPresent()) {
      extractCurrentStatementPathImplementation(
        childWithCursor.get(),
        pathSoFar);
    }
  }

  @Override
  public String toString() {
    return "Statement{" +
      "tokens=" + tokens +
      ", children=" + children +
      '}';
  }
}
