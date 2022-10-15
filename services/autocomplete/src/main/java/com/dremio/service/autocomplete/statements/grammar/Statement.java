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

import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;

/**
 * Base class for all statements
 */
public abstract class Statement {
  private final ImmutableList<DremioToken> tokens;
  private final ImmutableList<Statement> children;

  protected Statement(
    ImmutableList<DremioToken> tokens,
    ImmutableList<Statement> children) {
    this.tokens = tokens;
    this.children = children;
  }

  public ImmutableList<DremioToken> getTokens() {
    return tokens;
  }

  public ImmutableList<? extends Statement> getChildren() {
    return children;
  }

  public boolean hasCursor() {
    return Cursor.tokensHasCursor(tokens);
  }

  /**
   * This is somewhat similar to what a parser does, but much more limited in scope.
   * We also don't check correctness of the query in any way
   * We rely on specific keywords to infer scopes.
   * The result is a list of statements in a given query. IntelliSenseElement is something that
   */
  public static Statement parse(String corpus, int cursorPosition) {
    ImmutableList<DremioToken> tokens = Cursor.tokenizeWithCursor(corpus, cursorPosition);
    return StatementList.parse(TokenBuffer.create(tokens));
  }

  protected static ImmutableList<Statement> asListIgnoringNulls(Statement... elements) {
    ImmutableList.Builder<Statement> nonNullElements = new ImmutableList.Builder<>();
    for (Statement element : elements) {
      if (element != null) {
        nonNullElements.add(element);
      }
    }

    return nonNullElements.build();
  }

  @Override
  public String toString() {
    return "IntelliSenseElement{" +
      "tokens=" + tokens +
      ", children=" + children +
      '}';
  }
}
