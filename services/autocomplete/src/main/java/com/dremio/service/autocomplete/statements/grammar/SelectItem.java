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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.AS;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.DOT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.IDENTIFIER;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.STAR;

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 *  "*" | ( Expression [AS] [alias])
 */
public final class SelectItem extends LeafStatement {
  private final Expression expression;
  private final String alias;

  private SelectItem(
    ImmutableList<DremioToken> tokens,
    Expression expression,
    String alias) {
    super(tokens);
    this.expression = expression;
    this.alias = alias;
  }

  public Expression getExpression() {
    return expression;
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public Completions getCompletions(AutocompleteEngineContext autocompleteEngineContext) {
    boolean isStar = expression == null;
    if (isStar) {
      return Completions.EMPTY;
    }

    boolean hasAlias = alias != null;
    if (hasAlias) {
      return Completions.EMPTY;
    }

    boolean shouldCompleteExpression = expression.hasCursor();
    if (!shouldCompleteExpression) {
      return Completions.EMPTY;
    }

    ImmutableList<DremioToken> expressionTokens = Cursor
      .getTokensUntilCursor(expression.getTokens());

    boolean expressionIsPath = !expressionTokens.isEmpty() && expressionTokens
      .stream()
      .allMatch(token -> token.getKind() == IDENTIFIER || token.getKind() == DOT);
    // We don't want to complicate the suggestions if all the user has is a path
    return expressionIsPath ? Completions.EMPTY : expression.getCompletions(autocompleteEngineContext);
  }

  public static SelectItem parse(TokenBuffer tokenBuffer, ImmutableList<TableReference> tableReferences) {
    Preconditions.checkNotNull(tokenBuffer);
    Preconditions.checkNotNull(tableReferences);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    if (tokenBuffer.peekKind() == STAR) {
      return new SelectItem(tokenBuffer.drainRemainingTokens(), null, null);
    }

    ImmutableList<DremioToken> tokens = tokenBuffer.toList();
    ImmutableList<DremioToken> expressionTokens = tokenBuffer.readUntilKind(AS);
    tokenBuffer.readIfKind(AS);

    String alias = null;
    boolean hasImplicitAlias = (expressionTokens.size() > 2)
      && (expressionTokens.get(expressionTokens.size() - 1).getKind() == IDENTIFIER)
      && (expressionTokens.get(expressionTokens.size() - 2).getKind() != DOT);
    if (hasImplicitAlias) {
      expressionTokens = expressionTokens.subList(0, expressionTokens.size() - 1);
      alias = expressionTokens.get(expressionTokens.size() - 1).getImage();
    }

    Expression expression = Expression.parse(expressionTokens, tableReferences);
    if (tokenBuffer.isEmpty()) {
      return new SelectItem(tokens, expression, null);
    }

    ImmutableList<DremioToken> aliasTokens = tokenBuffer.drainRemainingTokens();
    if (aliasTokens.size() == 1) {
      alias = aliasTokens.get(0).getImage();
    }

    return new SelectItem(tokens, expression,alias);
  }
}
