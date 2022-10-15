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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.DELETE;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.WHERE;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * delete:
 *       DELETE FROM tablePrimary [ [ AS ] alias ]
 *       [ WHERE booleanExpression ]
 */
public final class DeleteStatement extends Statement {
  private final FromClause fromClause;
  private final Expression condition;

  protected DeleteStatement(
    ImmutableList<DremioToken> tokens,
    FromClause fromClause,
    Expression condition) {
    super(tokens, asListIgnoringNulls(fromClause, condition));
    this.fromClause = fromClause;
    this.condition = condition;
  }

  public FromClause getFromClause() {
    return fromClause;
  }

  public Expression getCondition() {
    return condition;
  }

  public static DeleteStatement parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);
    return new Builder(tokenBuffer.toList())
      .addFromClause(parseFromClause(tokenBuffer))
      .addCondition(tokenBuffer)
      .build();
  }

  private static FromClause parseFromClause(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(DELETE);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> fromClauseTokens = tokenBuffer.readUntilKind(WHERE);
    return FromClause.parse(fromClauseTokens);
  }

  private static Expression parseCondition(TokenBuffer tokenBuffer, FromClause fromClause) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(WHERE);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return Expression.parse(tokenBuffer.drainRemainingTokens(), fromClause);
  }

  private static final class Builder {
    private final ImmutableList<DremioToken> tokens;
    private FromClause fromClause;
    private Expression condition;

    public Builder(ImmutableList<DremioToken> tokens) {
      this.tokens = tokens;
    }

    public Builder addFromClause(FromClause fromClause) {
      this.fromClause = fromClause;
      return this;
    }

    public Builder addCondition(TokenBuffer tokenBuffer) {
      this.condition = parseCondition(tokenBuffer, fromClause);
      return this;
    }

    public DeleteStatement build() {
      return new DeleteStatement(tokens, fromClause, condition);
    }
  }
}
