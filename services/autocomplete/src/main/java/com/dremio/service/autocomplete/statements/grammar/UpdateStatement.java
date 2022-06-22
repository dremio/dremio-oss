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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.SET;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.UPDATE;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.WHERE;

import org.apache.arrow.util.Preconditions;

import com.dremio.service.autocomplete.statements.visitors.StatementInputOutputVisitor;
import com.dremio.service.autocomplete.statements.visitors.StatementVisitor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;

/**
 *  update:
 *       UPDATE tablePrimary
 *       SET assign [, assign ]*
 *       [ WHERE booleanExpression ]
 */
public final class UpdateStatement extends Statement {
  private final CatalogPath tablePrimary;
  private final ImmutableList<DremioToken> assignTokens;
  private final ImmutableList<DremioToken> condition;

  protected UpdateStatement(
    ImmutableList<DremioToken> tokens,
    CatalogPath tablePrimary,
    ImmutableList<DremioToken> assignTokens,
    ImmutableList<DremioToken> condition) {
    super(tokens, ImmutableList.of());

    this.tablePrimary = tablePrimary;
    this.assignTokens = assignTokens;
    this.condition = condition;
  }

  public CatalogPath getTablePrimary() {
    return tablePrimary;
  }

  public ImmutableList<DremioToken> getAssignTokens() {
    return assignTokens;
  }

  public ImmutableList<DremioToken> getCondition() {
    return condition;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public <I, O> O accept(StatementInputOutputVisitor<I, O> visitor, I input) {
    return visitor.visit(this, input);
  }

  public static UpdateStatement parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);

    return new Builder(tokenBuffer.toList())
      .addTablePrimary(parseTablePrimary(tokenBuffer))
      .addAssignTokens(parseAssignTokens(tokenBuffer))
      .addCondition(parseCondition(tokenBuffer))
      .build();
  }

  private static CatalogPath parseTablePrimary(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(UPDATE);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> setTokens = tokenBuffer.readUntilKind(SET);
    tokenBuffer.read();

    return CatalogPath.parse(setTokens);
  }

  private static ImmutableList<DremioToken> parseAssignTokens(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> assignTokens = tokenBuffer.readUntilKind(WHERE);
    tokenBuffer.read();

    return assignTokens;
  }

  private static ImmutableList<DremioToken> parseCondition(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return tokenBuffer.drainRemainingTokens();
  }

  private static final class Builder {
    private final ImmutableList<DremioToken> tokens;
    private CatalogPath tablePrimary;
    private ImmutableList<DremioToken> assignTokens;
    private ImmutableList<DremioToken> condition;

    private Builder(ImmutableList<DremioToken> tokens) {
      this.tokens = tokens;
    }

    public Builder addTablePrimary(CatalogPath tablePrimary) {
      this.tablePrimary = tablePrimary;
      return this;
    }

    public Builder addAssignTokens(ImmutableList<DremioToken> assignTokens) {
      this.assignTokens = assignTokens;
      return this;
    }

    public Builder addCondition(ImmutableList<DremioToken> condition) {
      this.condition = condition;
      return this;
    }

    public UpdateStatement build() {
      return new UpdateStatement(tokens, tablePrimary, assignTokens, condition);
    }
  }
}
