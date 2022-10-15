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

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 *  update:
 *       UPDATE tablePrimary
 *       SET assign [, assign ]*
 *       [ WHERE booleanExpression ]
 */
public final class UpdateStatement extends Statement {
  private final TableReference tablePrimary;
  private final Column assignment;
  private final Column condition;

  protected UpdateStatement(
    ImmutableList<DremioToken> tokens,
    TableReference tablePrimary,
    Column assignment,
    Column condition) {
    super(tokens, asListIgnoringNulls(tablePrimary, assignment, condition));

    this.tablePrimary = tablePrimary;
    this.assignment = assignment;
    this.condition = condition;
  }

  public TableReference getTablePrimary() {
    return tablePrimary;
  }

  public Column getAssignment() {
    return assignment;
  }

  public Column getCondition() {
    return condition;
  }

  public static UpdateStatement parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);

    return new Builder(tokenBuffer.toList())
      .addTablePrimary(parseTablePrimary(tokenBuffer))
      .addAssignTokens(tokenBuffer)
      .addCondition(tokenBuffer)
      .build();
  }

  private static TableReference parseTablePrimary(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(UPDATE);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> setTokens = tokenBuffer.readUntilKind(SET);
    tokenBuffer.read();

    return TableReference.parse(new TokenBuffer(setTokens));
  }

  private static Column parseAssignTokens(TokenBuffer tokenBuffer, TableReference tableReference) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> assignTokens = tokenBuffer.readUntilKind(WHERE);
    tokenBuffer.read();

    return Column.parse(assignTokens, tableReference);
  }

  private static Column parseCondition(TokenBuffer tokenBuffer, TableReference tableReference) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> tokens = tokenBuffer.drainRemainingTokens();
    return Column.parse(tokens, tableReference);
  }

  private static final class Builder {
    private final ImmutableList<DremioToken> tokens;
    private TableReference tablePrimary;
    private Column assignTokens;
    private Column condition;

    private Builder(ImmutableList<DremioToken> tokens) {
      this.tokens = tokens;
    }

    public Builder addTablePrimary(TableReference tablePrimary) {
      this.tablePrimary = tablePrimary;
      return this;
    }

    public Builder addAssignTokens(TokenBuffer tokenBuffer) {
      this.assignTokens = parseAssignTokens(tokenBuffer, tablePrimary);
      return this;
    }

    public Builder addCondition(TokenBuffer tokenBuffer) {
      this.condition = parseCondition(tokenBuffer, tablePrimary);
      return this;
    }

    public UpdateStatement build() {
      return new UpdateStatement(tokens, tablePrimary, assignTokens, condition);
    }
  }
}
