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
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.FROM;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.WHERE;

import org.apache.arrow.util.Preconditions;

import com.dremio.service.autocomplete.statements.visitors.StatementInputOutputVisitor;
import com.dremio.service.autocomplete.statements.visitors.StatementVisitor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;

/**
 * delete:
 *       DELETE FROM tablePrimary [ [ AS ] alias ]
 *       [ WHERE booleanExpression ]
 */
public final class DeleteStatement extends Statement {
  private final CatalogPath catalogPath;
  private final ImmutableList<DremioToken> condition;

  protected DeleteStatement(
    ImmutableList<DremioToken> tokens,
    CatalogPath catalogPath,
    ImmutableList<DremioToken> condition) {
    super(tokens, ImmutableList.of());
    this.catalogPath = catalogPath;
    this.condition = condition;
  }

  public CatalogPath getCatalogPath() {
    return catalogPath;
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

  public static DeleteStatement parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);
    return new Builder(tokenBuffer.toList())
      .addCatalogPath(parseCatalogPath(tokenBuffer))
      .addCondition(parseCondition(tokenBuffer))
      .build();
  }

  private static CatalogPath parseCatalogPath(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(DELETE);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(FROM);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> catalogPathTokens = tokenBuffer.readUntilKind(WHERE);
    return CatalogPath.parse(catalogPathTokens);
  }

  private static ImmutableList<DremioToken> parseCondition(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(WHERE);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return tokenBuffer.drainRemainingTokens();
  }

  private static final class Builder {
    private final ImmutableList<DremioToken> tokens;
    private CatalogPath catalogPath;
    private ImmutableList<DremioToken> condition;

    public Builder(ImmutableList<DremioToken> tokens) {
      this.tokens = tokens;
    }

    public Builder addCatalogPath(CatalogPath catalogPath) {
      this.catalogPath = catalogPath;
      return this;
    }

    public Builder addCondition(ImmutableList<DremioToken> condition) {
      this.condition = condition;
      return this;
    }

    public DeleteStatement build() {
      return new DeleteStatement(tokens, catalogPath, condition);
    }
  }
}
