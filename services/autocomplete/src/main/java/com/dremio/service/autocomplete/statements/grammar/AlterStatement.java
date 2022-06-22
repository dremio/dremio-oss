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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.ALTER;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.CREATE;

import java.util.Arrays;

import org.apache.arrow.util.Preconditions;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.statements.visitors.StatementInputOutputVisitor;
import com.dremio.service.autocomplete.statements.visitors.StatementVisitor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * ALTER (TABLE | VDS | PDS | DATASET) tableName
 * CREATE REFLECTION_CREATE_STATEMENT
 */
public final class AlterStatement extends Statement {
  private final Type type;
  private final CatalogPath catalogPath;

  private AlterStatement(
    ImmutableList<DremioToken> tokens,
    Type type,
    CatalogPath catalogPath,
    Statement child) {
    super(tokens, child != null ? ImmutableList.of(child) : ImmutableList.of());
    this.type = type;
    this.catalogPath = catalogPath;
  }

  public Type getType() {
    return type;
  }

  public CatalogPath getCatalogPath() {
    return catalogPath;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public <I, O> O accept(StatementInputOutputVisitor<I, O> visitor, I input) {
    return visitor.visit(this, input);
  }

  public static AlterStatement parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);

    return builder(tokenBuffer.toList())
      .type(parseType(tokenBuffer))
      .catalogPath(parseCatalogPath(tokenBuffer))
      .child(tokenBuffer)
      .build();
  }

  private static Type parseType(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(ALTER);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return Type.fromKind(tokenBuffer.readKind());
  }

  private static CatalogPath parseCatalogPath(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> catalogPathTokens = tokenBuffer.readUntilKind(CREATE);
    return CatalogPath.parse(catalogPathTokens);
  }

  private static Statement parseChildStatement(TokenBuffer tokenBuffer, CatalogPath catalogPath) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    switch (tokenBuffer.peekKind()) {
    case CREATE:
      return ReflectionCreateStatement.parse(tokenBuffer, catalogPath);
    default:
      throw new UnsupportedOperationException("UNKNOWN KIND: " + tokenBuffer.peekKind());
    }
  }

  public enum Type {
    UNKNOWN(-1),
    TABLE(ParserImplConstants.TABLE),
    VDS(ParserImplConstants.VDS),
    PDS(ParserImplConstants.PDS),
    DATASET(ParserImplConstants.DATASET);

    private final int kind;

    Type(int kind) {
      this.kind = kind;
    }

    public int getKind() {
      return kind;
    }

    public static AlterStatement.Type fromKind(int kind) {
      if (!isValidType(kind)) {
        return AlterStatement.Type.UNKNOWN;
      }

      return VALID_TYPES.get(kind);
    }

    private static final ImmutableMap<Integer, AlterStatement.Type> VALID_TYPES =
      Arrays.stream(AlterStatement.Type.values())
        .filter(s -> s.kind > 0)
        .collect(ImmutableMap.toImmutableMap(s -> s.kind, s -> s));

    private static boolean isValidType(int kind) {
      return VALID_TYPES.containsKey(kind);
    }
  }

  private static TypeStep builder(ImmutableList<DremioToken> tokens) {
    return new Builder(tokens);
  }

  private interface TypeStep {
    CatalogPathStep type(Type type);
  }

  private interface CatalogPathStep {
    ChildStep catalogPath(CatalogPath catalogPath);
  }

  private interface ChildStep {
    Build child(TokenBuffer tokenBuffer);
  }

  private interface Build {
    AlterStatement build();
  }

  private static final class Builder implements TypeStep, CatalogPathStep, ChildStep, Build{
    private final ImmutableList<DremioToken> tokens;

    private Type type;
    private CatalogPath catalogPath;
    private Statement child;

    public Builder(ImmutableList<DremioToken> tokens) {
      this.tokens = tokens;
    }

    @Override
    public Builder type(Type type) {
      this.type = type;
      return this;
    }

    @Override
    public Builder catalogPath(CatalogPath catalogPath) {
      this.catalogPath = catalogPath;
      return this;
    }

    @Override
    public Builder child(TokenBuffer tokenBuffer) {
      this.child = parseChildStatement(tokenBuffer, this.catalogPath);
      return this;
    }

    @Override
    public AlterStatement build() {
      return new AlterStatement(tokens, type, catalogPath, child);
    }
  }
}
