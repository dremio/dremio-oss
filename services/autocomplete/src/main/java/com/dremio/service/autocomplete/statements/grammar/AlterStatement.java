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

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * ALTER (TABLE | VDS | PDS | DATASET) tableName
 * CREATE REFLECTION_CREATE_STATEMENT
 */
public final class AlterStatement extends Statement {
  private final Type type;
  private final TableReference tableReference;

  private AlterStatement(
    ImmutableList<DremioToken> tokens,
    Type type,
    TableReference tableReference,
    Statement child) {
    super(tokens, asListIgnoringNulls(tableReference, child));
    this.type = type;
    this.tableReference = tableReference;
  }

  public Type getType() {
    return type;
  }

  public TableReference getTableReference() {
    return tableReference;
  }

  public static AlterStatement parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);

    return builder(tokenBuffer.toList())
      .type(parseType(tokenBuffer))
      .catalogPath(parseTableReference(tokenBuffer))
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

  private static TableReference parseTableReference(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> tableReferenceTokens = tokenBuffer.readUntilKind(CREATE);
    return TableReference.parse(new TokenBuffer(tableReferenceTokens));
  }

  private static Statement parseChildStatement(TokenBuffer tokenBuffer, TableReference tableReference) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    switch (tokenBuffer.peekKind()) {
    case CREATE:
      return ReflectionCreateStatement.parse(tokenBuffer, tableReference);
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
    TableReferenceStep type(Type type);
  }

  private interface TableReferenceStep {
    ChildStep catalogPath(TableReference tableReference);
  }

  private interface ChildStep {
    Build child(TokenBuffer tokenBuffer);
  }

  private interface Build {
    AlterStatement build();
  }

  private static final class Builder implements TypeStep, TableReferenceStep, ChildStep, Build{
    private final ImmutableList<DremioToken> tokens;

    private Type type;
    private TableReference tableReference;
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
    public Builder catalogPath(TableReference tableReference) {
      this.tableReference = tableReference;
      return this;
    }

    @Override
    public Builder child(TokenBuffer tokenBuffer) {
      this.child = parseChildStatement(tokenBuffer, tableReference);
      return this;
    }

    @Override
    public AlterStatement build() {
      return new AlterStatement(tokens, type, tableReference, child);
    }
  }
}
