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

import java.util.Arrays;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * DROP (DDL) statement.
 *
 * DROP DROPTYPE [ IF EXISTS ] name
 *
 * DROPTYPE
 *  | BRANCH
 *  | ROLE
 *  | TABLE
 *  | TAG
 *  | VDS
 *  | VIEW
 *
 */
public final class DropStatement extends Statement {
  private final Type type;
  private final CatalogPath catalogPath;

  private DropStatement(
    ImmutableList<DremioToken> tokens,
    Type type,
    CatalogPath catalogPath) {
    super(tokens, asListIgnoringNulls(catalogPath));
    this.type = type;
    this.catalogPath = catalogPath;
  }

  public Type getDropType() {
    return type;
  }

  public CatalogPath getCatalogPath() {
    return catalogPath;
  }

  public static DropStatement parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);
    return new Builder(tokenBuffer.toList())
      .addType(parseType(tokenBuffer))
      .addCatalogPath(parseCatalogPath(tokenBuffer))
      .build();
  }

  private static Type parseType(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(ParserImplConstants.DROP);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return Type.getDropType(tokenBuffer.readKind());
  }

  private static CatalogPath parseCatalogPath(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }
    tokenBuffer.readIfKind(ParserImplConstants.IF);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readIfKind(ParserImplConstants.EXISTS);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return CatalogPath.parse(tokenBuffer.drainRemainingTokens());
  }

  public enum Type {
    UNKNOWN(-1),
    BRANCH(ParserImplConstants.BRANCH),
    ROLE(ParserImplConstants.ROLE),
    TABLE(ParserImplConstants.TABLE),
    TAG(ParserImplConstants.TAG),
    VDS(ParserImplConstants.VDS),
    VIEW(ParserImplConstants.VIEW);

    private final int kind;

    Type(int kind) {
      this.kind = kind;
    }

    public int getKind() {
      return kind;
    }

    public static Type getDropType(int kind) {
      if (!isValidDropType(kind)) {
        return Type.UNKNOWN;
      }

      return VALID_DROP_TYPES.get(kind);
    }

    private static final ImmutableMap<Integer, Type> VALID_DROP_TYPES =
      Arrays.stream(Type.values())
        .filter(s -> s.kind > 0)
        .collect(ImmutableMap.toImmutableMap(s -> s.kind, s -> s));

    private static boolean isValidDropType(int kind) {
      return VALID_DROP_TYPES.containsKey(kind);
    }
  }

  private static final class Builder {
    private final ImmutableList<DremioToken> tokens;
    private Type type;
    private CatalogPath catalogPath;

    public Builder(ImmutableList<DremioToken> tokens) {
      this.tokens = tokens;
      this.type = Type.UNKNOWN;
      this.catalogPath = CatalogPath.EMPTY;
    }

    public Builder addType(Type type) {
      this.type = type;
      return this;
    }

    public Builder addCatalogPath(CatalogPath catalogPath) {
      this.catalogPath = catalogPath;
      return this;
    }

    public DropStatement build() {
      return new DropStatement(tokens, type, catalogPath);
    }
  }
}
