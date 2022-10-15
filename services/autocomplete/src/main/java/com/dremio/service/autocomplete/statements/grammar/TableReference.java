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
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.AT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.IDENTIFIER;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * tableReference:
 *       tablePrimary
 *       [ FOR SYSTEM_TIME AS OF expression ]
 *       [ pivot ]
 *       [ unpivot ]
 *       [ matchRecognize ]
 *       [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]
 *       [ AT (BRANCH | TAG | COMMIT) NESSIE_VERSION
 */
public final class TableReference extends Statement {
  private static final ImmutableSet<Integer> CATALOG_PATH_STOP_KINDS = new ImmutableSet.Builder<Integer>()
    .add(AS)
    .add(AT)
    .build();
  private final CatalogPath catalogPath;
  private final Alias alias;
  private final NessieVersion nessieVersion;

  private TableReference(
    ImmutableList<DremioToken> tokens,
    CatalogPath catalogPath,
    Alias alias,
    NessieVersion nessieVersion) {
    super(tokens, asListIgnoringNulls(catalogPath, alias, nessieVersion));
    this.catalogPath = catalogPath;
    this.alias = alias;
    this.nessieVersion = nessieVersion;
  }

  public static TableReference parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);

    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> tokens = tokenBuffer.toList();
    return new Builder(tokens)
      .addCatalogPath(parseCatalogPath(tokenBuffer))
      .addAlias(parseAlias(tokenBuffer))
      .addNessieVersion(parseNessieVersion(tokenBuffer))
      .build();
  }

  private static CatalogPath parseCatalogPath(TokenBuffer tokenBuffer) {
    // We need to read until we hit a stop kind or until we see two identifiers in a row,
    // since we could have space.folder.emp e which is an implied alias
    ImmutableList<DremioToken> tokens = tokenBuffer.toList();
    int stopIndex;
    for (stopIndex = 0; stopIndex < tokens.size(); stopIndex++) {
      DremioToken token = tokens.get(stopIndex);
      if (CATALOG_PATH_STOP_KINDS.contains(token.getKind())) {
        break;
      }

      if (token.getKind() == IDENTIFIER && (stopIndex + 1 < tokens.size()) && tokens.get(stopIndex + 1).getKind() == IDENTIFIER) {
        stopIndex++;
        break;
      }
    }

    ImmutableList<DremioToken> catalogPathTokens = tokenBuffer.read(stopIndex);
    return CatalogPath.parse(catalogPathTokens);
  }

  private static Alias parseAlias(TokenBuffer tokenBuffer) {
    ImmutableList<DremioToken> aliasTokens = tokenBuffer.readUntilKind(AT);
    return Alias.parse(aliasTokens);
  }

  private static NessieVersion parseNessieVersion(TokenBuffer tokenBuffer) {
    ImmutableList<DremioToken> nessieVersionTokens = tokenBuffer.drainRemainingTokens();
    return NessieVersion.parse(new TokenBuffer(nessieVersionTokens));
  }

  public CatalogPath getCatalogPath() {
    return catalogPath;
  }

  public Alias getAlias() {
    return alias;
  }

  public NessieVersion getNessieVersion() {
    return nessieVersion;
  }

  private static final class Builder {
    private final ImmutableList<DremioToken> tokens;
    private CatalogPath catalogPath;
    private Alias alias;
    private NessieVersion nessieVersion;

    public Builder(ImmutableList<DremioToken> tokens) {
      this.tokens = tokens;
    }

    public Builder addCatalogPath(CatalogPath catalogPath) {
      this.catalogPath = catalogPath;
      return this;
    }

    public Builder addAlias(Alias alias) {
      this.alias = alias;
      return this;
    }

    public Builder addNessieVersion(NessieVersion nessieVersion) {
      this.nessieVersion = nessieVersion;
      return this;
    }

    public TableReference build() {
      return new TableReference(tokens, catalogPath, alias, nessieVersion);
    }
  }
}
