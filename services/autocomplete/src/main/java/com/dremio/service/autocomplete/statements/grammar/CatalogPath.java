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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.IDENTIFIER;

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.google.common.collect.ImmutableList;

/**
 * Path in a Catalog.
 */
public final class CatalogPath extends LeafStatement {
  public static final CatalogPath EMPTY = new CatalogPath(ImmutableList.of(), ImmutableList.of());
  private final ImmutableList<String> pathTokens;

  private CatalogPath(
    ImmutableList<DremioToken> tokens,
    ImmutableList<String> pathTokens) {
    super(tokens);
    this.pathTokens = pathTokens;
  }

  public ImmutableList<String> getPathTokens() {
    return pathTokens;
  }

  public static CatalogPath parse(ImmutableList<DremioToken> tokens) {
    if (tokens.isEmpty()) {
      return CatalogPath.EMPTY;
    }

    /*
    The path is identifiers delimited by dots with an optional alias and AT statement. For example:
    "myspace.myfolder.mytable AS tbl AT BRANCH myBranch" needs to parse into [myspace, myfolder, mytable]
     */
    ImmutableList.Builder<DremioToken> tokensBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<String> identifiersBuilder = new ImmutableList.Builder<>();
    for (DremioToken token : tokens) {
      switch (token.getKind()) {
      case IDENTIFIER:
        identifiersBuilder.add(token.getImage());
      default:
        tokensBuilder.add(token);
        break;
      }
    }

    return new CatalogPath(tokensBuilder.build(), identifiersBuilder.build());
  }

  @Override
  public Completions getCompletions(AutocompleteEngineContext autocompleteEngineContext) {
    return Completions
      .builder()
      .addCatalogNodes(autocompleteEngineContext
        .getAutocompleteSchemaProvider()
        .getChildrenInScope(pathTokens))
      .build();
  }
}
