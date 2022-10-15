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

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * [ [ <AS> ] alias = SimpleIdentifier() ]
 */
public final class Alias extends LeafStatement {
  private final String alias;

  protected Alias(ImmutableList<DremioToken> tokens, String alias) {
    super(tokens);
    this.alias = alias;
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public Completions getCompletions(AutocompleteEngineContext autocompleteEngineContext) {
    // The system treats alias as an identifier, but really it's a string literal, so we can't give suggestions.
    return Completions.EMPTY;
  }

  public static Alias parse(ImmutableList<DremioToken> tokens) {
    Preconditions.checkNotNull(tokens);

    return parse(new TokenBuffer(tokens));
  }

  public static Alias parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);

    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> tokens = tokenBuffer.toList();

    tokenBuffer.readIfKind(AS);
    if (tokenBuffer.isEmpty()) {
      return new Alias(tokens, null);
    }

    String alias = tokenBuffer.readImage();
    return new Alias(tokens, alias);
  }
}
