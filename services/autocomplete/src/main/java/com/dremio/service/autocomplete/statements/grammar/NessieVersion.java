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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.AT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.BRANCH;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.COMMIT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.SNAPSHOT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.TAG;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.TIMESTAMP;

import java.util.Optional;

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.nessie.NessieElementResolver;
import com.dremio.service.autocomplete.nessie.NessieElementType;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * AT (BRANCH | TAG | COMMIT) VERSION
 */
public final class NessieVersion extends LeafStatement {
  private static final Completions TYPE_COMPLETIONS = Completions.builder()
    .addKeywords(ImmutableList.of(BRANCH,TAG,COMMIT,SNAPSHOT,TIMESTAMP))
    .build();
  private final Optional<NessieElementType> type;
  private final ImmutableList<DremioToken> versionTokens;

  private NessieVersion(
    ImmutableList<DremioToken> tokens,
    Optional<NessieElementType> type,
    ImmutableList<DremioToken> versionTokens) {
    super(tokens);
    this.type = type;
    this.versionTokens = versionTokens;
  }

  public Optional<NessieElementType> getType() {return type;}

  @Override
  public Completions getCompletions(AutocompleteEngineContext autocompleteEngineContext) {
    if (!type.isPresent()) {
      return TYPE_COMPLETIONS;
    }

    return Completions
      .builder()
      .addNessieElements(ImmutableList.copyOf(
        new NessieElementResolver(autocompleteEngineContext
          .getNessieElementReaderSupplier()
          .get())
        .resolve(type.get())))
      .build();
  }

  public static NessieVersion parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);

    ImmutableList<DremioToken> tokens = tokenBuffer.toList();
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(AT);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    DremioToken kindToken = tokenBuffer.read();
    Optional<NessieElementType> optionalNessieElementType = NessieElementType.tryConvertFromDremioToken(kindToken);
    if (tokenBuffer.isEmpty()) {
      return new NessieVersion(tokens, optionalNessieElementType, null);
    }

    ImmutableList<DremioToken> versionTokens = tokenBuffer.drainRemainingTokens();
    return new NessieVersion(tokens, optionalNessieElementType, versionTokens);
  }
}
