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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.OPTIMIZE;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.REWRITE;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.TABLE;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public final class OptimizeStatement extends Statement {
  private OptimizeStatement(
    ImmutableList<DremioToken> tokens,
    TableReference table) {
    super(tokens, asListIgnoringNulls(table));
  }

  public static Statement parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);

    ImmutableList<DremioToken> tokens = tokenBuffer.toList();
    TableReference tableReference = parseTable(tokenBuffer);
    return new OptimizeStatement(tokens,
      tableReference);
  }

  private static TableReference parseTable(TokenBuffer tokenBuffer) {
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(OPTIMIZE);
    tokenBuffer.readAndCheckKind(TABLE);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> setTokens = tokenBuffer.readUntilKind(REWRITE);
    tokenBuffer.read();

    return TableReference.parse(new TokenBuffer(setTokens));
  }
}
