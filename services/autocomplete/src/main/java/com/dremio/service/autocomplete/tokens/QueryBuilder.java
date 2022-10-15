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

package com.dremio.service.autocomplete.tokens;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.statements.grammar.FromClause;
import com.dremio.service.autocomplete.statements.grammar.TableReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public final class QueryBuilder {
  private static final class Tokens {
    private static final DremioToken SELECT = DremioToken.createFromParserKind(ParserImplConstants.SELECT);
    private static final DremioToken FROM = DremioToken.createFromParserKind(ParserImplConstants.FROM);
    private static final DremioToken COMMA = DremioToken.createFromParserKind(ParserImplConstants.COMMA);

    private static final ImmutableList<DremioToken> STAR_COLUMN = ImmutableList.of(DremioToken.createFromParserKind(ParserImplConstants.STAR));

    private Tokens() {
    }
  }

  private QueryBuilder() {
  }

  public static String build(FromClause fromClause) {
    Preconditions.checkNotNull(fromClause);
    return build(fromClause.getTableReferences());
  }

  public static String build(ImmutableList<TableReference> tableReferences) {
    Preconditions.checkNotNull(tableReferences);
    return build(Tokens.STAR_COLUMN, tableReferences);
  }

  public static String build(
    ImmutableList<DremioToken> columnTokens,
    ImmutableList<TableReference> tableReferences) {
    Preconditions.checkNotNull(columnTokens);
    Preconditions.checkNotNull(tableReferences);

    ImmutableList.Builder<DremioToken> modifiedQueryTokensBuilder = new ImmutableList.Builder<DremioToken>()
      .add(Tokens.SELECT)
      .addAll(columnTokens);

    if (!tableReferences.isEmpty()) {
      modifiedQueryTokensBuilder.add(Tokens.FROM);

      for (int i = 0; i < tableReferences.size(); i++) {
        TableReference tableReference = tableReferences.get(i);
        modifiedQueryTokensBuilder.addAll(tableReference.getTokens());

        if (i != tableReferences.size() - 1) {
          modifiedQueryTokensBuilder.add(Tokens.COMMA);
        }
      }
    }

    return SqlQueryUntokenizer.untokenize(modifiedQueryTokensBuilder.build());
  }
}
