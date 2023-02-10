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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.COMMA;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.LPAREN;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.RPAREN;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * SELECT_ITEM (, SELECT_ITEM)*
 */
public final class SelectList extends Statement {
  private SelectList(
    ImmutableList<DremioToken> tokens,
    ImmutableList<SelectItem> items) {
    super(tokens, items);
  }

  public static SelectList parse(TokenBuffer tokenBuffer, ImmutableList<TableReference> tableReferences) {
    Preconditions.checkNotNull(tokenBuffer);
    Preconditions.checkNotNull(tableReferences);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    ImmutableList<DremioToken> tokens = tokenBuffer.toList();

    ImmutableList.Builder<SelectItem> selectItemListBuilder = new ImmutableList.Builder<>();
    while (!tokenBuffer.isEmpty()) {
      ImmutableList<DremioToken> subTokens = tokenBuffer.readUntilMatchKindAtSameLevel(COMMA);
      int level = 0;
      for (DremioToken subToken : subTokens) {
        switch (subToken.getKind()) {
        case LPAREN:
          level++;
          break;

        case RPAREN:
          level--;
          break;
        default:
          // Do NOTHING
        }
      }

      if (level > 0) {
        // This means we are still inside of a function
        // As a workaround to not have a full expression parser we will just read to the end.
        ImmutableList<DremioToken> remainingTokens = tokenBuffer.drainRemainingTokens();
        subTokens = ImmutableList.<DremioToken>builder().addAll(subTokens).addAll(remainingTokens).build();
      }

      SelectItem selectItem = SelectItem.parse(new TokenBuffer(subTokens), tableReferences);
      selectItemListBuilder.add(selectItem);
      tokenBuffer.readIfKind(COMMA);
    }

    return new SelectList(tokens, selectItemListBuilder.build());
  }
}
