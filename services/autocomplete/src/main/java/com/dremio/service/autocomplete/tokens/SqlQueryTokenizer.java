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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.dremio.exec.planner.sql.parser.impl.ParserImplTokenManager;
import com.dremio.exec.planner.sql.parser.impl.SimpleCharStream;
import com.dremio.exec.planner.sql.parser.impl.Token;
import com.google.common.collect.ImmutableList;

/**
 * Tokenizer for SQL queries.
 */
public final class SqlQueryTokenizer {
  private SqlQueryTokenizer() {
  }

  public static ImmutableList<DremioToken> tokenize(String query) {
    assert query != null;

    SimpleCharStream simpleCharStream = new SimpleCharStream(new ByteArrayInputStream(query.getBytes()));
    ParserImplTokenManager tokenManager = new ParserImplTokenManager(simpleCharStream);

    List<DremioToken> tokens = new ArrayList<>();

    while (true) {
      Token token = tokenManager.getNextToken();
      DremioToken dremioToken;

      switch (token.kind) {
      case EOF:
        return ImmutableList.copyOf(tokens);

      case BEL:
        // Need to see if the bel character is attached to the previous token
        boolean isAttachedToPreviousToken = Character.isLetterOrDigit(getPreviousCharacter(simpleCharStream));
        if (isAttachedToPreviousToken) {
          DremioToken lastToken = tokens.remove(tokens.size() - 1);
          DremioToken lastTokenWithBellCharacter = new DremioToken(lastToken.getKind(), lastToken.getImage() + Cursor.CURSOR_CHARACTER);
          dremioToken = lastTokenWithBellCharacter;
        } else {
          dremioToken = DremioToken.fromCalciteToken(token);
        }
        break;

      case DOUBLE_QUOTE:
      case QUOTE:
        // Read from the char stream directly until we see another matching quote
        char stopCharacter = token.kind == DOUBLE_QUOTE ? '"' : '\'';
        StringBuilder tokenImageBuilder = new StringBuilder();
        while (true) {
          try {
            char readCharacter = simpleCharStream.readChar();
            if (readCharacter == stopCharacter) {
              break;
            }

            tokenImageBuilder.append(readCharacter);
          } catch (IOException e) {
            break;
          }
        }

        dremioToken = new DremioToken(IDENTIFIER, tokenImageBuilder.toString());
        break;

      default:
        dremioToken = DremioToken.fromCalciteToken(token);
      }

      tokens.add(dremioToken);
    }
  }

  private static char getPreviousCharacter(SimpleCharStream simpleCharStream) {
    simpleCharStream.backup(2);

    try {
      char previousCharacter = simpleCharStream.readChar();
      simpleCharStream.readChar();
      return previousCharacter;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
