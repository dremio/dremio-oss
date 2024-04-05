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
package com.dremio.dac.service.autocomplete.utils;

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.*;

import com.dremio.exec.planner.sql.parser.impl.ParserImplTokenManager;
import com.dremio.exec.planner.sql.parser.impl.SimpleCharStream;
import com.dremio.exec.planner.sql.parser.impl.Token;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Tokenizer for SQL queries. */
public final class SqlQueryTokenizer {

  private static final char SINGLE_QUOTE_SYMBOL = '\'';
  private static final char DOUBLE_QUOTE_SYMBOL = '"';
  private static final char CURSOR_SYMBOL = Cursor.CURSOR_CHARACTER.charAt(0);

  private SqlQueryTokenizer() {}

  public static ImmutableList<DremioToken> tokenize(String query) {
    assert query != null;

    SimpleCharStream simpleCharStream =
        new SimpleCharStream(new ByteArrayInputStream(query.getBytes()));
    ParserImplTokenManager tokenManager = new ParserImplTokenManager(simpleCharStream);

    List<DremioToken> tokens = new ArrayList<>();

    while (true) {
      Token token;
      try {
        token = tokenManager.getNextToken();
      } catch (Exception | Error ex) {
        // We got a character that can't be read as a token.
        return null;
      }

      switch (token.kind) {
        case EOF:
          return ImmutableList.copyOf(tokens);

        case BEL:
          parseCursor(simpleCharStream, tokens, token);
          break;

        case DOUBLE_QUOTE:
          parseIdentifier(simpleCharStream, tokens);
          break;
        case QUOTE:
          parseStringLiteral(simpleCharStream, tokens);
          break;

        default:
          tokens.add(DremioToken.fromCalciteToken(token));
      }
    }
  }

  /**
   * Need to see if the bel character is attached to the previous token. If it is, then we make
   * cursor to be part of the previous identifier.
   */
  private static void parseCursor(
      SimpleCharStream stream, List<DremioToken> tokens, Token currentToken) {
    char previousCharacter = getPreviousCharacter(stream);
    boolean isAttachedToPreviousToken = Character.isLetterOrDigit(previousCharacter);
    if (isAttachedToPreviousToken) {
      DremioToken lastToken = tokens.remove(tokens.size() - 1);
      DremioToken lastTokenWithBellCharacter =
          new DremioToken(lastToken.getKind(), lastToken.getImage() + Cursor.CURSOR_CHARACTER);
      tokens.add(lastTokenWithBellCharacter);
    } else {
      tokens.add(DremioToken.fromCalciteToken(currentToken));
    }
  }

  /**
   * Input is ALWAYS at the Double quote character. Resulting stream will either be after EOF or
   * right after the closing double quote. If cursor is in the middle of the identifier, then - The
   * processing stops at the cursor and what came before is counted as identifier AND - Anything
   * that comes after the cursor BUT before the closing quote is discarded. NOTE: We might want to
   * do some postfix matching in the future on the rest of the identifier, but might be too much.
   */
  private static void parseIdentifier(SimpleCharStream stream, List<DremioToken> tokens) {
    StringBuilder tokenImageBuilder = new StringBuilder();
    boolean reachedCursor = false;
    while (true) {
      try {
        char readCharacter = stream.readChar();
        if (readCharacter == EOF || readCharacter == DOUBLE_QUOTE_SYMBOL) {
          break;
        }
        if (readCharacter == CURSOR_SYMBOL) {
          tokenImageBuilder.append(CURSOR_SYMBOL);
          reachedCursor = true;
        }
        if (!reachedCursor) {
          tokenImageBuilder.append(readCharacter);
        }
      } catch (IOException e) {
        break;
      }
    }
    tokens.add(new DremioToken(IDENTIFIER, tokenImageBuilder.toString()));
  }

  /**
   * Input is ALWAYS at the Single quote character. Output will always contain BOTH beginning and
   * end single quotes in the resulting image even if the input didn't. NOTE: This is a simple
   * implementation which greedily takes all input until the end of the input if quote is unclosed.
   * This is fine for a single quote, since we are guaranteed to never have cursor inside a single
   * quote. This is because AutocompleteEngine will not try to autocomplete these cases. NOTE: In
   * reality, parser will get here only if it finds unmatched quote, so very likely we will always
   * go until the end of the stream. It also likely means that the user input is incorrect.
   */
  private static void parseStringLiteral(SimpleCharStream stream, List<DremioToken> tokens) {
    StringBuilder tokenImageBuilder = new StringBuilder();
    tokenImageBuilder.append(SINGLE_QUOTE_SYMBOL);
    while (true) {
      try {
        char readCharacter = stream.readChar();
        if (readCharacter == EOF || readCharacter == SINGLE_QUOTE_SYMBOL) {
          break;
        }

        tokenImageBuilder.append(readCharacter);
      } catch (IOException e) {
        break;
      }
    }
    tokenImageBuilder.append(SINGLE_QUOTE_SYMBOL);
    tokens.add(new DremioToken(QUOTED_STRING, tokenImageBuilder.toString()));
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
