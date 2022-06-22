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
import com.dremio.service.autocomplete.statements.grammar.ParseException;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public final class TokenBuffer {
  private final ImmutableList<DremioToken> tokens;
  private int position;

  public TokenBuffer(ImmutableList<DremioToken> tokens) {
    Preconditions.checkNotNull(tokens);

    this.tokens = tokens;
  }

  public DremioToken peek() {
    if (this.isEmpty()) {
      return null;
    }

    return tokens.get(position);
  }

  public int peekKind() {
    DremioToken next = peek();
    if (next == null) {
      return -1;
    }

    return next.getKind();
  }

  public boolean kindIs(int kind) {
    return peekKind() == kind;
  }

  public DremioToken read() {
    if (this.isEmpty()) {
      return null;
    }

    DremioToken token = tokens.get(position);
    position++;
    return token;
  }

  public int readKind() {
    DremioToken token = read();
    if (token == null) {
      return -1;
    }

    return token.getKind();
  }

  public String readImage() {
    DremioToken token = read();
    if (token == null) {
      return null;
    }

    return token.getImage();
  }

  public DremioToken readIf(Predicate<DremioToken> predicate) {
    DremioToken token = peek();
    if (token == null) {
      return null;
    }

    if (!predicate.apply(token)) {
      return null;
    }

    return read();
  }

  public DremioToken readIfKind(int expectedKind) {
    return readIf(token -> token.getKind() == expectedKind);
  }

  public DremioToken readAndCheckKind(int expectedKind) {
    DremioToken dremioToken = read();
    if (dremioToken == null || dremioToken.getKind() != expectedKind) {
      throw ParseException.createUnexpectedToken(
        DremioToken.createFromParserKind(expectedKind),
        dremioToken);
    }

    return dremioToken;
  }

  public boolean isEmpty() {
    return position >= tokens.size();
  }

  public ImmutableList<DremioToken> toList() {
    return tokens.subList(position, tokens.size());
  }

  public ImmutableList<DremioToken> readUntil(Predicate<DremioToken> predicate) {
    int index;
    for (index = position; index < tokens.size(); index++) {
      // Work is done in the stop condition
      if (predicate.test(tokens.get(index))) {
        break;
      }
    }

    ImmutableList<DremioToken> tokensRead = tokens.subList(position, index);
    position = index;
    return tokensRead;
  }

  /**
   * Goes through the tokens and tries to find the first instance when there is a predicate match
   * at the same scope level.
   * If parenthesis are unbalanced, then open ones are ignored under assumption that this is where completion happens.
   */
  public ImmutableList<DremioToken> readUntilMatchAtSameLevel(Predicate<DremioToken> predicate) {
    int level = 0;
    // if we encounter a match with a positive level, it might be a match if parenthesis are unbalanced.
    int potentialIndex = tokens.size();
    for (int i = position; i < tokens.size(); i++) {
      DremioToken currentToken = tokens.get(i);
      boolean matches = predicate.test(currentToken);
      if (matches && level > 0) {
        potentialIndex = i;
      } else if (level == 0 && matches) {
        ImmutableList<DremioToken> tokensRead = tokens.subList(position, i);
        position = i;
        return tokensRead;
      } else if (currentToken.getKind() == ParserImplConstants.LPAREN) {
        level += 1;
      } else if (currentToken.getKind() == ParserImplConstants.RPAREN) {
        level -= 1;
      }

      if (level < 0) {
        potentialIndex = tokens.size();
      }
    }

    ImmutableList<DremioToken> tokensRead = tokens.subList(position, potentialIndex);
    position = potentialIndex;
    return tokensRead;
  }

  public ImmutableList<DremioToken> readUntilMatchKindAtSameLevel(int kind) {
    return readUntilMatchAtSameLevel(token -> token.getKind() == kind);
  }

  public ImmutableList<DremioToken> readUntilMatchKindsAtSameLevel(ImmutableSet<Integer> kinds) {
    return readUntilMatchAtSameLevel(token -> kinds.contains(token.getKind()));
  }

  public ImmutableList<DremioToken> drainRemainingTokens() {
    return readUntil(token -> false);
  }

  public ImmutableList<DremioToken> readUntilKind(int kind) {
    return readUntil(token -> token.getKind() == kind);
  }

  public ImmutableList<DremioToken> readUntilKinds(ImmutableSet<Integer> kinds) {
    return readUntil(token -> kinds.contains(token.getKind()));
  }
}
