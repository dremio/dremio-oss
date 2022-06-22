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

import com.google.common.collect.ImmutableList;

public final class Cursor {
  public static final String CURSOR_CHARACTER = "\07";
  private Cursor() {}

  public static boolean tokenEndsWithCursor(DremioToken token) {
    return token.getImage().endsWith(CURSOR_CHARACTER);
  }

  public static ImmutableList<DremioToken> getTokensUntilCursor(ImmutableList<DremioToken> tokens) {
    int cursorIndex = indexOfTokenWithCursor(tokens);
    return tokens.subList(0, cursorIndex);
  }

  public static boolean tokensHasCursor(ImmutableList<DremioToken> tokens) {
    return indexOfTokenWithCursor(tokens) != -1;
  }

  public static int indexOfTokenWithCursor(ImmutableList<DremioToken> tokens) {
    for (int i = 0; i < tokens.size(); i++) {
      DremioToken token = tokens.get(i);
      if (tokenEndsWithCursor(token)) {
        return i;
      }
    }

    return -1;
  }

  public static ImmutableList<DremioToken> tokenizeWithCursor(String corpus, int cursorPosition) {
    // Add the bell character which is never used in valid sql to represent the cursor.
    String corpusWithCursor = new StringBuilder(corpus)
      .insert(cursorPosition, CURSOR_CHARACTER)
      .toString();

    ImmutableList<DremioToken> tokens = SqlQueryTokenizer.tokenize(corpusWithCursor);
    return tokens;
  }
}
