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
package com.dremio.service.autocomplete;

import static com.dremio.service.autocomplete.SqlStateMachine.State.IN_CODE;
import static com.dremio.service.autocomplete.SqlStateMachine.State.IN_DOUBLE_QUOTE_STRING_LITERAL;
import static com.dremio.service.autocomplete.SqlStateMachine.State.IN_MULTILINE_COMMENT;
import static com.dremio.service.autocomplete.SqlStateMachine.State.IN_NUMERIC_LITERAL;
import static com.dremio.service.autocomplete.SqlStateMachine.State.IN_SINGLE_LINE_COMMENT;
import static com.dremio.service.autocomplete.SqlStateMachine.State.IN_SINGLE_QUOTE_STRING_LITERAL;

/**
 * State Machine for Sql texts.
 */
public final class SqlStateMachine {
  private SqlStateMachine() {}

  /**
   * States of a SQL query.
   */
  public enum State {
    IN_CODE,
    IN_MULTILINE_COMMENT,
    IN_SINGLE_LINE_COMMENT,
    IN_SINGLE_QUOTE_STRING_LITERAL,
    IN_DOUBLE_QUOTE_STRING_LITERAL,
    IN_NUMERIC_LITERAL
  }

  public static State getState(String corpus) {
    return getState(corpus, corpus.length());
  }

  public static State getState(String corpus, int until) {
    State state = IN_CODE;
    for (int i = 0; i < until; i++) {
      char character = corpus.charAt(i);
      switch (character) {
      case '/':
        if ((state == IN_CODE) && startsWithAtIndex(corpus, i, "/*")) {
          state = IN_MULTILINE_COMMENT;
          i++;
        }
        break;

      case '*':
        if ((state == IN_MULTILINE_COMMENT) && startsWithAtIndex(corpus, i, "*/")) {
          state = IN_CODE;
          i++;
        }
        break;

      case '-':
        if ((state == IN_CODE) && startsWithAtIndex(corpus, i, "--")) {
          state = IN_SINGLE_LINE_COMMENT;
          i++;
        }
        break;

      case '\n':
        switch (state) {
        case IN_SINGLE_LINE_COMMENT:
        case IN_NUMERIC_LITERAL:
          state = IN_CODE;
          break;

        default:
          // DO NOTHING
          break;
        }
        break;

      case '"':
        switch (state) {
        case IN_CODE:
          state = IN_DOUBLE_QUOTE_STRING_LITERAL;
          break;

        case IN_DOUBLE_QUOTE_STRING_LITERAL:
          state = IN_CODE;
          break;

        default:
          // Do Nothing
          break;
        }
        break;

      case '\'':
        switch (state) {
        case IN_CODE:
          state = IN_SINGLE_QUOTE_STRING_LITERAL;
          break;

        case IN_SINGLE_QUOTE_STRING_LITERAL:
          state = IN_CODE;
          break;

        default:
          // Do Nothing
          break;
        }
        break;

      case '\\':
        // treat the next character as an escaped character
        if (i + 1 < corpus.length()) {
          char nextChar = corpus.charAt(i + 1);
          switch (nextChar) {
          case '\'':
          case '"':
            i++;
            break;
          default:
            // Do Nothing
            break;
          }
        }
        break;

      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':
        if ((state == IN_CODE)) {
          if (i - 1 > 0) {
            char previousChar = corpus.charAt(i - 1);
            if (Character.isWhitespace(previousChar)) {
              state = IN_NUMERIC_LITERAL;
            }
          } else {
            state = IN_NUMERIC_LITERAL;
          }
        }
        break;

      case ' ':
      case '\r':
      case '\b':
      case '\t':
        if (state == IN_NUMERIC_LITERAL) {
          state = IN_CODE;
        }
        break;

      default:
        // Do Nothing
        break;
      }
    }

    return state;
  }

  private static boolean startsWithAtIndex(String corpus, int index, String prefix) {
    if (prefix.length() + index > corpus.length()) {
      return false;
    }

    for (int i = 0; i < prefix.length(); i++) {
      if(corpus.charAt(index + i) != prefix.charAt(i)) {
        return false;
      }
    }

    return true;
  }
}
