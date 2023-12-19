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
package com.dremio.plugins.dataplane.store;

import static com.dremio.exec.util.InformationSchemaCatalogUtil.getEscapeCharacter;

import java.util.Set;

import com.dremio.service.catalog.SearchQuery;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Helpers for converting from SearchQuery to CEL syntax
 */
final class SearchQueryToCelConversionUtilities {

  private SearchQueryToCelConversionUtilities() {}

  private static final ImmutableMap<Character, String> SEARCH_QUERY_SPECIAL_CHARACTERS_MAP =
    ImmutableMap.<Character, String>builder()
      .put('%', ".*")
      .put('_', ".")
      .build();
  // CEL uses RE2 syntax: https://github.com/google/re2/wiki/Syntax
  private static final Set<Character> RE2_SPECIAL_CHARACTERS =
    ImmutableSet.of('*', '+', '?', '(', ')', '|', '[', ']', ':', '^', '\\', '.', '{', '}');
  private static final char RE2_ESCAPE = '\\';

  /**
   * Converts a string into the equivalent CEL raw string literal. By using a
   * raw string wrapped in single quotes, only existing single quote characters
   * need to be escaped. This is particularly helpful for passing regex patterns
   * to a CEL matches filter.
   * <p>
   * See <a href="https://github.com/google/cel-spec/blob/master/doc/langdef.md#string-and-bytes-values">the spec</a>.
   */
  public static String convertToRawCelStringLiteral(String string) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < string.length(); i++) {
      char c = string.charAt(i);

      if ('\'' == c) {
        sb.append('\'');
      }
      sb.append(c);
    }

    return String.format("r'%s'", sb);
  }

  /**
   * Converts a SearchQuery Like object into the equivalent RE2 regex pattern.
   */
  public static String likeQueryToRe2Regex(SearchQuery.Like likeQuery) {
    String pattern = likeQuery.getPattern();
    String escape = likeQuery.getEscape();

    final char e = getEscapeCharacter(escape);

    StringBuilder sb = new StringBuilder();
    boolean lastCharacterWasEscape = false;
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);

      if (lastCharacterWasEscape) {
        appendRawCharacter(sb, c);
        lastCharacterWasEscape = false;
        continue;
      }

      if (c == e) {
        lastCharacterWasEscape = true;
        continue;
      }

      if (SEARCH_QUERY_SPECIAL_CHARACTERS_MAP.containsKey(c)) {
        sb.append(SEARCH_QUERY_SPECIAL_CHARACTERS_MAP.get(c));
      } else {
        appendRawCharacter(sb, c);
      }
    }

    return "^" + sb + "$";
  }

  private static void appendRawCharacter(StringBuilder sb, char c) {
    if (RE2_SPECIAL_CHARACTERS.contains(c)) {
      sb.append(RE2_ESCAPE);
    }
    sb.append(c);
  }
}
