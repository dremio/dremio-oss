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
package com.dremio.common.utils;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URL;
import java.util.List;

import org.apache.commons.lang3.text.StrTokenizer;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;

/**
 * Utility methods to quote or find reserved keywords.
 */
public class SqlUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlUtils.class);

  // SqlUtils should not be in common, and this should be probably removed too
  // as it conflicts with ParserConfig
  public static final String LEGACY_USE_BACKTICKS = "dremio.legacy.backticks_for_identifiers";

  public static final char QUOTE; // backticks allowed to escape dot/keywords.
  public static final String QUOTE_WITH_ESCAPE; // backticks with escape character

  static {
    if ("true".equalsIgnoreCase(System.getProperty(LEGACY_USE_BACKTICKS))) {
      QUOTE = '`';
      QUOTE_WITH_ESCAPE = "``";
    } else {
      QUOTE = '"';
      QUOTE_WITH_ESCAPE = "\"\"";
    }
  }

  private static final String RESERVED_SQL_KEYWORDS_FILE = "sql-reserved-keywords.txt";

  private static final CharMatcher ALPHANUM_MATCHER =
      CharMatcher.inRange('0', '9' )
      .or(CharMatcher.inRange('a', 'z'))
      .or(CharMatcher.inRange('A', 'Z'))
      .or(CharMatcher.is('_'))
      .precomputed();

  private static final CharMatcher NEWLINE_MATCHER = CharMatcher.anyOf("\n\r").precomputed();

  /** list of reserved keywords in parser */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  public static ImmutableSet<String> RESERVED_SQL_KEYWORDS;

  static {
    URL parserListPath = null;
    try  {
      parserListPath = Resources.getResource(SqlUtils.RESERVED_SQL_KEYWORDS_FILE);
    } catch (final Throwable e) {
      logger.error(format("Failed to find Dremio SQL parser's reserved keywords file '%s' on classpath.",
          SqlUtils.RESERVED_SQL_KEYWORDS_FILE), e);
    }

    ImmutableSet<String> set = null;
    if (parserListPath != null) {
      try {
        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String line : Resources.readLines(parserListPath, UTF_8)) {
          String trimmedLine = line.trim();
          if (!trimmedLine.startsWith("#")) {
            builder.add(trimmedLine);
          }
        }
        set = builder.build();
      } catch (final Throwable e) {
        logger.error(format("Failed to load Dremio SQL parser's reserved keywords from file: %s",
            parserListPath.toString()), e);
      }
    }

    RESERVED_SQL_KEYWORDS = set;
  }

  public static String quotedCompound(List<String> strings){
    return quotedCompound(strings, QUOTER);
  }

  public static String quotedCompound(List<String> strings, Function<String, String> quoter){
    return FluentIterable.from(strings).transform(quoter).join(Joiner.on('.'));
  }

  /**
   * quote the identifier if it is a:
   *  - doesn't start with a character,
   *  - contains non-alphanumeric characters or
   *  - is a reserved keyword
   * @param id
   * @return
   */
  public static String quoteIdentifier(final String id) {
    if (id.isEmpty()) {
      return id;
    }

    if (isKeyword(id)) {
      return quoteString(id);
    }

    if (Character.isAlphabetic(id.charAt(0)) && ALPHANUM_MATCHER.matchesAllOf(id)) {
      return id;
    }

    // Special case
    if (NEWLINE_MATCHER.matchesAnyOf(id)) {
      return quoteUnicodeString(id);
    }

    return quoteString(id);
  }

  public static String quoteString(final String id) {
    StringBuilder sb = new StringBuilder();
    sb.append(QUOTE);
    sb.append(CharMatcher.is(QUOTE).replaceFrom(id, QUOTE_WITH_ESCAPE));
    sb.append(QUOTE);
    return sb.toString();
  }

  private static String quoteUnicodeString(final String id) {
    StringBuilder sb = new StringBuilder();
    sb.append("U&").append(QUOTE);
    for(int i = 0; i<id.length(); i++) {
      char c = id.charAt(i);
      switch(c) {
      case '\n':
        sb.append("\\000a");
        break;

      case '\r':
        sb.append("\\000d");
        break;

      default:
        if (c == QUOTE) {
          sb.append(QUOTE_WITH_ESCAPE);
        } else {
          sb.append(c);
        }
      }
    }

    sb.append(QUOTE);
    return sb.toString();
  }
  /**
   * Is the given id a reserved keyword?
   * @param id
   * @return
   */
  public static boolean isKeyword(String id) {
    Preconditions.checkState(RESERVED_SQL_KEYWORDS != null,
        "SQL reserved keyword list is not loaded. Please check the logs for error messages.");
    return RESERVED_SQL_KEYWORDS.contains(id.toUpperCase());
  }

  /**
   * Convert given string value as string literal in SQL.
   *
   * @param value
   * @return
   */
  public static final String stringLiteral(String value) {
    String escaped = value.replaceAll("'", "''");
    return "'" + escaped + "'";
  }

  /**
   * Parse the schema path into a list of schema entries.
   * @param schemaPath
   * @return
   */
  public static List<String> parseSchemaPath(String schemaPath) {
    return new StrTokenizer(schemaPath, '.', SqlUtils.QUOTE)
        .setIgnoreEmptyTokens(true)
        .getTokenList();
  }

  public static final Function<String, String> QUOTER = SqlUtils::quoteIdentifier;
}
