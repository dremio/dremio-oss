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
package com.dremio.service.autocomplete.functions;

import java.util.Optional;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Dictionary of all the KeywordOrFunctionDetectors mapping name to the corresponding detector.
 * This list only covers the tokens that overlap with function in the system.
 */
public final class TokenTypeDetector {
  private static final TokenTypeDetectorImplementation ALWAYS_KEYWORD = TokenTypeDetectorImplementation.AlwaysKeyword.INSTANCE;
  private static final TokenTypeDetectorImplementation ALWAYS_FUNCTION = TokenTypeDetectorImplementation.AlwaysFunction.INSTANCE;
  private static final TokenTypeDetectorImplementation LEFT_RIGHT = TokenTypeDetectorImplementation.LeftRight.INSTANCE;
  private static final TokenTypeDetectorImplementation AGGREGATE = TokenTypeDetectorImplementation.Aggregate.INSTANCE;

  private static final ImmutableMap<String, TokenTypeDetectorImplementation> map = ImmutableMap.<String, TokenTypeDetectorImplementation>builder()
    // Binary Operators
    .put(",", ALWAYS_KEYWORD)
    .put("!=", ALWAYS_KEYWORD)
    .put("%", ALWAYS_KEYWORD)
    .put("%=", ALWAYS_KEYWORD)
    .put("&", ALWAYS_KEYWORD)
    .put("&&", ALWAYS_KEYWORD)
    .put("&=", ALWAYS_KEYWORD)
    .put("*", ALWAYS_KEYWORD)
    .put("*=", ALWAYS_KEYWORD)
    .put("+", ALWAYS_KEYWORD)
    .put("+=", ALWAYS_KEYWORD)
    .put("-", ALWAYS_KEYWORD)
    .put("-=", ALWAYS_KEYWORD)
    .put("->", ALWAYS_KEYWORD)
    .put("/", ALWAYS_KEYWORD)
    .put("/=", ALWAYS_KEYWORD)
    .put("<>", ALWAYS_KEYWORD)
    .put("<", ALWAYS_KEYWORD)
    .put("<<", ALWAYS_KEYWORD)
    .put("<<=", ALWAYS_KEYWORD)
    .put("<=", ALWAYS_KEYWORD)
    .put("=", ALWAYS_KEYWORD)
    .put("==", ALWAYS_KEYWORD)
    .put(">", ALWAYS_KEYWORD)
    .put(">=", ALWAYS_KEYWORD)
    .put(">>", ALWAYS_KEYWORD)
    .put(">>=", ALWAYS_KEYWORD)
    .put("^", ALWAYS_KEYWORD)
    .put("^=", ALWAYS_KEYWORD)
    .put("|", ALWAYS_KEYWORD)
    .put("|=", ALWAYS_KEYWORD)
    .put("||", ALWAYS_KEYWORD)
    .put("<=>", ALWAYS_KEYWORD)
    // Unary Operators
    .put("!", ALWAYS_KEYWORD)
    .put("~", ALWAYS_KEYWORD)
    .put("++", ALWAYS_KEYWORD)
    .put("--", ALWAYS_KEYWORD)
    // Intersection List
    .put("ABS", ALWAYS_FUNCTION)
    .put("ADD", ALWAYS_FUNCTION)
    .put("AND", ALWAYS_KEYWORD)
    .put("AVG", ALWAYS_FUNCTION)
    .put("CARDINALITY", ALWAYS_FUNCTION)
    .put("CAST", ALWAYS_FUNCTION)
    .put("CEIL", ALWAYS_FUNCTION)
    .put("CHARACTER_LENGTH", ALWAYS_FUNCTION)
    .put("CHAR_LENGTH", ALWAYS_FUNCTION)
    .put("CLASSIFIER", ALWAYS_FUNCTION)
    .put("COALESCE", ALWAYS_FUNCTION)
    .put("COLLECT", ALWAYS_FUNCTION)
    .put("CONTAINS", ALWAYS_FUNCTION)
    .put("CONVERT", ALWAYS_FUNCTION)
    .put("CORR", ALWAYS_FUNCTION)
    .put("COUNT", AGGREGATE)
    .put("COVAR_POP", ALWAYS_FUNCTION)
    .put("COVAR_SAMP", ALWAYS_FUNCTION)
    .put("CUME_DIST", ALWAYS_FUNCTION)
    .put("CURRENT_CATALOG", ALWAYS_FUNCTION)
    .put("CURRENT_DATE", ALWAYS_FUNCTION)
    .put("CURRENT_PATH", ALWAYS_FUNCTION)
    .put("CURRENT_ROLE", ALWAYS_FUNCTION)
    .put("CURRENT_SCHEMA", ALWAYS_FUNCTION)
    .put("CURRENT_TIME", ALWAYS_FUNCTION)
    .put("CURRENT_TIMESTAMP", ALWAYS_FUNCTION)
    .put("CURRENT_USER", ALWAYS_FUNCTION)
    .put("DENSE_RANK", ALWAYS_FUNCTION)
    .put("ELEMENT", ALWAYS_FUNCTION)
    .put("EVERY", ALWAYS_FUNCTION)
    .put("EXP", ALWAYS_FUNCTION)
    .put("EXTRACT", ALWAYS_FUNCTION)
    .put("FIRST", ALWAYS_FUNCTION)
    .put("FIRST_VALUE", ALWAYS_FUNCTION)
    .put("FLOOR", ALWAYS_FUNCTION)
    .put("FUSION", ALWAYS_FUNCTION)
    .put("GROUPING", ALWAYS_FUNCTION)
    .put("HASH", ALWAYS_FUNCTION)
    .put("HOUR", ALWAYS_FUNCTION)
    .put("LAG", ALWAYS_FUNCTION)
    .put("LAST", ALWAYS_FUNCTION)
    .put("LAST_VALUE", ALWAYS_FUNCTION)
    .put("LEAD", ALWAYS_FUNCTION)
    .put("LEFT", LEFT_RIGHT)
    .put("LENGTH", ALWAYS_FUNCTION)
    .put("LIKE", ALWAYS_KEYWORD)
    .put("LN", ALWAYS_FUNCTION)
    .put("LOCALTIME", ALWAYS_FUNCTION)
    .put("LOCALTIMESTAMP", ALWAYS_FUNCTION)
    .put("LOWER", ALWAYS_FUNCTION)
    .put("MAX", AGGREGATE)
    .put("MIN", AGGREGATE)
    .put("MINUTE", ALWAYS_FUNCTION)
    .put("MOD", ALWAYS_FUNCTION)
    .put("MONTH", ALWAYS_FUNCTION)
    .put("NEXT", ALWAYS_FUNCTION)
    .put("NOT", ALWAYS_KEYWORD)
    .put("NTH_VALUE", ALWAYS_FUNCTION)
    .put("NTILE", ALWAYS_FUNCTION)
    .put("NULLIF", ALWAYS_FUNCTION)
    .put("OCTET_LENGTH", ALWAYS_FUNCTION)
    .put("OR", ALWAYS_KEYWORD)
    .put("OVERLAY", ALWAYS_FUNCTION)
    .put("PERCENTILE_CONT", ALWAYS_FUNCTION)
    .put("PERCENTILE_DISC", ALWAYS_FUNCTION)
    .put("PERCENT_RANK", ALWAYS_FUNCTION)
    .put("POSITION", ALWAYS_FUNCTION)
    .put("POWER", ALWAYS_FUNCTION)
    .put("PREV", ALWAYS_FUNCTION)
    .put("QUARTER", ALWAYS_FUNCTION)
    .put("RANK", ALWAYS_FUNCTION)
    .put("REGR_SXX", ALWAYS_FUNCTION)
    .put("REGR_SYY", ALWAYS_FUNCTION)
    .put("REPLACE", ALWAYS_FUNCTION)
    .put("RIGHT", LEFT_RIGHT)
    .put("ROW_NUMBER", ALWAYS_FUNCTION)
    .put("SECOND", ALWAYS_FUNCTION)
    .put("SESSION", ALWAYS_FUNCTION)
    .put("SESSION_USER", ALWAYS_FUNCTION)
    .put("SIMILAR", ALWAYS_FUNCTION)
    .put("SQRT", ALWAYS_FUNCTION)
    .put("STDDEV_POP", ALWAYS_FUNCTION)
    .put("STDDEV_SAMP", ALWAYS_FUNCTION)
    .put("SUBSTRING", ALWAYS_FUNCTION)
    .put("SUM", AGGREGATE)
    .put("SYSTEM_USER", ALWAYS_FUNCTION)
    .put("TIMESTAMPADD", ALWAYS_FUNCTION)
    .put("TIMESTAMPDIFF", ALWAYS_FUNCTION)
    .put("TRANSLATE", ALWAYS_FUNCTION)
    .put("TRIM", ALWAYS_FUNCTION)
    .put("TRUNCATE", ALWAYS_FUNCTION)
    .put("UPPER", ALWAYS_FUNCTION)
    .put("USER", ALWAYS_FUNCTION)
    .put("VAR_POP", ALWAYS_FUNCTION)
    .put("VAR_SAMP", ALWAYS_FUNCTION)
    .put("WEEK", ALWAYS_FUNCTION)
    .put("YEAR", ALWAYS_FUNCTION)
    .build();

  private abstract static class TokenTypeDetectorImplementation {
    public abstract boolean isKeyword(ImmutableList<DremioToken> context);

    private static final class AlwaysKeyword extends TokenTypeDetectorImplementation {
      public static final AlwaysKeyword INSTANCE = new AlwaysKeyword();
      private AlwaysKeyword() {}
      @Override
      public boolean isKeyword(ImmutableList<DremioToken> context) {
        return true;
      }
    }

    private static final class AlwaysFunction extends TokenTypeDetectorImplementation {
      public static final AlwaysFunction INSTANCE = new AlwaysFunction();
      private AlwaysFunction() {}
      @Override
      public boolean isKeyword(ImmutableList<DremioToken> context) {
        return false;
      }
    }

    private static final class LeftRight extends TokenTypeDetectorImplementation {
      public static final LeftRight INSTANCE = new LeftRight();
      private LeftRight() {}

      @Override
      public boolean isKeyword(ImmutableList<DremioToken> context) {
        for (int i = context.size() -  1; i >= 0; i--) {
          DremioToken token = context.get(i);
          // If a FROM clause comes first then it's a KEYWORD
          if (token.getKind() == ParserImplConstants.FROM) {
            return true;
          }

          // If a SELECT clause comes first then it's a FUNCTION
          if (token.getKind() == ParserImplConstants.SELECT) {
            return false;
          }
        }

        return false;
      }
    }

    private static final class Aggregate extends TokenTypeDetectorImplementation {
      public static final Aggregate INSTANCE = new Aggregate();
      private Aggregate() {}
      @Override
      public boolean isKeyword(ImmutableList<DremioToken> context) {
        // We need to see if we are inside a reflection measure or not
        for (int i = context.size() -  1; i >= 0; i--) {
          DremioToken token = context.get(i);
          // If a MEASURES comes first then it's a KEYWORD
          if (token.getKind() == ParserImplConstants.MEASURES) {
            return true;
          }

          // If a SELECT clause comes first then it's a FUNCTION
          if (token.getKind() == ParserImplConstants.SELECT) {
            return false;
          }
        }

        return false;
      }
    }
  }

  public static Optional<Boolean> isKeyword(String token, ImmutableList<DremioToken> context) {
    Preconditions.checkNotNull(token);
    Preconditions.checkNotNull(context);
    TokenTypeDetectorImplementation implementation = map.get(token.toUpperCase());
    if (implementation == null) {
      return Optional.empty();
    }

    return Optional.of(implementation.isKeyword(context));
  }
}
