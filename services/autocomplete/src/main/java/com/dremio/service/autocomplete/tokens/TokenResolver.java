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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.functions.TokenTypeDetector;
import com.dremio.service.autocomplete.parsing.ParserFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import software.amazon.awssdk.utils.Either;

/**
 * Resolves what tokens can come next in a sql query text.
 */
public final class TokenResolver {
  private static final DremioTokenNGramFrequencyTable N_GRAM_FREQUENCY_TABLE = DremioTokenNGramFrequencyTable.create("markov_chain_queries.sql", 4);
  // Token that never occurs in the real query.
  private static final DremioToken invalidToken = new DremioToken(0, "dummy42\07");

  private TokenResolver() {}

  public static Predictions getNextPossibleTokens(ImmutableList<DremioToken> corpus) {
    Preconditions.checkNotNull(corpus);

    Either<SqlNode, Predictions> parseResult = tryParse(corpus);
    Predictions predictions;
    if (parseResult.left().isPresent()) {
      // If we make it here this means we have a proper query like: "SELECT * FROM emp "
      // We can add a dummy token to force the parser to give us expectedTokens:
      // FindCompletions("SELECT * FROM emp ") = FindCompletions("SELECT * FROM emp 42dummy")
      ImmutableList<DremioToken> corpusWithDummy = new ImmutableList.Builder<DremioToken>()
        .addAll(corpus)
        .add(invalidToken)
        .build();
      predictions = getNextPossibleTokens(corpusWithDummy);
    } else {
      predictions = parseResult.right().get();
    }

    TokenSequenceComparator comparator = new TokenSequenceComparator(
      corpus,
      N_GRAM_FREQUENCY_TABLE);

    return new Predictions(
      predictions.isIdentifierPossible(),
      ImmutableList.sortedCopyOf(
        comparator,
        predictions.getKeywords()));
  }

  public static final class Predictions {
    private final boolean isIdentifierPossible;
    private final ImmutableList<DremioToken> keywords;

    private Predictions(
      boolean isIdentifierPossible,
      ImmutableList<DremioToken> keywords) {
      Preconditions.checkNotNull(keywords);

      this.isIdentifierPossible = isIdentifierPossible;
      this.keywords = keywords;
    }

    public boolean isIdentifierPossible() {
      return isIdentifierPossible;
    }

    public ImmutableList<DremioToken> getKeywords() {
      return keywords;
    }
  }

  private static Either<SqlNode, Predictions> tryParse(ImmutableList<DremioToken> corpus) {
    try {
      String sql = SqlQueryUntokenizer.untokenize(corpus);
      SqlNode parsedQuery = ParserFactory.create(sql).parseStmt();
      return Either.left(parsedQuery);
    } catch (SqlParseException sqlParseException) {
      Collection<String> expectedTokenNames = getExpectedTokenName(sqlParseException, corpus);
      Set<Integer> tokenKinds = new HashSet<>();
      for (String tokenName : expectedTokenNames) {
        int kind = NormalizedTokenDictionary.INSTANCE.imageToIndex(tokenName);
        tokenKinds.add(kind);
      }

      boolean isIdentifierPossible = false;
      ImmutableList.Builder<DremioToken> expectedTokensBuilder = new ImmutableList.Builder<>();
      for (Integer tokenKind : tokenKinds) {
        DremioToken token = DremioToken.createFromParserKind(tokenKind);
        if (token.getKind() == ParserImplConstants.IDENTIFIER) {
          isIdentifierPossible = true;
        }

        // Remove placeholder tokens
        if ((token.getImage().startsWith("<")) && (token.getImage().endsWith(">"))) {
          continue;
        }

        // Remove tokens that are really function names
        // Since they are technically apart of the grammar,
        // but not autocomplete.
        boolean isKeyword = TokenTypeDetector.isKeyword(token.getImage(), corpus).orElse(true);
        if (!isKeyword) {
          continue;
        }

        expectedTokensBuilder.add(token);
      }

      Predictions predictions = new Predictions(
        isIdentifierPossible,
        expectedTokensBuilder.build());

      return Either.right(predictions);
    }
  }

  private static Collection<String> getExpectedTokenName(
    SqlParseException sqlParseException,
    ImmutableList<DremioToken> corpus) {
    if (!sqlParseException.getMessage().equals("BETWEEN operator has no terminating AND")) {
      return sqlParseException.getExpectedTokenNames();
    }

    // For some reason the parser does not just return AND as an expected token inside of a BETWEEN operator
    if (Iterables.getLast(corpus).getKind() == ParserImplConstants.AND) {
      return ImmutableList.of("<IDENTIFIER>");
    }

    return ImmutableList.of("\"AND\"");
  }
}
