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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

import com.dremio.exec.planner.sql.parser.impl.ParseException;
import com.dremio.exec.planner.sql.parser.impl.Token;
import com.dremio.service.autocomplete.functions.TokenTypeDetector;
import com.dremio.service.autocomplete.parsing.BaseSqlNodeParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import software.amazon.awssdk.utils.Either;

/**
 * Resolves what tokens can come next in a sql query text.
 */
public final class TokenResolver {
  private static final DremioTokenNGramFrequencyTable N_GRAM_FREQUENCY_TABLE = DremioTokenNGramFrequencyTable.create("markov_chain_queries.sql", 4);
  // Token that never occurs in the real query.
  private static final DremioToken invalidToken = new DremioToken(0, "\07");

  private TokenResolver() {}

  public static Predictions getNextPossibleTokens(ImmutableList<DremioToken> corpus) {
    Preconditions.checkNotNull(corpus);
    ImmutableList<DremioToken> corpusWithoutCursor = Cursor.getTokensUntilCursor(corpus);
    ImmutableList<DremioToken> corpusInCurrentStatement = getTokensInCurrentStatement(corpusWithoutCursor);
    Predictions predictions = getNextPossibleTokensImpl(corpusInCurrentStatement);

    TokenSequenceComparator comparator = new TokenSequenceComparator(
      corpusWithoutCursor.stream().map(token -> token.getKind()).collect(ImmutableList.toImmutableList()),
      N_GRAM_FREQUENCY_TABLE);

    return new Predictions(
      predictions.isIdentifierPossible(),
      ImmutableList.sortedCopyOf(
        comparator,
        predictions.getKeywords()));
  }

  private static Predictions getNextPossibleTokensImpl(ImmutableList<DremioToken> corpus) {
    Either<SqlNode, Predictions> parseResult = tryParse(corpus);
    if (parseResult.right().isPresent()) {
      return parseResult.right().get();
    }

    // If we make it here this means we have a proper query like: "SELECT * FROM emp "
    // We can add a dummy token to force the parser to give us expectedTokens:
    // FindCompletions("SELECT * FROM emp ") = FindCompletions("SELECT * FROM emp 42dummy")
    ImmutableList<DremioToken> corpusWithDummy = new ImmutableList.Builder<DremioToken>()
      .addAll(corpus)
      .add(invalidToken)
      .build();
    return getNextPossibleTokensImpl(corpusWithDummy);
  }

  public static final class Predictions {
    private final boolean isIdentifierPossible;
    private final ImmutableList<Integer> keywords;

    private Predictions(
      boolean isIdentifierPossible,
      ImmutableList<Integer> keywords) {
      Preconditions.checkNotNull(keywords);

      this.isIdentifierPossible = isIdentifierPossible;
      this.keywords = keywords;
    }

    public boolean isIdentifierPossible() {
      return isIdentifierPossible;
    }

    public ImmutableList<Integer> getKeywords() {
      return keywords;
    }
  }

  private static Either<SqlNode, Predictions> tryParse(ImmutableList<DremioToken> corpus) {
    try {
      SqlNode parsedQuery = BaseSqlNodeParser.INSTANCE.parseWithException(corpus);
      return Either.left(parsedQuery);
    } catch (SqlParseException sqlParseException) {
      Set<Integer> tokenKinds = getExpectedTokenKinds(sqlParseException, corpus);

      boolean isIdentifierPossible = false;
      ImmutableList.Builder<Integer> expectedTokensBuilder = new ImmutableList.Builder<>();
      for (Integer tokenKind : tokenKinds) {
        if (tokenKind == IDENTIFIER ) {
          isIdentifierPossible = true;
        }

        DremioToken token = DremioToken.createFromParserKind(tokenKind);

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

        expectedTokensBuilder.add(tokenKind);
      }

      Predictions predictions = new Predictions(
        isIdentifierPossible,
        expectedTokensBuilder.build());

      return Either.right(predictions);
    }
  }

  private static Set<Integer> getExpectedTokenKinds(
    SqlParseException sqlParseException,
    ImmutableList<DremioToken> corpus) {
    if (sqlParseException.getExpectedTokenSequences() != null) {
      // We get a list of expected tokens from the parse exception.
      // The way the parse exception is formatted is that it gives:
      //  1) A cursor for where the query hit an error
      //  2) What token sequences can come after that to complete the query.
      //
      // Now for autocomplete we have a limitation ...
      // We can't ask the user to back up the cursor in order to complete their query.
      // So the solution is to take the list of results from the parse exception
      // and filter the possible sequences down to only the ones that match the tokens the user has already submitted.
      ParseException parseException = (ParseException) sqlParseException.getCause();
      Token startOfUserPath = parseException.currentToken.next;

      List<Integer> pathUserHasChosen = new ArrayList<>();
      while (startOfUserPath != null) {
        boolean skipToken = (startOfUserPath.kind == EOF) || (startOfUserPath.kind == BEL);
        if (!skipToken) {
          pathUserHasChosen.add(startOfUserPath.kind);
        }

        startOfUserPath = startOfUserPath.next;
      }

      return Arrays
        .stream(sqlParseException.getExpectedTokenSequences())
        .map(intarray -> Arrays
          .stream(intarray)
          // <HINT_BEG> is not a useful autocomplete recommendation.
          .filter(tokenKind -> tokenKind != HINT_BEG)
          .boxed()
          .collect(ImmutableList.toImmutableList()))
        .filter(sequence -> sequenceStartsWithPrefix(sequence, pathUserHasChosen))
        // We only want to provide the next token to the user instead of the whole chain of tokens.
        .map(sequence -> sequence.get(pathUserHasChosen.size()))
        .collect(Collectors.toSet());
    }

    // For some reason the parser does not just return AND as an expected token inside of a BETWEEN operator
    if (sqlParseException.getMessage().equals("BETWEEN operator has no terminating AND")) {
      if (Iterables.getLast(corpus).getKind() == AND) {
        return ImmutableSet.of(IDENTIFIER);
      }

      return ImmutableSet.of(AND);
    }

    if (sqlParseException.getMessage().equals("Non-query expression encountered in illegal context")) {
      return ImmutableSet.of();
    }

    if (sqlParseException.getMessage().equals("CURSOR expression encountered in illegal context")) {
      return ImmutableSet.of();
    }

    throw new RuntimeException(
      "FAILED TO GET TOKENS FROM TOKEN RESOLVER. Corpus: " + SqlQueryUntokenizer.untokenize(corpus),
      sqlParseException);
  }

  private static boolean sequenceStartsWithPrefix(List<Integer> sequence, List<Integer> prefix) {
    if (prefix.size() >= sequence.size()) {
      return false;
    }

    for (int i = 0; i < prefix.size(); i++) {
      if (!sequence.get(i).equals(prefix.get(i))) {
        return false;
      }
    }

    return true;
  }

  private static ImmutableList<DremioToken> getTokensInCurrentStatement(ImmutableList<DremioToken> tokens) {
    ImmutableList<DremioToken> tokensForPrediction = Cursor.getTokensUntilCursor(tokens);
    int semiColonIndex = lastIndexOf(
      tokensForPrediction,
      token -> token.getKind() == SEMICOLON);
    return tokensForPrediction.subList(semiColonIndex + 1, tokensForPrediction.size());
  }

  private static <T> int lastIndexOf (
    ImmutableList<T> tokens,
    Predicate<T> predicate) {
    for (int i = tokens.size() - 1; i >= 0; i--) {
      T currentToken = tokens.get(i);
      if (predicate.test(currentToken)) {
        return i;
      }
    }
    return -1;
  }
}
