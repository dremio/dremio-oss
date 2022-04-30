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

import java.util.List;

import org.junit.Test;

import com.dremio.service.autocomplete.DremioToken;
import com.dremio.service.autocomplete.SqlQueryTokenizer;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Test for the SQL autocomplete resolver.
 */
public class TokenResolverTests {
  private static final SqlTokenKindMarkovChain MARKOV_CHAIN = ResourceBackedMarkovChainFactory.create("markov_chain_queries.sql");

  public static final TokenResolver TOKEN_RESOLVER = new TokenResolver(MARKOV_CHAIN);

  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(TokenResolverTests::executeTest)
      .add("EMPTY STRING", "")
      .add("MID SELECT", "S")
      .add("SELECT", "SELECT ")
      .add("SELECT STAR", "SELECT * ")
      .add("SELECT STAR FROM", "SELECT * FROM ")
      .add("SELECT STAR FROM IDENTIFIER", "SELECT * FROM emp ")
      .add("SELECT STAR FROM IDENTIFIER WHERE", "SELECT * FROM emp WHERE ")
      .add("SELECT STAR FROM IDENTIFIER WHERE IDENTIFIER", "SELECT * FROM emp WHERE age ")
      .add("MID TOKEN + VALID QUERY", "SELECT MI")
      .add("MID TOKEN + INVALID QUERY", "SELECT * FROM emp WHER")
      .add("FUNCTION", "SELECT ABS(")
      .runTests();
  }

  private static ResolverTestResults executeTest(String queryCorpus) {
    assert queryCorpus != null;

    ImmutableList<DremioToken> tokenizedCorpus = ImmutableList.copyOf(SqlQueryTokenizer.tokenize(queryCorpus));
    boolean hasTrailingWhitespace = (queryCorpus.length() != 0) && Character.isWhitespace(queryCorpus.charAt(queryCorpus.length() - 1));
    List<DremioToken> tokens = TOKEN_RESOLVER.resolve(
      tokenizedCorpus,
      hasTrailingWhitespace);

    final int numCompletions = 5;
    final String[] completionStrings = tokens
      .stream()
      .limit(numCompletions)
      .map(token -> token.getImage())
      .toArray(String[]::new);

    final int numRemainingResults = Math.max(tokens.size() - numCompletions, 0);

    return new ResolverTestResults(completionStrings, numRemainingResults);
  }

  private static final class ResolverTestResults {
    public final String[] tokens;
    public final int numRemainingResults;

    public ResolverTestResults(String[] tokens, int numRemainingResults) {
      this.tokens = tokens;
      this.numRemainingResults = numRemainingResults;
    }
  }
}
