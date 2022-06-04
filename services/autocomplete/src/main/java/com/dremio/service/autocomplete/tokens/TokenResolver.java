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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import com.dremio.service.autocomplete.DremioToken;
import com.dremio.service.autocomplete.SqlQueryUntokenizer;
import com.google.common.collect.ImmutableList;

/**
 * Resolves what tokens can come next in a sql query text.
 */
public final class TokenResolver {
  private static final SqlParser Parser = SqlParser.create("", SqlParser.Config.DEFAULT);
  private static final DremioToken dummyToken = new DremioToken(0, "dummy42");

  private final SqlTokenKindMarkovChain markovChain;

  public TokenResolver(SqlTokenKindMarkovChain markovChain) {
    Preconditions.checkNotNull(markovChain);
    this.markovChain = markovChain;
  }

  public List<DremioToken> resolve(ImmutableList<DremioToken> corpus, boolean trailingWhitespace) {
    Preconditions.checkNotNull(corpus);

    List<DremioToken> tokens;
    try {
      SqlNode parsedQuery = Parser.parseQuery(SqlQueryUntokenizer.untokenize(corpus));
      // If we make it here this means one of two cases:
      if (trailingWhitespace) {
        // 1) A proper query (like "SELECT * FROM emp ")
        // We can add a dummy token to force the parser to give us expectedTokens:
        // FindCompletions("SELECT * FROM emp ") = FindCompletions("SELECT * FROM emp 42dummy")
        ImmutableList<DremioToken> corpusWithDummy = new ImmutableList.Builder<DremioToken>()
          .addAll(corpus)
          .add(dummyToken)
          .build();
        tokens = resolve(corpusWithDummy, true);
      } else {
        // 2) A query that is a prefix to another query (like "SELECT MED" for "SELECT MEDIAN(...)")
        // We can remove the last token and take only the completions that
        // start with the last token as a prefix
        // FindCompletions("SELECT MED") => FindCompletions("SELECT").filter(blah => blah.startsWith("MED"));
        // In the future this can be done more efficiently with a trie.
        DremioToken lastToken = corpus.get(corpus.size() - 1);
        corpus = corpus.subList(0, corpus.size() - 1);
        tokens = resolve(corpus, true)
          .stream()
          .filter(token -> token.getImage().toUpperCase().startsWith(lastToken.getImage().toUpperCase()))
          .collect(Collectors.toList());
      }
    } catch (SqlParseException sqlParseException) {
      int[][] tokenSequences = sqlParseException.getExpectedTokenSequences();
      if (tokenSequences == null) {
        DremioToken lastToken = corpus.get(corpus.size() - 1);
        corpus = corpus.subList(0, corpus.size() - 1);
        tokens = resolve(corpus, true)
          .stream()
          .filter(token -> token.getImage().toUpperCase().startsWith(lastToken.getImage().toUpperCase()))
          .collect(Collectors.toList());
      } else {
        Set<Integer> tokenKinds = new HashSet<>();
        for (int[] tokenSequence : tokenSequences) {
          // The parser sometimes will return the context leading up to the next suggested token.
          // For example parse("SELECT ABS(") will return:
          // ["(", "<IDENTIFIER>"]
          // We don't need "(" for autocomplete, so just take the identifier.
          // In the future do a prefix match instead of assuming the token sequence is length 2
          int lookupIndex;
          if (tokenSequence.length == 1) {
            lookupIndex = tokenSequence[0];
          } else {
            lookupIndex = tokenSequence[1];
          }

          String image = sqlParseException.getTokenImages()[lookupIndex];
          int kind = NormalizedTokenDictionary.INSTANCE.imageToIndex(image);
          tokenKinds.add(kind);
        }

        tokens = new ArrayList<>();
        for (Integer tokenKind : tokenKinds) {
          String normalizedImage = NormalizedTokenDictionary.INSTANCE.indexToImage(tokenKind);
          DremioToken token = new DremioToken(tokenKind, normalizedImage);

          tokens.add(token);
        }
      }
    }

    TokenSequenceComparator comparator = new TokenSequenceComparator(
      corpus,
      this.markovChain);

    tokens = ImmutableList.sortedCopyOf(
      comparator,
      tokens);

    return tokens;
  }
}
