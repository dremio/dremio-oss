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

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

/**
 * Datastructure to store the frequency counts of the n-grams of tokens in a dataset of SQL queries.
 *
 * Suppose we add the following queries to frequency table where n = 2:
 * SELECT * FROM emp
 * SELECT name FROM emp
 *
 * Which when tokenized looks like so:
 * [SELECT] [STAR] [FROM] [IDENTIFIER]
 * [SELECT] [IDENTIFIER] [FROM] [IDENTIFIER]
 *
 * This gives us the following 2-grams of tokens:
 * ([SELECT], [STAR]), ([STAR], [FROM]), ([FROM], [IDENTIFIER])
 * ([SELECT], [IDENTIFIER]), ([IDENTIFIER], [FROM]), ([FROM], [IDENTIFIER])
 *
 * And we can create a frequency dictionary from one token to another:
 *
 * {
 *   SELECT: {
 *     STAR: 1,
 *     IDENTIFIER: 1
 *   },
 *   STAR: {
 *     FROM: 1
 *   },
 *   FROM: {
 *     IDENTIFIER: 2
 *   },
 *   IDENTIFIER: {
 *     FROM: 1
 *   }
 * }
 *
 * Now let's say a user types "SELECT " and we need to come up with recommendations for the following tokens.
 * We can look into datastructure and see that tokens are likely to come after "SELECT":
 *
 * SELECT: {
 *   STAR: 1,
 *   IDENTIFIER 1
 * }
 *
 * Which means in this scenario we would recommend STAR or an IDENTIFIER and all other tokens are lower precedence.
 *
 * Note that this is just when n = 2.
 * We can increase n to become more context aware (make better predictions based on previous tokens),
 * but we don't want to increase it too much,
 * since as n increases the chains become longer and less likely that we will have seen any particular chain of tokens before.
 * To mitigate the limitation of increasing n and not seeing a chain we can add more data to increase the odds that all
 * chains that are possible have been seen before (with appropriate frequency).
 */
public final class DremioTokenNGramFrequencyTable {
  private final Map<ImmutableList<Integer>, Integer> frequencies;
  private final int n;

  public DremioTokenNGramFrequencyTable(int n) {
    this.frequencies = new HashMap<>();
    this.n = n;
  }

  public int getN() {
    return n;
  }

  public void updateFrequency(ImmutableList<Integer> ngram) {
    int frequency = this.frequencies.getOrDefault(ngram, 0);
    frequency++;

    this.frequencies.put(ngram, frequency);
  }

  public int getFrequency(ImmutableList<Integer> ngram) {
    return this.frequencies.getOrDefault(ngram, 0);
  }

  public void addQuery(String query) {
    ImmutableList<Integer> tokens = ImmutableList.<Integer>builder()
      .add(DremioToken.START_TOKEN.getKind())
      .addAll(SqlQueryTokenizer.tokenize(query).stream().map(token -> token.getKind()).collect(Collectors.toList()))
      .build();
    addTokens(tokens);
  }

  private void addTokens(ImmutableList<Integer> tokens) {
    for (int i = 1; i <= n; i++) {
      int stopIndex = Math.max(tokens.size() - i, 0);
      for (int j = 0; j < stopIndex; j++) {
        ImmutableList<Integer> nGram = tokens.subList(j, j + i);
        updateFrequency(nGram);
      }
    }
  }

  public static DremioTokenNGramFrequencyTable create(String resourcePath, int n) {
    Preconditions.checkNotNull(resourcePath);

    final URL url = Resources.getResource(resourcePath);
    if (url == null) {
      throw new RuntimeException("file not found! " + resourcePath);
    }

    String[] queries;
    try {
      queries = Resources
        .toString(url, Charsets.UTF_8)
        .split(System.lineSeparator());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    DremioTokenNGramFrequencyTable dremioTokenNGramFrequencyTable = new DremioTokenNGramFrequencyTable(n);
    for (String query : queries) {
      dremioTokenNGramFrequencyTable.addQuery(query);
    }

    return dremioTokenNGramFrequencyTable;
  }
}
