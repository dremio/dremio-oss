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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.DremioToken;
import com.dremio.service.autocomplete.SqlQueryTokenizer;

/**
 * Datastructure to store the frequency counts of the token state transitions in a dataset of SQL queries.
 *
 * Suppose we add the following queries to the markov chain:
 * SELECT * FROM emp
 * SELECT name FROM emp
 *
 * Which when tokenized looks like so:
 * [SELECT] [STAR] [FROM] [IDENTIFIER]
 * [SELECT] [IDENTIFIER] [FROM] [IDENTIFIER]
 *
 * This gives us the following bigrams of tokens:
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
 * Note that this model gets better as we add more data to train on.
 * Another future improvement is to increase the length of the k-grams from 2 to 3, so
 * the suggestions become more context aware.
 */
public final class SqlTokenKindMarkovChain {
  private final Map<StateTransition, Integer> stateTransitionFrequencies;

  public SqlTokenKindMarkovChain() {
    this.stateTransitionFrequencies = new HashMap<>();
  }

  public void addQuery(String query) {
    final List<DremioToken> tokens = SqlQueryTokenizer.tokenize(query);
    // Prepend the start state token
    tokens.add(0, DremioToken.START_TOKEN);

    for (int i = 0; i < tokens.size() - 1; i++) {
      final DremioToken startToken = tokens.get(i);
      final DremioToken endToken = tokens.get(i + 1);
      this.addStateTransition(startToken, endToken);
    }
  }

  public void addQueries(Stream<String> queries) {
    queries.forEach(query -> this.addQuery(query));
  }

  public void addStateTransition(DremioToken startToken, DremioToken endToken) {
    final StateTransition transition = new StateTransition(startToken.getKind(), endToken.getKind());

    int frequency = this.stateTransitionFrequencies.getOrDefault(transition, 0);
    frequency++;

    this.stateTransitionFrequencies.put(transition, frequency);
  }

  public int getTransitionFrequency(DremioToken startToken, DremioToken endToken) {
    final StateTransition transition = new StateTransition(startToken.getKind(), endToken.getKind());

    int frequency = this.stateTransitionFrequencies.getOrDefault(transition, 0);
    frequency++;

    return frequency;
  }

  private static final class StateTransition {
    private final int startState;
    private final int endState;

    public StateTransition(int startState, int endState) {
      this.startState = startState;
      this.endState = endState;
    }

    public int getStartState() {
      return startState;
    }

    public int getEndState() {
      return endState;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StateTransition that = (StateTransition) o;
      return (startState == that.startState) && (endState == that.endState);
    }

    @Override
    public int hashCode() {
      return Objects.hash(startState, endState);
    }

    @Override
    public String toString() {
      return "(" + ParserImplConstants.tokenImage[startState] + ", " + ParserImplConstants.tokenImage[endState] + ")";
    }
  }
}
