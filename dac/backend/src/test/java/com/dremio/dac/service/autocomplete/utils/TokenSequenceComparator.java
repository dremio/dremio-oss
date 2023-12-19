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
package com.dremio.dac.service.autocomplete.utils;

import java.util.Comparator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Comparator for SQL query token sequences
 */
public final class TokenSequenceComparator implements Comparator<Integer> {
  private final ImmutableList<Integer> lastNMinusOneTokens;
  private final DremioTokenNGramFrequencyTable frequencyTable;

  public TokenSequenceComparator(
    ImmutableList<Integer> partialQuery,
    DremioTokenNGramFrequencyTable frequencyTable) {
    Preconditions.checkNotNull(partialQuery);
    Preconditions.checkNotNull(frequencyTable);
    partialQuery = ImmutableList.<Integer>builder().add(DremioToken.START_TOKEN.getKind()).addAll(partialQuery).build();
    int startIndex = Math.max(partialQuery.size() - frequencyTable.getN() + 1, 0);
    this.lastNMinusOneTokens = partialQuery.subList(startIndex, partialQuery.size());
    this.frequencyTable = frequencyTable;
  }

  @Override
  public int compare(Integer token1, Integer token2) {
    Preconditions.checkNotNull(token1);
    Preconditions.checkNotNull(token2);

    for (int i = 0; i < lastNMinusOneTokens.size(); i++) {
      ImmutableList<Integer> prefixChain = lastNMinusOneTokens.subList(i, lastNMinusOneTokens.size());
      ImmutableList<Integer> nGram1 = ImmutableList.<Integer>builder().addAll(prefixChain).add(token1).build();
      ImmutableList<Integer> nGram2 = ImmutableList.<Integer>builder().addAll(prefixChain).add(token2).build();

      int frequency1 = frequencyTable.getFrequency(nGram1);
      int frequency2 = frequencyTable.getFrequency(nGram2);
      int cmp = frequency2 - frequency1;
      if (cmp != 0) {
        return cmp;
      }
    }

    String image1 = NormalizedTokenDictionary.INSTANCE.indexToImage(token1);
    String image2 = NormalizedTokenDictionary.INSTANCE.indexToImage(token2);
    return image1.compareTo(image2);
  }
}
