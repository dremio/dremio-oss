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
package com.dremio.dac.explore.model.extract;

import com.dremio.dac.explore.model.HistogramValue;
import com.dremio.dac.proto.model.dataset.ReplacePatternRule;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** ExtractCards model */
public class ReplaceCards {

  private final List<Card<ReplacePatternRule>> cards;

  private final ReplaceValuesCard values;

  @JsonCreator
  public ReplaceCards(
      @JsonProperty("cards") List<Card<ReplacePatternRule>> cards,
      @JsonProperty("values") ReplaceValuesCard values) {
    this.cards = cards;
    this.values = values;
  }

  public List<Card<ReplacePatternRule>> getCards() {
    return cards;
  }

  public ReplaceValuesCard getValues() {
    return values;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }

  /** support for displaying the replace values card */
  public static class ReplaceValuesCard {

    private final List<HistogramValue> availableValues;
    private final long matchedValues;
    private final long unmatchedValues;
    private final long availableValuesCount;

    @JsonCreator
    public ReplaceValuesCard(
        @JsonProperty("availableValues") List<HistogramValue> availableValues,
        @JsonProperty("matchedValues") long matchedValues,
        @JsonProperty("unmatchedValues") long unmatchedValues,
        @JsonProperty("availableValuesCount") long availableValuesCount) {
      super();
      this.availableValues = availableValues;
      this.matchedValues = matchedValues;
      this.unmatchedValues = unmatchedValues;
      this.availableValuesCount = availableValuesCount;
    }

    public List<HistogramValue> getAvailableValues() {
      return availableValues;
    }

    public long getMatchedValues() {
      return matchedValues;
    }

    public long getUnmatchedValues() {
      return unmatchedValues;
    }

    public long getAvailableValuesCount() {
      return availableValuesCount;
    }
  }
}
