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

import java.util.List;

import com.dremio.dac.proto.model.dataset.CardExample;
import com.dremio.dac.util.JSONUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * To display in the UI the cards on extract/split/replace
 * @param <T> the rule type
 */
public class Card<T> {
  public static final int EXAMPLES_TO_SHOW = 3;

  private final T rule;
  private final List<CardExample> examples;
  private final int matchedCount;
  private final int unmatchedCount;
  private final String description;

  @JsonCreator
  public Card(
      @JsonProperty("rule") T rule,
      @JsonProperty("examplesList") List<CardExample> examples,
      @JsonProperty("matchedCount") int matchedCount,
      @JsonProperty("unmatchedCount") int unmatchedCount,
      @JsonProperty("description") String description) {
    super();
    this.rule = rule;
    this.examples = examples;
    this.matchedCount = matchedCount;
    this.unmatchedCount = unmatchedCount;
    this.description = description;
  }

  public T getRule() {
    return rule;
  }
  public List<CardExample> getExamplesList() {
    if (examples == null) {
      return null;
    }
    return examples.subList(0, (EXAMPLES_TO_SHOW > examples.size()) ? examples.size() : EXAMPLES_TO_SHOW);
  }
  public int getMatchedCount() {
    return matchedCount;
  }
  public int getUnmatchedCount() {
    return unmatchedCount;
  }
  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
     return JSONUtil.toString(this);
  }

}
