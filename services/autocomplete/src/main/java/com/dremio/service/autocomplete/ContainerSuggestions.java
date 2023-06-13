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
package com.dremio.service.autocomplete;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class ContainerSuggestions implements AutocompleteV2Response {
  private final String suggestionsType;
  private final Integer count;
  private final Integer maxCount;
  private final List<SuggestionEntity> containers;

  @JsonCreator
  public ContainerSuggestions(
    @JsonProperty("type") String suggestionsType,
    @JsonProperty("count") Integer count,
    @JsonProperty("maxCount") Integer maxCount,
    @JsonProperty("suggestions") List<SuggestionEntity> containers) {
    this.suggestionsType = suggestionsType;
    this.count = count;
    this.maxCount = maxCount;
    this.containers = containers;
  }

  @Override
  public String getSuggestionsType() {
    return suggestionsType;
  }

  @Override
  public Integer getCount() {
    return count;
  }

  @Override
  public Integer getMaxCount() {
    return maxCount;
  }

  public List<SuggestionEntity> getContainers() {
    return containers;
  }
}
