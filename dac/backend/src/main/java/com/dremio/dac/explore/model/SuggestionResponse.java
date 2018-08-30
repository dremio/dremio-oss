/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.explore.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

/**
 * SQL Analyze API Response object to return for SQL query suggestions.
 */
public class SuggestionResponse {

  private ImmutableList<Suggestion> suggestions;

  @JsonCreator
  public SuggestionResponse(@JsonProperty("suggestions") List<Suggestion> suggestions) {
    this.suggestions = ImmutableList.copyOf(suggestions);
  }

  /**
   * Get list of suggestions.
   */
  public ImmutableList<Suggestion> getSuggestions() {
    return suggestions;
  }

  @Override
  public String toString() {
    return "SuggestionResponse{" +
      "suggestions=" + suggestions +
      '}';
  }

  /**
   * Query Suggestion object to return in SQL Analyze response.
   */
  public static class Suggestion {

    private final String name;
    private final String type;

    @JsonCreator
    public Suggestion(
      @JsonProperty("name") String name,
      @JsonProperty("type") String type) {
      this.name = name;
      this.type = type;
    }

    /**
     * Get Name
     *
     * @return the suggestion value.
     */
    public String getName() {
      return name;
    }

    /**
     * Get Type
     *
     * @return the suggestion type.
     */
    public String getType() {
      return type;
    }

    @Override
    public String toString() {
      return "Suggestion{" +
        "name='" + name + '\'' +
        ", type='" + type + '\'' +
        '}';
    }
  }
}
