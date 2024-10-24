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
package com.dremio.dac.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nullable;

public class SearchResponse {
  private final String nextPageToken;
  private final List<SearchResultObject> results;
  private final List<SearchResultCategoryCount> categoryCounts;

  @JsonCreator
  SearchResponse(
      @JsonProperty("nextPageToken") String nextPageToken,
      @JsonProperty("results") List<SearchResultObject> results,
      @JsonProperty("categoryCounts") List<SearchResultCategoryCount> categoryCounts) {
    this.nextPageToken = nextPageToken;
    this.results = results;
    this.categoryCounts = categoryCounts;
  }

  @Nullable
  public String getNextPageToken() {
    return nextPageToken;
  }

  public List<SearchResultObject> getResults() {
    return results;
  }

  public List<SearchResultCategoryCount> getCategoryCounts() {
    return categoryCounts;
  }
}
