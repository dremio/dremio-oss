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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchRequest {
  private final String query;
  private final String filter;
  private final String pageToken;

  @JsonCreator
  SearchRequest(
      @JsonProperty("query") String query,
      @JsonProperty("filter") String filter,
      @JsonProperty("pageToken") String pageToken) {
    this.query = query;
    this.filter = filter;
    this.pageToken = pageToken;
  }

  public String getQuery() {
    return query;
  }

  public String getFilter() {
    return filter;
  }

  public String getPageToken() {
    return pageToken;
  }
}
