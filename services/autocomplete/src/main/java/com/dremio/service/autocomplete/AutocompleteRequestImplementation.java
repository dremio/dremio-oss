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

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * SQLAnalyze API Request
 */
public final class AutocompleteRequestImplementation {

  private final String query;
  private final List<String> context;
  private final int cursor;

  @JsonCreator
  public AutocompleteRequestImplementation(@JsonProperty("query") String query,
                        @JsonProperty("context") List<String> context,
                        @JsonProperty("cursor") int cursor) {
    this.query = query;
    this.context = new ArrayList<>(context);
    this.cursor = cursor;
  }

  /**
   * Get query SQL
   */
  public String getQuery() {
    return query;
  }

  /**
   * Get query context
   */
  public List<String> getContext() {
    return context;
  }

  /**
   * Get cursor position in SQL editor
   */
  public int getCursor() {
    return cursor;
  }
}
