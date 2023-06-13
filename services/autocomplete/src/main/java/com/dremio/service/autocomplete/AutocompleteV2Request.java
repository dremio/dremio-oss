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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Autocomplete v2 API Request
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AutocompleteV2Request {
  private final String prefix;
  private final AutocompleteV2RequestType type;
  private final List<List<String>> catalogEntityKeys;
  private final List<String> queryContext;
  private final String refType;
  private final String refValue;

  @JsonCreator
  public AutocompleteV2Request(
    @JsonProperty("prefix") String prefix,
    @JsonProperty("type") AutocompleteV2RequestType type,
    @JsonProperty("catalogEntityKeys") List<List<String>> catalogEntityKeys,
    @JsonProperty("queryContext") List<String> queryContext,
    @JsonProperty("refType") String refType,
    @JsonProperty("refValue") String refValue) {
    this.prefix = prefix;
    this.type = type;
    this.catalogEntityKeys = catalogEntityKeys;
    this.queryContext = queryContext;
    this.refType = refType;
    this.refValue = refValue;
  }

  /**
   * Get prefix
   */
  public String getPrefix() {
    return this.prefix;
  }

  /**
   * Get type
   */
  public AutocompleteV2RequestType getType() {
    return this.type;
  }

  /**
   * Get namespace keys
   */
  public List<List<String>> getCatalogEntityKeys() {
    return this.catalogEntityKeys;
  }

  /**
   * Get query context
   */
  public List<String> getQueryContext() {
    return this.queryContext;
  }

  /**
   * Get ref type
   */
  public String getRefType() {
    return this.refType;
  }

  /**
   * Get ref value
   */
  public String getRefValue() {
    return this.refValue;
  }
}
