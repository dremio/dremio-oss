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
package com.dremio.plugins.elastic.planning;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/** Scan specification for reading from Elastic. */
public class ElasticsearchScanSpec {

  private final String query;
  private final int fetch;
  private final String resource;
  private final boolean pushdown;

  @JsonCreator
  public ElasticsearchScanSpec(
      @JsonProperty("resource") String resource,
      @JsonProperty("query") String query,
      @JsonProperty("fetch") int fetch,
      @JsonProperty("pushdown") boolean pushdown) {
    this.resource = resource;
    this.query = query;
    this.fetch = fetch;
    this.pushdown = pushdown;
  }

  // This is only for testing purposes. Execution doesn't need this information.
  public boolean isPushdown() {
    return pushdown;
  }

  public String getResource() {
    return resource;
  }

  public String getQuery() {
    return query;
  }

  public int getFetch() {
    return fetch;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof ElasticsearchScanSpec)) {
      return false;
    }
    ElasticsearchScanSpec castOther = (ElasticsearchScanSpec) other;
    return Objects.equal(query, castOther.query)
        && Objects.equal(fetch, castOther.fetch)
        && Objects.equal(resource, castOther.resource);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(query, fetch, resource);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("query", query)
        .add("fetch", fetch)
        .add("resource", resource)
        .toString();
  }
}
