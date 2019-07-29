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

public class ElasticScanProperties {

  private final int fetch;
  private final boolean atLeastOnePushdown;
  private final boolean useElasticProject;

  @JsonCreator
  public ElasticScanProperties(
      @JsonProperty("fetch") int fetch,
      @JsonProperty("atLeastOnePushdown") boolean atLeastOnePushdown,
      @JsonProperty("useElasticProject") boolean useElasticProject) {
    super();
    this.fetch = fetch;
    this.atLeastOnePushdown = atLeastOnePushdown;
    this.useElasticProject = useElasticProject;
  }

  public int getFetch() {
    return fetch;
  }

  public boolean isAtLeastOnePushdown() {
    return atLeastOnePushdown;
  }

  public boolean isUseElasticProject() {
    return useElasticProject;
  }

}
