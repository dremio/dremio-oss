/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.model.job.acceleration;

import com.dremio.service.accelerator.proto.MaterializationDetails;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * UI wrapper for {@link MaterializationDetails}
 */
public class MaterializationDetailsUI {
  private final String id;
  private final Long refreshChainStartTime;

  @JsonCreator
  MaterializationDetailsUI(
    @JsonProperty("id") String id,
    @JsonProperty("refreshChainStartTime") Long refreshChainStartTime) {
    this.id = id;
    this.refreshChainStartTime = refreshChainStartTime;
  }

  MaterializationDetailsUI(MaterializationDetails details) {
    this(details.getId(), details.getRefreshChainStartTime());
  }

  public String getId() {
    return id;
  }

  public Long getRefreshChainStartTime() {
    return refreshChainStartTime;
  }
}
