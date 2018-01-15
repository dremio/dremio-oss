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

import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * UI wrapper for {@link AccelerationSettings}
 */
public class AccelerationSettingsUI {
  private final RefreshMethod method;
  private final String refreshField;
  private final Long refreshPeriod;
  private final Long gracePeriod;

  @JsonCreator
  AccelerationSettingsUI(
    @JsonProperty("method") RefreshMethod method,
    @JsonProperty("refreshField") String refreshField,
    @JsonProperty("refreshPeriod") Long refreshPeriod,
    @JsonProperty("gracePeriod") Long gracePeriod) {
    this.method = method;
    this.refreshField = refreshField;
    this.refreshPeriod = refreshPeriod;
    this.gracePeriod = gracePeriod;
  }

  AccelerationSettingsUI(AccelerationSettings settings) {
    this(settings.getMethod(), settings.getRefreshField(), settings.getGracePeriod(), settings.getGracePeriod());
  }

  public RefreshMethod getMethod() {
    return method;
  }

  public String getRefreshField() {
    return refreshField;
  }

  public Long getRefreshPeriod() {
    return refreshPeriod;
  }

  public Long getGracePeriod() {
    return gracePeriod;
  }
}
