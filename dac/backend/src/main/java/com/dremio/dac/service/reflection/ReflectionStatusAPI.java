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
package com.dremio.dac.service.reflection;

import com.dremio.dac.api.JsonISODateTime;
import com.dremio.service.reflection.ReflectionStatus;
import com.dremio.service.reflection.ReflectionStatus.AVAILABILITY_STATUS;
import com.dremio.service.reflection.ReflectionStatus.COMBINED_STATUS;
import com.dremio.service.reflection.ReflectionStatus.CONFIG_STATUS;
import com.dremio.service.reflection.ReflectionStatus.REFRESH_METHOD;
import com.dremio.service.reflection.ReflectionStatus.REFRESH_STATUS;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Reflection Status API based off ReflectionStatusUI except that this class
 * adheres to Dremio REST API guidelines
 */
@JsonAutoDetect(fieldVisibility=JsonAutoDetect.Visibility.ANY)
public class ReflectionStatusAPI {

  private CONFIG_STATUS configStatus;
  private REFRESH_STATUS refreshStatus;
  private AVAILABILITY_STATUS availabilityStatus;
  private COMBINED_STATUS combinedStatus;
  private REFRESH_METHOD refreshMethod;

  private int failureCount;

  @JsonISODateTime
  @JsonInclude(JsonInclude.Include.ALWAYS)
  private Long lastDataFetchAt;
  @JsonISODateTime
  @JsonInclude(JsonInclude.Include.ALWAYS)
  private Long expiresAt;
  private long lastRefreshDurationMillis;

  public ReflectionStatusAPI() {
  }

  public ReflectionStatusAPI(ReflectionStatus status) {
    this.configStatus = status.getConfigStatus();
    this.refreshStatus = status.getRefreshStatus();
    this.availabilityStatus = status.getAvailabilityStatus();
    this.combinedStatus = status.getCombinedStatus();
    this.failureCount = status.getNumFailures();
    this.lastDataFetchAt = status.getLastDataFetch() != -1 ? status.getLastDataFetch() : null;
    this.expiresAt = status.getExpiresAt() != -1 ? status.getExpiresAt() : null;
    this.lastRefreshDurationMillis = status.getLastRefreshDuration();
    this.refreshMethod = status.getRefreshMethod();
  }

  public ReflectionStatusAPI(CONFIG_STATUS config, REFRESH_STATUS refresh, AVAILABILITY_STATUS availability, REFRESH_METHOD refreshMethod,
                             COMBINED_STATUS combinedStatus, int failureCount, long lastDataFetch, long expiresAt, long lastRefreshDuration) {
    this.configStatus = config;
    this.refreshStatus = refresh;
    this.availabilityStatus = availability;
    this.combinedStatus = combinedStatus;
    this.failureCount = failureCount;
    this.lastDataFetchAt = lastDataFetch != -1 ? lastDataFetch : null;
    this.expiresAt = expiresAt != -1 ? expiresAt : null;
    this.lastRefreshDurationMillis = lastRefreshDuration;
    this.refreshMethod = refreshMethod;
  }

  public AVAILABILITY_STATUS getAvailabilityStatus() {
    return availabilityStatus;
  }

  public REFRESH_STATUS getRefreshStatus() {
    return refreshStatus;
  }

  public CONFIG_STATUS getConfigStatus() {
    return configStatus;
  }

  public COMBINED_STATUS getCombinedStatus() {
    return combinedStatus;
  }

  public REFRESH_METHOD getRefreshMethod() {
    return refreshMethod;
  }

  public int getFailureCount() {
    return failureCount;
  }

  public long getLastRefreshDurationMillis() {
    return lastRefreshDurationMillis;
  }
}
