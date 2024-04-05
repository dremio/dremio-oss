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
import com.dremio.service.reflection.ReflectionStatus.REFRESH_STATUS;
import com.fasterxml.jackson.annotation.JsonAutoDetect;

/** Reflection Status */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class ReflectionStatusUI {

  private CONFIG_STATUS config;
  private REFRESH_STATUS refresh;
  private AVAILABILITY_STATUS availability;
  private COMBINED_STATUS combinedStatus;

  private int failureCount;
  private String lastFailureMessage;
  @JsonISODateTime private long lastDataFetch;
  @JsonISODateTime private long expiresAt;

  public ReflectionStatusUI() {}

  public ReflectionStatusUI(ReflectionStatus status) {
    this.config = status.getConfigStatus();
    this.refresh = status.getRefreshStatus();
    this.availability = status.getAvailabilityStatus();
    this.combinedStatus = status.getCombinedStatus();
    this.failureCount = status.getNumFailures();
    this.lastFailureMessage =
        status.getLastFailure() != null ? status.getLastFailure().getMessage() : null;
    this.lastDataFetch = status.getLastDataFetch();
    this.expiresAt = status.getExpiresAt();
  }

  public ReflectionStatusUI(
      CONFIG_STATUS config,
      REFRESH_STATUS refresh,
      AVAILABILITY_STATUS availability,
      COMBINED_STATUS combinedStatus,
      int failureCount,
      String lastFailureMessage,
      long lastDataFetch,
      long expiresAt) {
    this.config = config;
    this.refresh = refresh;
    this.availability = availability;
    this.combinedStatus = combinedStatus;
    this.failureCount = failureCount;
    this.lastFailureMessage = lastFailureMessage;
    this.lastDataFetch = lastDataFetch;
    this.expiresAt = expiresAt;
  }

  public AVAILABILITY_STATUS getAvailability() {
    return availability;
  }

  public REFRESH_STATUS getRefresh() {
    return refresh;
  }

  public CONFIG_STATUS getConfig() {
    return config;
  }

  public COMBINED_STATUS getCombinedStatus() {
    return combinedStatus;
  }

  public int getFailureCount() {
    return failureCount;
  }

  public String getLastFailureMessage() {
    return lastFailureMessage;
  }
}
