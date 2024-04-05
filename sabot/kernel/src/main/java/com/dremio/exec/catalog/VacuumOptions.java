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
package com.dremio.exec.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VacuumOptions {
  public static final String GC_RETENTION_MILLIS_PROP_NAME = "GC_RETENTION_MILLIS";
  public static final String GC_LAST_RUN_TIMESTAMP_MILLIS_PROP_NAME =
      "GC_LAST_RUN_TIMESTAMP_MILLIS";
  public static final String GC_LAST_RUN_TIMESTAMP_DEFAULT = "0";
  private final Long olderThanInMillis;
  private final Integer retainLast;
  private final boolean expireSnapshots;
  private final boolean removeOrphans;
  private final String location;
  private final Long gracePeriodInMillis;

  @JsonCreator
  public VacuumOptions(
      @JsonProperty("expireSnapshots") boolean expireSnapshots,
      @JsonProperty("removeOrphans") boolean removeOrphans,
      @JsonProperty("olderThanInMillis") Long olderThanInMillis,
      @JsonProperty("retainLast") Integer retainLast,
      @JsonProperty("location") String location,
      @JsonProperty("gracePeriodInMillis") Long gracePeriodInMillis) {
    this.expireSnapshots = expireSnapshots;
    this.removeOrphans = removeOrphans;
    this.olderThanInMillis = olderThanInMillis;
    this.retainLast = retainLast;
    this.location = location;
    this.gracePeriodInMillis = gracePeriodInMillis;
  }

  public VacuumOptions(NessieGCPolicy nessieGCPolicy) {

    this.expireSnapshots = true;
    this.removeOrphans = true;
    this.location = null;
    this.olderThanInMillis = nessieGCPolicy.getOlderThanInMillis();
    this.retainLast = nessieGCPolicy.getRetainLast();
    this.gracePeriodInMillis = nessieGCPolicy.getGracePeriodInMillis();
  }

  public Long getOlderThanInMillis() {
    return olderThanInMillis;
  }

  public Integer getRetainLast() {
    return retainLast;
  }

  public boolean isExpireSnapshots() {
    return expireSnapshots;
  }

  public boolean isRemoveOrphans() {
    return removeOrphans;
  }

  public String getLocation() {
    return location;
  }

  public Long getGracePeriodInMillis() {
    return gracePeriodInMillis;
  }

  @Override
  public String toString() {
    return "VacuumOptions{"
        + "olderThanInMillis="
        + olderThanInMillis
        + ", retainLast="
        + retainLast
        + ", expireSnapshots="
        + expireSnapshots
        + ", removeOrphans="
        + removeOrphans
        + ", location="
        + location
        + ", gracePeriodInMillis="
        + gracePeriodInMillis
        + '}';
  }
}
