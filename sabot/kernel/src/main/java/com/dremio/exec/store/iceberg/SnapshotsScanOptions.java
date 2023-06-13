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
package com.dremio.exec.store.iceberg;


import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.getTimestampFromMillis;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SnapshotsScanOptions {
  public enum Mode {
    EXPIRED_SNAPSHOTS,  // Expired snapshots
    LIVE_SNAPSHOTS      // Live snapshots after expiration
  }

  private final Mode mode;
  private final Long olderThanInMillis;
  private final Integer retainLast;

  public SnapshotsScanOptions(
    @JsonProperty("mode") Mode mode,
    @JsonProperty("olderThanInMillis") Long olderThanInMillis,
    @JsonProperty("retainLast") Integer retainLast) {
    this.mode = mode;
    this.olderThanInMillis = olderThanInMillis;
    this.retainLast = retainLast;
  }

  public Mode getMode() {
    return mode;
  }

  public Long getOlderThanInMillis() {
    return olderThanInMillis;
  }

  public Integer getRetainLast() {
    return retainLast;
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("[mode=");
    s.append(mode.toString());
    if (olderThanInMillis != null) {
      s.append(", olderThan=" + getTimestampFromMillis(olderThanInMillis));
    }
    if(retainLast != null) {
      s.append(", retainLast=" + retainLast);
    }
    s.append("]");
    return s.toString();
  }
}
