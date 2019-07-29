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
package com.dremio.exec.store.hive.metadata;

import java.util.Objects;

/**
 * Helper class to hold capabilities of physical tables or partitions within the source.
 */
public final class HiveStorageCapabilities {

  public static final HiveStorageCapabilities DEFAULT_HDFS = newBuilder()
    .supportsImpersonation(true)
    .supportsLastModifiedTime(true)
    .supportsOrcSplitFileIds(true)
    .build();

  private final boolean supportsLastModifiedTime;
  private final boolean supportsImpersonation;
  private final boolean supportsOrcSplitFileIds;

  private HiveStorageCapabilities(boolean supportsLastModifiedTime, boolean supportsImpersonation, boolean supportsOrcSplitFileIds) {
    this.supportsLastModifiedTime = supportsLastModifiedTime;
    this.supportsImpersonation = supportsImpersonation;
    this.supportsOrcSplitFileIds = supportsOrcSplitFileIds;
  }

  public boolean supportsImpersonation() {
    return supportsImpersonation;
  }

  public boolean supportsLastModifiedTime() {
    return supportsLastModifiedTime;
  }

  public boolean supportsOrcSplitFileIds() {
    return supportsOrcSplitFileIds;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private Boolean supportsLastModifiedTime;
    private Boolean supportsImpersonation;
    private Boolean supportsOrcSplitFileIds;

    private Builder() {
    }

    public Builder supportsLastModifiedTime(boolean supportsLastModifiedTime) {
      this.supportsLastModifiedTime = supportsLastModifiedTime;
      return this;
    }

    public Builder supportsImpersonation(boolean supportsImpersonation) {
      this.supportsImpersonation = supportsImpersonation;
      return this;
    }

    public Builder supportsOrcSplitFileIds(boolean supportsOrcSplitFileIds) {
      this.supportsOrcSplitFileIds = supportsOrcSplitFileIds;
      return this;
    }

    public HiveStorageCapabilities build() {

      Objects.requireNonNull(supportsLastModifiedTime, "supportsLastModifiedTime is required");
      Objects.requireNonNull(supportsImpersonation, "supportsImpersonation is required");
      Objects.requireNonNull(supportsOrcSplitFileIds, "supportsOrcSplitFileIds is required");

      return new HiveStorageCapabilities(supportsLastModifiedTime, supportsImpersonation, supportsOrcSplitFileIds);
    }
  }
}
