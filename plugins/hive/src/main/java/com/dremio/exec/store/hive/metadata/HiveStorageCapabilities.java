/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

/**
 * Helper class to hold capabilities of physical tables or partitions within the source.
 */
public final class HiveStorageCapabilities {

  public static final HiveStorageCapabilities DEFAULT = newBuilder()
    .supportsImpersonation(true)
    .supportsLastModifiedTime(true)
    .build();

  private final boolean supportsLastModifiedTime;
  private final boolean supportsImpersonation;

  private HiveStorageCapabilities(boolean supportsLastModifiedTime, boolean supportsImpersonation) {
    this.supportsLastModifiedTime = supportsLastModifiedTime;
    this.supportsImpersonation = supportsImpersonation;
  }

  public boolean supportsImpersonation() {
    return supportsImpersonation;
  }

  public boolean supportsLastModifiedTime() {
    return supportsLastModifiedTime;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private boolean supportsLastModifiedTime;
    private boolean supportsImpersonation;

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

    public HiveStorageCapabilities build() {
      return new HiveStorageCapabilities(supportsLastModifiedTime, supportsImpersonation);
    }
  }
}
