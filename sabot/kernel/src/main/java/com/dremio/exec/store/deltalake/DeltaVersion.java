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

package com.dremio.exec.store.deltalake;

import java.util.Objects;

public final class DeltaVersion {
  private final long version;
  private final int subparts;
  private final boolean isCheckpoint;

  private DeltaVersion(long version, int subparts, boolean isCheckpoint) {
    this.version = version;
    this.subparts = subparts;
    this.isCheckpoint = isCheckpoint;
  }

  public static DeltaVersion of(long version) {
    return new DeltaVersion(version, 1, false);
  }

  public static DeltaVersion ofCheckpoint(long version, int subparts) {
    return new DeltaVersion(version, subparts, true);
  }

  public static DeltaVersion of(long version, int subparts, boolean isCheckpoint) {
    return new DeltaVersion(version, subparts, isCheckpoint);
  }

  public long getVersion() {
    return version;
  }

  public int getSubparts() {
    return subparts;
  }

  public boolean isCheckpoint() {
    return isCheckpoint;
  }

  @Override
  public String toString() {
    return "DeltaVersion{"
        + "version="
        + version
        + ", subparts="
        + subparts
        + ", isCheckpoint="
        + isCheckpoint
        + '}';
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof DeltaVersion)) {
      return false;
    }

    DeltaVersion otherVersion = (DeltaVersion) other;
    return version == otherVersion.version
        && subparts == otherVersion.subparts
        && isCheckpoint == otherVersion.isCheckpoint;
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, subparts, isCheckpoint);
  }
}
