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

import java.util.Objects;

import org.apache.iceberg.Snapshot;

public class SnapshotEntry {
  private final long snapshotId;
  private final String manifestListPath;
  private final String metadataJsonPath;

  public SnapshotEntry(String metadataJsonPath, Snapshot snapshot) {
    this(metadataJsonPath, snapshot.snapshotId(), snapshot.manifestListLocation());
  }

  public SnapshotEntry(String metadataJsonPath, long snapshotId, String manifestListPath) {
    this.metadataJsonPath = metadataJsonPath;
    this.snapshotId = snapshotId;
    this.manifestListPath = manifestListPath;
  }

  public long getSnapshotId() {
    return snapshotId;
  }

  public String getManifestListPath() {
    return manifestListPath;
  }

  public String getMetadataJsonPath() {
    return metadataJsonPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnapshotEntry that = (SnapshotEntry) o;
    return snapshotId == that.snapshotId && Objects.equals(manifestListPath,
        that.manifestListPath) && Objects.equals(metadataJsonPath, that.metadataJsonPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotId, manifestListPath, metadataJsonPath);
  }

  @Override
  public String toString() {
    return "SnapshotEntry{" +
        "snapshotId=" + snapshotId +
        ", manifestListPath='" + manifestListPath + '\'' +
        ", metadataJsonPath='" + metadataJsonPath + '\'' +
        '}';
  }
}
