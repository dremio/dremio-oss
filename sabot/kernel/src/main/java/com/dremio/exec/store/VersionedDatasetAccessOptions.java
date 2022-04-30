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
package com.dremio.exec.store;

import java.util.stream.Stream;

import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.MetadataOption;
import com.dremio.exec.catalog.VersionContext;

public class VersionedDatasetAccessOptions implements GetDatasetOption, GetMetadataOption, ListPartitionChunkOption {
  private final String versionedTableKeyPath;
  private final VersionContext versionContext;

  public static final VersionedDatasetAccessOptions DEFAULT_VERSIONED_DATASET_ACCESS_OPTIONS = new Builder()
      .setVersionedTableKeyPath("")
      .setVersionContext(VersionContext.NOT_SPECIFIED)
      .build();

  private VersionedDatasetAccessOptions(Builder builder) {
    this.versionedTableKeyPath = builder.versionedTableKeyPath;
    this.versionContext = builder.versionContext;
  }

  public VersionedDatasetAccessOptions getDefault() {
    return DEFAULT_VERSIONED_DATASET_ACCESS_OPTIONS;
  }

  // TODO: Note, this is unused, revisit
  public String getVersionedTableKey() {
    return versionedTableKeyPath;
  }

  public VersionContext getVersionContext() {
    return versionContext;
  }

  public static class Builder {
    private String versionedTableKeyPath;
    private VersionContext versionContext;

    public Builder() {
    }

    public Builder setVersionedTableKeyPath(String versionedTableKeyPath) {
      this.versionedTableKeyPath = versionedTableKeyPath;
      return this;
    }

    public Builder setVersionContext(VersionContext versionContext) {
      this.versionContext = versionContext;
      return this;
    }

    public VersionedDatasetAccessOptions build() {
      return new VersionedDatasetAccessOptions(this);
    }
  }

  public static VersionedDatasetAccessOptions getVersionedDatasetAccessOptions(MetadataOption... options) {
    return (VersionedDatasetAccessOptions) Stream.of(options).filter(o -> o instanceof VersionedDatasetAccessOptions).findFirst().orElse(null);
  }
}

