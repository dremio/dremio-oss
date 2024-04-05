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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.MetadataOption;
import java.util.stream.Stream;

public class VersionedDatasetAccessOptions
    implements GetDatasetOption, GetMetadataOption, ListPartitionChunkOption {
  private final ResolvedVersionContext versionContext;

  private VersionedDatasetAccessOptions(Builder builder) {
    this.versionContext = builder.versionContext;
  }

  public ResolvedVersionContext getVersionContext() {
    return versionContext;
  }

  public static class Builder {
    private ResolvedVersionContext versionContext;

    public Builder() {}

    public Builder setVersionContext(ResolvedVersionContext versionContext) {
      this.versionContext = versionContext;
      return this;
    }

    public VersionedDatasetAccessOptions build() {
      return new VersionedDatasetAccessOptions(this);
    }
  }

  public static VersionedDatasetAccessOptions getVersionedDatasetAccessOptions(
      MetadataOption... options) {
    return (VersionedDatasetAccessOptions)
        Stream.of(options)
            .filter(o -> o instanceof VersionedDatasetAccessOptions)
            .findFirst()
            .orElse(null);
  }
}
