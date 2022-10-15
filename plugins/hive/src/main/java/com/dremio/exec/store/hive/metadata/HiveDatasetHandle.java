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

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

public final class HiveDatasetHandle implements DatasetHandle {

  private final EntityPath datasetpath;

  private HiveDatasetHandle(final EntityPath datasetpath) {
    this.datasetpath = datasetpath;
  }

  @Override
  public EntityPath getDatasetPath() {
    return datasetpath;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private EntityPath datasetpath;

    public Builder datasetpath(EntityPath datasetpath) {
      this.datasetpath = datasetpath;
      return this;
    }

    public HiveDatasetHandle build() {

      Objects.requireNonNull(datasetpath, "dataset path is required");

      return new HiveDatasetHandle(datasetpath);
    }
  }
}
