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
package com.dremio.service.namespace;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import java.util.List;

/*
 * A class of a DatasetConfig of a dateset and Entities on the dataset path.
 * Used as return from {@Link NamespaceService#getDatasetAndEntitiesOnPath()} to save later retrieve of KV store for the
 * same dataset.
 */

public class DatasetConfigAndEntitiesOnPath {
  private final DatasetConfig datasetConfig;
  private final List<NameSpaceContainer> entitiesOnPath;

  public DatasetConfigAndEntitiesOnPath(
      DatasetConfig datasetConfig, List<NameSpaceContainer> entitiesOnPath) {
    this.datasetConfig = datasetConfig;
    this.entitiesOnPath = entitiesOnPath;
  }

  public DatasetConfig getDatasetConfig() {
    return datasetConfig;
  }

  public List<NameSpaceContainer> getEntitiesOnPath() {
    return entitiesOnPath;
  }

  public static DatasetConfigAndEntitiesOnPath of(
      DatasetConfig datasetConfig, List<NameSpaceContainer> entitiesOnPath) {
    return new DatasetConfigAndEntitiesOnPath(datasetConfig, entitiesOnPath);
  }
}
