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

import java.util.function.Function;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Simple facade around namespace that is responsible for persisting datasets. We do this to ensure
 * that system namespace access doesn't leak into user context except for persistence purposes (we
 * always persist as a system user but we need to retrieve as the query user).
 */
public interface DatasetSaver {
  void save(
    DatasetConfig datasetConfig,
    DatasetHandle handle,
    SourceMetadata sourceMetadata,
    boolean opportunisticSave,
    DatasetRetrievalOptions options,
    Function<DatasetConfig, DatasetConfig> datasetMutator,
    NamespaceAttribute... attributes
  );

  void save(
    DatasetConfig datasetConfig,
    DatasetHandle handle,
    SourceMetadata sourceMetadata,
    boolean opportunisticSave,
    DatasetRetrievalOptions options,
    NamespaceAttribute... attributes
  );

  void save(
    DatasetConfig datasetConfig,
    DatasetHandle handle,
    SourceMetadata sourceMetadata,
    boolean opportunisticSave,
    DatasetRetrievalOptions options,
    String userName,
    NamespaceAttribute... attributes
  );
}
