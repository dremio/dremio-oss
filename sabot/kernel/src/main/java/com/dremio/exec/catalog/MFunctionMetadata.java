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

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import java.util.Optional;

/** A metadata provider for macro such as table_history, table_snapshot, table_manifests */
public interface MFunctionMetadata {

  /**
   * @return Config of current table
   */
  DatasetConfig getCurrentConfig();

  Optional<DatasetHandle> getHandle();

  StoragePlugin getPlugin();

  StoragePluginId getPluginId();

  SchemaConfig getSchemaConfig();

  /**
   * @return Dataset options with time_travel context as well as versioned context
   */
  DatasetRetrievalOptions getOptions();

  /**
   * @return iceberg metadata location to read metadata details at execution
   */
  String getMetadataLocation();
}
