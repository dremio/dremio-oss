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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.schema.Function;

import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.datastore.Serializer;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.Service;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;

import io.protostuff.ByteString;

/**
 * Registry that's used to register a source with catalog service.
 */
public interface StoragePlugin extends Service, SourceMetadata {
  Serializer<DatasetConfig, byte[]> DATASET_CONFIG_SERIALIZER = Serializer.of(DatasetConfig.getSchema());

  /**
   * Whether the given user can access the entity at the given location
   * according to the underlying source. This will always return true for
   * non-impersonated sources. For impersonated sources this will consult the
   * underlying source. No caching should be done in the plugin.
   * If dataset config doesn't have complete information needed to check access permission
   * always return true.
   *
   * @param user
   *          username to validate.
   * @param key
   *          path to validate.
   * @param datasetConfig
   *          dataset properties
   * @return True if user has access.
   */
  boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig);

  /**
   * Get current state for source.
   * @return
   */
  SourceState getState();

  /**
   * Get the convention for this source instance. This allows the source's rules to choose to match only this source's convention.
   * @return
   */
  SourceCapabilities getSourceCapabilities();

  /**
   * Retrieves table function implementations based on give table schema path and schema config.
   * @param tableSchemaPath the table schema path.
   * @param schemaConfig the schema config.
   * @return a list of table functions implemented for this storage plugin. Returns an empty list by default.
   */
  default List<Function> getFunctions(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return Collections.emptyList();
  }

  /**
   * Create a new DatasetConfig based on a merger of a new schema and the DatasetConfig of this source.
   *
   * @param oldConfig The current DatasetConfiguration.
   * @param newSchema The newly detected BatchSchema.
   * @return The DatasetConfig resulting from the merger of the old config with the new schema.
   */
  default DatasetConfig createDatasetConfigFromSchema(DatasetConfig oldConfig, BatchSchema newSchema) {
    Preconditions.checkNotNull(oldConfig);
    Preconditions.checkNotNull(newSchema);

    final BatchSchema merge;
    if (DatasetHelper.getSchemaBytes(oldConfig) == null) {
      merge = newSchema;
    } else {
      merge = mergeSchemas(oldConfig, newSchema);
    }

    DatasetConfig newConfig = DATASET_CONFIG_SERIALIZER.deserialize(DATASET_CONFIG_SERIALIZER.serialize(oldConfig));
    newConfig.setRecordSchema(ByteString.copyFrom(merge.serialize()));

    return newConfig;
  }

  default BatchSchema mergeSchemas(DatasetConfig oldConfig, BatchSchema newSchema) {
    return CalciteArrowHelper.fromDataset(oldConfig).merge(newSchema);
  }

  @Deprecated // Remove this method as the namespace should keep track of views.
  ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig);

  /**
   * Get the factory class for rules for this source registry. Is designed to
   * ensure that rules don't have reference access to their underlying storage
   * plugin. Rules are created for each plugin instance
   *
   * @return A class that has a zero-arg constructor for generating rules.
   */
  Class<? extends StoragePluginRulesFactory>  getRulesFactoryClass();

  @Override
  void start() throws IOException;
}
