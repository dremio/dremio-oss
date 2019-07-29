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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * Interface used to retrieve virtual and physical datasets. This is always contextualized to a single user and
 * default schema. Implementations must be thread-safe
 */
public interface Catalog extends SimpleCatalog<Catalog> {

  /**
   * Retrieve a table ignoring the default schema.
   *
   * @param key
   * @return A DremioTable if found, otherwise null.
   */
  DremioTable getTableNoResolve(NamespaceKey key);

  /**
   * Retrieve a table
   *
   * @param datasetId
   * @return
   */
  DremioTable getTable(String datasetId);

  /**
   * @return all tables that have been requested from this catalog.
   */
  Iterable<DremioTable> getAllRequestedTables();

  /**
   * Resolve an ambiguous reference using the following rules: if the reference is a single value
   * and a default schema is defined, resolve using the default schema. Otherwise, resolve using the
   * name directly.
   *
   * @param key
   * @return
   */
  NamespaceKey resolveSingle(NamespaceKey key);

  /**
   * Determine whether the container at the given path exists. Note that this first looks to see if
   * the container exists directly via a lookup. However, in the case of sources, we have to do two
   * additional checks. First, we have to check if there is a physical dataset in the path (because
   * in FileSystems, sometimes the folders leading to a dataset don't exist). If that returns false,
   * we finally have to consult the source directly to see if the path exists.
   *
   * @param path Container path to check.
   * @return True if the path exists and is readable by the user. False if it doesn't exist.
   */
  boolean containerExists(NamespaceKey path);

  String getUser();

  /**
   * Resolve the provided key to the default schema path, if there is one.
   * @param key
   * @return
   */
  NamespaceKey resolveToDefault(NamespaceKey key);


  /**
   * Return a new Catalog contextualized to the provided username
   *
   * @param username
   * @return
   */
  Catalog resolveCatalog(String username);

  MetadataStatsCollector getMetadataStatsCollector();

  CreateTableEntry createNewTable(final NamespaceKey key, final WriterOptions writerOptions, final Map<String, Object> storageOptions);

  void createView(final NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException;

  void updateView(final NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException;

  void dropView(final NamespaceKey key) throws IOException;

  void dropTable(NamespaceKey key);

  /**
   * Create a new dataset at this location and mutate the dataset before saving.
   * @param key
   * @param datasetMutator
   */
  void createDataset(NamespaceKey key, com.google.common.base.Function<DatasetConfig, DatasetConfig> datasetMutator);

  UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions);

  SourceState refreshSourceStatus(NamespaceKey key) throws Exception;

  Iterable<String> getSubPartitions(NamespaceKey key, List<String> partitionColumns, List<String> partitionValues) throws PartitionNotFoundException;

  /**
   * Create or update a physical dataset along with its read definitions and splits.
   *
   * @param userNamespaceService namespace service for a user who is adding or modifying a dataset.
   * @param source source where dataset is to be created/updated
   * @param datasetPath dataset full path
   * @param datasetConfig minimum configuration needed to define a dataset (format settings)
   * @param attributes optional namespace attributes
   * @return true if dataset is created/updated
   * @throws NamespaceException
   */
  boolean createOrUpdateDataset(NamespaceService userNamespaceService, NamespaceKey source, NamespaceKey datasetPath, DatasetConfig datasetConfig, NamespaceAttribute... attributes) throws NamespaceException;

  /**
   * Update a dataset configuration with a newly detected schema.
   * @param datasetKey the dataset NamespaceKey
   * @param newSchema the detected schema from the executor
   */
  void updateDatasetSchema(NamespaceKey datasetKey, BatchSchema newSchema);

  /**
   * Update a dataset configuration with a newly detected schema.
   * @param datasetKey the dataset NamespaceKey
   * @param originField the original field
   * @param fieldSchema the new schema
   */
  void updateDatasetField(NamespaceKey datasetKey, String originField, CompleteType fieldSchema);

  /**
   * Get a source based on the provided name. If the source doesn't exist, synchronize with the
   * KVStore to confirm creation status.
   *
   * @param name
   * @return A StoragePlugin casted to the expected output.
   */
  <T extends StoragePlugin> T getSource(String name);

  /**
   * Create a source based on the provided configuration. Includes both the creation as well the
   * startup of the source. If the source fails to start, no creation will be done. The provided
   * configuration should have a null version. Failure to create or failure to start with throw an
   * exception. Additionally, if "store.plugin.check_state" is enabled, a plugin that starts but
   * then reveals a bad state, will also result in exception.
   *
   * @param config Configuration for the source.
   * @param attributes Optional namespace attributes to pass to namespace entity creation
   */
  void createSource(SourceConfig config, NamespaceAttribute... attributes);

  /**
   * Update an existing source with the given config. The config version must be the same as the
   * currently active source. If it isn't, this call will fail with an exception.
   *
   * @param config Configuration for the source.
   * @param attributes Optional namespace attributes to pass to namespace entity creation
   */
  void updateSource(SourceConfig config, NamespaceAttribute... attributes);

  /**
   * Delete a source with the provided config. If the source doesn't exist or the config doesn't
   * match, the method with throw an exception.
   *
   * @param config
   */
  void deleteSource(SourceConfig config);


  /**
   * Determines if a SourceConfig changes metadata impacting properties compared to the existing SourceConfig.
   *
   * @param sourceConfig source config
   * @return boolean
   */
  boolean isSourceConfigMetadataImpacting(SourceConfig sourceConfig);

  /**
   * Get the cached source state for a plugin.
   *
   * @param name plugin name whose state to retrieve
   * @return Last refreshed source state. Null if source is not found.
   */
  SourceState getSourceState(String name);

  enum UpdateStatus {
    /**
     * Metadata hasn't changed.
     */
    UNCHANGED,


    /**
     * Metadata has changed.
     */
    CHANGED,

    /**
     * Dataset has been deleted.
     */
    DELETED
  }
}
