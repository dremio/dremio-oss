/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.concurrent.TimeUnit;

import com.dremio.exec.store.StoragePlugin.UpdateStatus;
import com.dremio.service.Service;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;

/**
 * Manages metadata for sources and datasets under these sources.
 */
public interface CatalogService extends AutoCloseable, Service {

  long DEFAULT_REFRESH_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  long DEFAULT_EXPIRE_MILLIS = TimeUnit.MILLISECONDS.convert(3, TimeUnit.HOURS);
  long DEFAULT_AUTHTTLS_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
  long CENTURY_MILLIS = TimeUnit.DAYS.toMillis(365*100);

  MetadataPolicy REFRESH_EVERYTHING_NOW = new MetadataPolicy()
      .setAuthTtlMs(0l)
      .setDatasetUpdateMode(UpdateMode.PREFETCH)
      .setNamesRefreshMs(0l)
      .setDatasetDefinitionRefreshAfterMs(0l)
      .setDatasetDefinitionExpireAfterMs(DEFAULT_EXPIRE_MILLIS);

  MetadataPolicy DEFAULT_METADATA_POLICY = new MetadataPolicy()
      .setAuthTtlMs(DEFAULT_AUTHTTLS_MILLIS)
      .setDatasetUpdateMode(UpdateMode.PREFETCH_QUERIED)
      .setNamesRefreshMs(DEFAULT_REFRESH_MILLIS)
      .setDatasetDefinitionRefreshAfterMs(DEFAULT_REFRESH_MILLIS)
      .setDatasetDefinitionExpireAfterMs(DEFAULT_EXPIRE_MILLIS);

  MetadataPolicy NEVER_REFRESH_POLICY = new MetadataPolicy()
    .setAuthTtlMs(CENTURY_MILLIS)
    .setDatasetUpdateMode(UpdateMode.PREFETCH)
    .setNamesRefreshMs(CENTURY_MILLIS)
    .setDatasetDefinitionRefreshAfterMs(CENTURY_MILLIS)
    .setDatasetDefinitionExpireAfterMs(CENTURY_MILLIS);

  /**
   * Register a source to catalog service
   * @param source source name
   * @param sourceRegistry source registry for given source
   */
  void registerSource(NamespaceKey source, StoragePlugin sourceRegistry);

  /**
   * Unregister a source from a catalog service
   * @param source source name
   */
  void unregisterSource(NamespaceKey source);

  /**
   * Schedule regular updates for a source's metadata (names and dataset schemas)
   * @param source source name
   * @param sourceConfig source configuration
   */
  void scheduleMetadataRefresh(NamespaceKey source, SourceConfig sourceConfig);

  /**
   * Retrieve the last time when the metadata of 'source' was fully refreshed
   * @param source source name
   * @return milliseconds since start of epoch when 'source' was last fully refreshed. 0 if never refreshed
   */
  long getLastFullMetadataRefreshDateMs(NamespaceKey source);

  /**
   * Refresh metadata cached for given source.
   * @param source source name
   * @param metadataPolicy Metadata update policy for 'source'
   * @return true if cached metadata has changed
   * @throws NamespaceException
   */
  boolean refreshSource(NamespaceKey source, MetadataPolicy metadataPolicy) throws NamespaceException;

  /**
   * Refresh dataset names for a given source. Only adds new names. Only the full metadata refresh {@Link refreshSource}
   * can remove names and associated definitions
   * @param source source name
   * @param metadataPolicy Metadata update policy for 'source' -- only used for V1 storage plugins. To be removed once V1 support has sunset
   * @throws NamespaceException
   */
  void refreshSourceNames(NamespaceKey source, MetadataPolicy metadataPolicy) throws NamespaceException;

  /**
   * Create a new dataset at this location and mutate the dataset before saving.
   * @param key
   * @param datasetMutator
   */
  void createDataset(NamespaceKey key, Function<DatasetConfig, DatasetConfig> datasetMutator);

  UpdateStatus refreshDataset(NamespaceKey key) throws NamespaceException;

  /**
   * get storage plugin2 for a given source
   * @param name source name
   * @return new storage plugin, null if source does not support new storage plugin.
   */
  StoragePlugin getStoragePlugin(String name);

  /**
   * Create or update a physical dataset along with its read definitions and splits.
   * @param userNamespaceService namespace service for a user who is adding or modifying a dataset.
   * @param source source where dataset is to be created/updated
   * @param datasetPath dataset full path
   * @param datasetConfig minimum configuration needed to define a dataset (format settings)
   * @return true if dataset is created/updated
   * @throws NamespaceException
   */
  boolean createOrUpdateDataset(NamespaceService userNamespaceService, NamespaceKey source, NamespaceKey datasetPath, DatasetConfig datasetConfig) throws NamespaceException;

  @Deprecated
  StoragePluginRegistry getOldRegistry();
}
