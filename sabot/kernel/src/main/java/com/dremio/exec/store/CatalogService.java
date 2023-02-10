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

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.calcite.tools.RuleSet;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.proto.CatalogRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.Service;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.google.common.annotations.VisibleForTesting;

/**
 * Manages metadata for sources and datasets under these sources.
 */
@ThreadSafe
public interface CatalogService extends AutoCloseable, Service, StoragePluginResolver {
  long DEFAULT_REFRESH_MILLIS = TimeUnit.MILLISECONDS.convert(
    Integer.getInteger("dremio.metadata.default_refresh_time_in_hours", 1), TimeUnit.HOURS);
  long DEFAULT_EXPIRE_MILLIS = TimeUnit.MILLISECONDS.convert(
    Integer.getInteger("dremio.metadata.default_expire_time_in_hours", 3), TimeUnit.HOURS);
  long DEFAULT_AUTHTTLS_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
  long CENTURY_MILLIS = TimeUnit.DAYS.toMillis(365 * 100);

  @VisibleForTesting
  MetadataPolicy REFRESH_EVERYTHING_NOW = new MetadataPolicy()
    .setAuthTtlMs(0L)
    .setDeleteUnavailableDatasets(true)
    .setAutoPromoteDatasets(DatasetRetrievalOptions.DEFAULT_AUTO_PROMOTE)
    .setDatasetUpdateMode(UpdateMode.PREFETCH)
    .setNamesRefreshMs(0L)
    .setDatasetDefinitionRefreshAfterMs(0L)
    .setDatasetDefinitionExpireAfterMs(DEFAULT_EXPIRE_MILLIS);

  MetadataPolicy DEFAULT_METADATA_POLICY = new MetadataPolicy()
    .setAuthTtlMs(DEFAULT_AUTHTTLS_MILLIS)
    .setDeleteUnavailableDatasets(true)
    .setAutoPromoteDatasets(DatasetRetrievalOptions.DEFAULT_AUTO_PROMOTE)
    .setDatasetUpdateMode(UpdateMode.PREFETCH_QUERIED)
    .setNamesRefreshMs(DEFAULT_REFRESH_MILLIS)
    .setDatasetDefinitionRefreshAfterMs(DEFAULT_REFRESH_MILLIS)
    .setDatasetDefinitionExpireAfterMs(DEFAULT_EXPIRE_MILLIS);

  @VisibleForTesting
  MetadataPolicy DEFAULT_METADATA_POLICY_WITH_AUTO_PROMOTE = new MetadataPolicy()
      .setAuthTtlMs(DEFAULT_AUTHTTLS_MILLIS)
      .setDeleteUnavailableDatasets(true)
      .setAutoPromoteDatasets(true)
      .setDatasetUpdateMode(UpdateMode.PREFETCH_QUERIED)
      .setNamesRefreshMs(DEFAULT_REFRESH_MILLIS)
      .setDatasetDefinitionRefreshAfterMs(DEFAULT_REFRESH_MILLIS)
      .setDatasetDefinitionExpireAfterMs(DEFAULT_EXPIRE_MILLIS);

  MetadataPolicy NEVER_REFRESH_POLICY = new MetadataPolicy()
    .setAuthTtlMs(CENTURY_MILLIS)
    .setDeleteUnavailableDatasets(true)
    .setAutoPromoteDatasets(DatasetRetrievalOptions.DEFAULT_AUTO_PROMOTE)
    .setDatasetUpdateMode(UpdateMode.PREFETCH)
    .setNamesRefreshMs(CENTURY_MILLIS)
    .setDatasetDefinitionRefreshAfterMs(CENTURY_MILLIS)
    .setDatasetDefinitionExpireAfterMs(CENTURY_MILLIS);

  MetadataPolicy NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE = new MetadataPolicy()
      .setAuthTtlMs(CENTURY_MILLIS)
      .setDeleteUnavailableDatasets(true)
      .setAutoPromoteDatasets(true)
      .setDatasetUpdateMode(UpdateMode.PREFETCH)
      .setNamesRefreshMs(CENTURY_MILLIS)
      .setDatasetDefinitionRefreshAfterMs(CENTURY_MILLIS)
      .setDatasetDefinitionExpireAfterMs(CENTURY_MILLIS);

  MetadataPolicy NEVER_REFRESH_POLICY_WITH_PREFETCH_QUERIED = new MetadataPolicy()
    .setAuthTtlMs(CENTURY_MILLIS)
    .setDeleteUnavailableDatasets(true)
    .setAutoPromoteDatasets(DatasetRetrievalOptions.DEFAULT_AUTO_PROMOTE)
    .setDatasetUpdateMode(UpdateMode.PREFETCH_QUERIED)
    .setNamesRefreshMs(CENTURY_MILLIS)
    .setDatasetDefinitionRefreshAfterMs(CENTURY_MILLIS)
    .setDatasetDefinitionExpireAfterMs(CENTURY_MILLIS);

  /**
   * Create the provided source if a source by the provided name doesn't already exist.
   *
   * @param config Source configuration.
   * @return True if the source is created, else false.
   * @throws ConcurrentModificationException
   */
  boolean createSourceIfMissingWithThrow(SourceConfig config) throws ConcurrentModificationException;


  /**
   * Get the cached source state for a plugin.
   *
   * @param name plugin name whose state to retrieve
   * @return Last refreshed source state. Null if source is not found.
   */
  SourceState getSourceState(String name);

  /**
   * Get a source based on the provided name. If the source doesn't exist, synchronize with the
   * KVStore to confirm creation status.
   *
   * @param name
   * @return A StoragePlugin casted to the expected output.
   */
  <T extends StoragePlugin> T getSource(String name);

  /**
   * Collect all rules for StoragePlugins.
   * <p>
   * Collects the following:
   * - One set of rules for each storage plugin type.
   * - One set of rules for each storage plugin instance.
   *
   * @param context
   * @param phase
   * @return
   */
  RuleSet getStorageRules(OptimizerRulesContext context, PlannerPhase phase);

  /**
   * Get a new {@link Catalog} contextualized to the {@link SchemaConfig} provided via the given
   * {@link MetadataRequestOptions metadata request options}, and constrained by the other request options.
   * <p>
   * {@link Catalog Catalogs} are used to interact with datasets within the context of a particular session.
   *
   * @param requestOptions metadata request options
   * @return catalog with the given constraints
   */
  Catalog getCatalog(MetadataRequestOptions requestOptions);

  /**
   * Determines if a SourceConfig changes metadata impacting properties compared to the existing SourceConfig.
   *
   * @param sourceConfig source config
   * @return boolean
   */
  boolean isSourceConfigMetadataImpacting(SourceConfig sourceConfig);

  ManagedStoragePlugin getManagedSource(String name);

  /**
   * Communicate change to executors
   *
   * @param nodeEndpointList
   * @param config
   * @param rpcType
   * @return
   */
  void communicateChangeToExecutors(List<CoordinationProtos.NodeEndpoint> nodeEndpointList, SourceConfig config, CatalogRPC.RpcType rpcType);

  Stream<VersionedPlugin> getAllVersionedPlugins();

  }
