/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.calcite.tools.RuleSet;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.service.Service;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;

/**
 * Manages metadata for sources and datasets under these sources.
 */
@ThreadSafe
public interface CatalogService extends AutoCloseable, Service {

  long DEFAULT_REFRESH_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  long DEFAULT_EXPIRE_MILLIS = TimeUnit.MILLISECONDS.convert(3, TimeUnit.HOURS);
  long DEFAULT_AUTHTTLS_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
  long CENTURY_MILLIS = TimeUnit.DAYS.toMillis(365 * 100);

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
   * Create the provided source if a source by the provided name doesn't already exist.
   *
   * @param config Source configuration.
   * @return True if the source is created, else false.
   * @throws ConcurrentModificationException
   */
  boolean createSourceIfMissingWithThrow(SourceConfig config) throws ConcurrentModificationException;

  /**
   * Get a StoragePlugin according to the provided StoragePluginId. StoragePluginId is used as the
   * key when moving from planning to execution and will generally be exposed by Tables returned by
   * Catalog. Typically, a CatalogService consumer should never create a StoragePluginId.
   * <p>
   * This method will update the storage plugin if the config is newer than the one held locally.
   * This method will not check the kvstore for additional updates, simply trusting the provided
   * PluginId as a canonical source of truth. If the provided StoragePluginId has an older version
   * of configuration than the one currently active on this node, an exception will be thrown.
   *
   * @param pluginId
   * @return A StoragePlugin casted to the expected output.
   */
  <T extends StoragePlugin> T getSource(StoragePluginId pluginId);


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

  enum UpdateType {
    NAMES, FULL, NONE
  }

  /**
   * Refresh the metadata cached for a given source. If no metadata policy is provided, the
   * currently active source one is used.
   *
   * @param source         The name of the source to update.
   * @param metadataPolicy The metadata policy to use when updating. A null value tells the update to use the
   *                       currently active metadata policy associated with that source.
   * @param updateType     The type of update perform.
   * @return Whether or not any changes to metadata occurred as part of the update.
   * @throws NamespaceException
   */
  boolean refreshSource(NamespaceKey source, MetadataPolicy metadataPolicy, UpdateType updateType) throws NamespaceException;

  /**
   * Get a Catalog contextualized to the provided SchemaConfig. Catalogs are used to interact with
   * datasets within the context a particular session.
   *
   * @param schemaConfig schema config
   * @return catalog
   */
  Catalog getCatalog(SchemaConfig schemaConfig);

  /**
   * Get a new {@link Catalog catalog} that only considers metadata valid if it is newer than the
   * provided maxRequestTime.
   *
   * @param schemaConfig   schema config
   * @param maxRequestTime max request time
   * @return catalog with given constraints
   */
  Catalog getCatalog(SchemaConfig schemaConfig, long maxRequestTime);

  /**
   * Determines if a SourceConfig changes metadata impacting properties compared to the existing SourceConfig.
   *
   * @param sourceConfig source config
   * @return boolean
   */
  boolean isSourceConfigMetadataImpacting(SourceConfig sourceConfig);
}
