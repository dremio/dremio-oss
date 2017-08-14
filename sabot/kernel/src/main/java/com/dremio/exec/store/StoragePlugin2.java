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

import java.util.List;

import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import io.protostuff.ByteString;

/**
 * Registry that's used to register a source with catalog service.
 */
public interface StoragePlugin2 {

  /**
   * Lists datasets under this source. Used to pull in datasets for the first
   * time and by routine namespace check (for now).
   *
   * @return List of shallow datasets. Expected to be a set of lazy-loaded
   *         datasets so namespace can determine when to retrieve and save
   *         properties.
   */
  Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception;

  /**
   * Get dataset for given path and user.
   * @param datasetPath
   * @param oldDataset dataset information (currently used for format settings)
   * @param ignoreAuthErrors
   * @return The Source table definition associated with this key. If doesn't exist, return null.
   */
  SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldDataset, boolean ignoreAuthErrors) throws Exception;

  /**
   * Whether an entity exists at the given path. This should be done using system user permissions.
   * @param key The path to check
   * @return True if an entity (folder/database) exists at this location.
   */
  boolean containerExists(NamespaceKey key);

  /**
   * Whether an entity exists at the given path. This should be done using system user permissions.
   * @param key The path to check
   * @return True if an entity (table) exists at this location.
   */
  boolean datasetExists(NamespaceKey key);

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
  StoragePluginId getId();

  @Deprecated // Remove this method as the namespace should keep track of views.
  ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig);

  /**
   * Get the factory class for rules for this source registry. Is designed to
   * ensure that rules don't have reference access to their underlying storage
   * plugin. Rules are created for each plugin instance
   *
   * @return A class that has a zero-arg constructor for generating rules.
   */
  Class<? extends StoragePluginInstanceRulesFactory> getRulesFactoryClass();

  /**
   * The status of the dataset that has been cheked
   */
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

  /**
   * Describes the result of checking the existing read signature for changes.
   */
  public interface CheckResult {

    /**
     * The type of result for the check.
     */
    UpdateStatus getStatus();

    /**
     * Returns an updated dataset iff the UpdateStatus is CHANGED.
     */
    SourceTableDefinition getDataset();

    public CheckResult UNCHANGED = new CheckResult(){

      @Override
      public UpdateStatus getStatus() {
        return UpdateStatus.UNCHANGED;
      }

      @Override
      public SourceTableDefinition getDataset() {
        throw new UnsupportedOperationException("Dataset is unchanged.");
      }};

    public CheckResult DELETED = new CheckResult(){

      @Override
      public UpdateStatus getStatus() {
        return UpdateStatus.DELETED;
      }

      @Override
      public SourceTableDefinition getDataset() {
        throw new UnsupportedOperationException("Dataset is deleted.");
      }};
  }

  /**
   * Check to see if the read signature for a dataset has changed. If so,
   * refresh the dataset config and return in the result.
   *
   * @param key
   * @param datasetConfig
   * @return
   * @throws Exception
   */
  CheckResult checkReadSignature(ByteString key, DatasetConfig datasetConfig) throws Exception;

}
