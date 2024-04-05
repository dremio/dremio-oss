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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.file.proto.IcebergFileConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;

public class VersionedDatasetAdapterFactory {

  @Inject
  public VersionedDatasetAdapterFactory() {}

  public VersionedDatasetAdapter newInstance(
      List<String> versionedTableKey,
      ResolvedVersionContext versionContext,
      StoragePlugin storagePlugin,
      StoragePluginId storagePluginId,
      OptionManager optionManager) {
    Preconditions.checkNotNull(versionedTableKey);
    Preconditions.checkNotNull(versionContext);
    Preconditions.checkNotNull(storagePlugin);
    Preconditions.checkNotNull(storagePluginId);

    DatasetConfig versionedDatasetConfig = createShallowIcebergDatasetConfig(versionedTableKey);

    return tryGetDatasetHandle(storagePlugin, versionedDatasetConfig, versionContext)
        .map(
            datasetHandle ->
                newInstance(
                    versionedTableKey,
                    versionContext,
                    storagePlugin,
                    storagePluginId,
                    optionManager,
                    datasetHandle,
                    versionedDatasetConfig))
        .orElse(null);
  }

  protected VersionedDatasetAdapter newInstance(
      List<String> versionedTableKey,
      ResolvedVersionContext versionContext,
      StoragePlugin storagePlugin,
      StoragePluginId storagePluginId,
      OptionManager optionManager,
      DatasetHandle datasetHandle,
      DatasetConfig versionedDatasetConfig) {
    return new VersionedDatasetAdapter(
        versionedTableKey,
        versionContext,
        storagePlugin,
        storagePluginId,
        optionManager,
        datasetHandle,
        versionedDatasetConfig);
  }

  /**
   * This is a helper method that creates a shell datasetConfig that populates the format so the
   * FileSystem plugin knows what format we will be using to access the Iceberg tables.
   *
   * @param versionedTableKey Namespace key
   * @return DatasetConfig populated with the basic info needed by the FileSsystem plugin to match
   *     and unwrap to Iceberg format plugin
   */
  private DatasetConfig createShallowIcebergDatasetConfig(List<String> versionedTableKey) {
    return new DatasetConfig()
        .setId(new EntityId().setId(UUID.randomUUID().toString()))
        .setName(String.join(".", versionedTableKey))
        .setFullPathList(versionedTableKey)
        // This format setting allows us to pick the Iceberg format explicitly
        .setPhysicalDataset(
            new PhysicalDataset().setFormatSettings(new IcebergFileConfig().asFileConfig()))
        .setLastModified(System.currentTimeMillis());
  }

  /** Helper method that gets a handle to the FileSystem(Iceberg format) storage plugin */
  private Optional<DatasetHandle> tryGetDatasetHandle(
      StoragePlugin storagePlugin,
      DatasetConfig versionedDatasetConfig,
      ResolvedVersionContext versionContext) {
    final EntityPath entityPath = new EntityPath(versionedDatasetConfig.getFullPathList());

    try {
      return storagePlugin.getDatasetHandle(
          entityPath,
          DatasetRetrievalOptions.DEFAULT.toBuilder()
              .setVersionedDatasetAccessOptions(
                  new VersionedDatasetAccessOptions.Builder()
                      .setVersionContext(versionContext)
                      .build())
              .build()
              .asGetDatasetOptions(versionedDatasetConfig));
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
          .message("Failure while retrieving dataset [%s].", entityPath)
          .buildSilently();
    }
  }
}
