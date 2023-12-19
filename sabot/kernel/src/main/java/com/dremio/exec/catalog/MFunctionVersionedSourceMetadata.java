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

import static com.dremio.exec.catalog.CatalogUtil.getTimeTravelRequest;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.extensions.SupportsIcebergMetadata;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;

/**
 * Table functions metadata for plugin like data plane.
 */
public class MFunctionVersionedSourceMetadata extends MFunctionMetadataImpl {

  private final VersionedDatasetAccessOptions versionedDatasetAccessOptions;

  public MFunctionVersionedSourceMetadata(NamespaceKey canonicalKey, ManagedStoragePlugin plugin, DatasetConfig datasetConfig,
                                          SchemaConfig schemaConfig, VersionedDatasetAccessOptions versionedDatasetAccessOptions, TableVersionContext context) {
    super(canonicalKey, datasetConfig, plugin, schemaConfig, context);
    this.versionedDatasetAccessOptions = versionedDatasetAccessOptions;
  }

  @Override
  public DatasetRetrievalOptions getOptions() {
    return plugin.getDefaultRetrievalOptions()
      .toBuilder()
      .setTimeTravelRequest(getTimeTravelRequest(canonicalKey, context))
      .setVersionedDatasetAccessOptions(versionedDatasetAccessOptions)
      .build();
  }

  @Override
  public String getMetadataLocation() {
    Preconditions.checkArgument(getHandle().isPresent());
    try {
      return  ((SupportsIcebergMetadata) ((FileDatasetHandle) getHandle().get()).getDatasetMetadata(getOptions().asGetMetadataOptions(currentConfig))).getMetadataFileLocation();
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
        .buildSilently();
    }
  }
}
