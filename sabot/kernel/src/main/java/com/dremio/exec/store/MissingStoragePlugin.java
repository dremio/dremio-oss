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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * A storage plugin implementation which is always marked as failed.
 * Plugin configurations for storage plugin types that were in previous versions of
 * Dremio but have now been removed get mapped to this StoragePlugin implementation.
 */
public class MissingStoragePlugin implements StoragePlugin, SupportsListingDatasets {
  private final String errorMessage;
  private boolean throwOnInvocation;

  public MissingStoragePlugin(String reason, boolean throwOnInvocation) {
    errorMessage = reason;
    this.throwOnInvocation = throwOnInvocation;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    if (throwOnInvocation) {
      throw new UnsupportedOperationException(errorMessage);
    }

    return false;
  }

  @Override
  public SourceState getState() {
    if (throwOnInvocation) {
      return SourceState.badState(errorMessage);
    }

    return SourceState.GOOD;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return new SourceCapabilities();
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    if (throwOnInvocation) {
      throw new UnsupportedOperationException(errorMessage);
    }

    return null;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return null;
  }

  @Override
  public void start() throws IOException {

  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
    if (throwOnInvocation) {
      throw new UnsupportedOperationException(errorMessage);
    }

    return () -> Collections.emptyIterator();
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
    if (throwOnInvocation) {
      throw new UnsupportedOperationException(errorMessage);
    }

    return Optional.empty();
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options
  ) {
    throw new UnsupportedOperationException(errorMessage);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
    throw new UnsupportedOperationException(errorMessage);
  }

  @Override
  public boolean containerExists(EntityPath containerPath) {
    if (throwOnInvocation) {
      throw new UnsupportedOperationException(errorMessage);
    }

    return false;
  }
}
