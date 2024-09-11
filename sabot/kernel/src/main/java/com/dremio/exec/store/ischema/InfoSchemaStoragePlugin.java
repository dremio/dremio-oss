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
package com.dremio.exec.store.ischema;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.connector.metadata.extensions.SupportsReadSignature;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class InfoSchemaStoragePlugin
    implements StoragePlugin, SupportsReadSignature, SupportsListingDatasets {
  public static String NAME = "INFORMATION_SCHEMA";

  static final ImmutableMap<String, InformationSchemaTable> TABLE_MAP =
      FluentIterable.from(InformationSchemaTable.values())
          .uniqueIndex(input -> input.name().toLowerCase());

  private final SabotContext context;

  public InfoSchemaStoragePlugin(SabotContext context, String name) {
    Preconditions.checkArgument(NAME.equals(name));
    this.context = context;
  }

  SabotContext getSabotContext() {
    return context;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public SourceState getState() {
    return SourceState.GOOD;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return InfoSchemaRulesFactory.class;
  }

  @Override
  public void close() {}

  @Override
  public void start() {}

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
    return () -> Arrays.stream(InformationSchemaTable.values()).iterator();
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(
      EntityPath datasetPath, GetDatasetOption... options) {
    if (datasetPath.size() != 2) {
      return Optional.empty();
    }

    final InformationSchemaTable table = TABLE_MAP.get(datasetPath.getName().toLowerCase());
    if (table == null) {
      return Optional.empty();
    }

    return Optional.of(table);
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options) {
    return datasetHandle.unwrap(InformationSchemaTable.class);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(
      DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
    return datasetHandle.unwrap(InformationSchemaTable.class);
  }

  @Override
  public boolean containerExists(EntityPath containerPath, GetMetadataOption... options) {
    return false;
  }

  @Override
  public BytesOutput provideSignature(DatasetHandle datasetHandle, DatasetMetadata metadata) {
    return BytesOutput.NONE;
  }

  @Override
  public MetadataValidity validateMetadata(
      BytesOutput signature,
      DatasetHandle datasetHandle,
      DatasetMetadata metadata,
      ValidateMetadataOption... options) {
    return MetadataValidity.VALID;
  }
}
