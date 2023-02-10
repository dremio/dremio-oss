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
package com.dremio.dac.explore;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.catalog.DataplaneTableInfo;
import com.dremio.exec.catalog.DataplaneViewInfo;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.catalog.View;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Fake Versioned Plugin class for test
 */
public class FakeVersionedPlugin implements VersionedPlugin, StoragePlugin {
  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    return Optional.empty();
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) throws ConnectorException {
    return null;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle datasetHandle, PartitionChunkListing chunkListing, GetMetadataOption... options) throws ConnectorException {
    return null;
  }

  @Override
  public boolean containerExists(EntityPath containerPath) {
    return false;
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(VersionContext versionContext) {
    return null;
  }

  @Override
  public Stream<ExternalNamespaceEntry> listTablesIncludeNested(List<String> catalogPath, VersionContext version) {
    return null;
  }

  @Override
  public Stream<ExternalNamespaceEntry> listViewsIncludeNested(List<String> catalogPath, VersionContext version) {
    return null;
  }

  @Override
  public EntityType getType(List<String> key, ResolvedVersionContext version) {
    return null;
  }

  @Override
  public Stream<DataplaneTableInfo> getAllTableInfo() {
    return null;
  }

  @Override
  public Stream<DataplaneViewInfo> getAllViewInfo() {
    return null;
  }

  @Override
  public Stream<Table> getAllInformationSchemaTableInfo(SearchQuery searchQuery) {
    return null;
  }

  @Override
  public Stream<View> getAllInformationSchemaViewInfo(SearchQuery searchQuery) {
    return null;
  }

  @Override
  public Stream<Schema> getAllInformationSchemaSchemataInfo(SearchQuery searchQuery) {
    return null;
  }

  @Override
  public Stream<TableSchema> getAllInformationSchemaColumnInfo(SearchQuery searchQuery) {
    return null;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return false;
  }

  @Override
  public SourceState getState() {
    return null;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return null;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
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
}
