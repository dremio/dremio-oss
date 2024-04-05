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
package com.dremio.exec.store.sys;

import com.dremio.common.utils.PathUtils;
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
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.JobResultInfoProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.Views;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SystemStoragePlugin
    implements StoragePlugin, SupportsReadSignature, SupportsListingDatasets {
  public static final String NAME = "sys";

  private static final ImmutableMap<EntityPath, SystemTable> DATASET_MAP =
      ImmutableMap.copyOf(
          Stream.of(SystemTable.values())
              .collect(
                  Collectors.toMap(
                      systemTable -> canonicalize(systemTable.getDatasetPath()),
                      Function.identity())));

  private static final String JOBS_STORAGE_PLUGIN_NAME = "__jobResultsStore";

  private final SabotContext context;
  private final JobResultInfoProvider jobResultInfoProvider;

  SystemStoragePlugin(SabotContext context, String name) {
    Preconditions.checkArgument(NAME.equals(name));
    this.context = context;
    this.jobResultInfoProvider = context.getJobResultInfoProvider();
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
    if (!JobResultInfoProvider.isJobResultsTable(tableSchemaPath)) {
      return null;
    }

    final String jobId = Iterables.getLast(tableSchemaPath);
    final Optional<JobResultInfoProvider.JobResultInfo> jobResultInfo =
        jobResultInfoProvider.getJobResultInfo(jobId, schemaConfig.getUserName());

    return jobResultInfo
        .map(
            info -> {
              final View view =
                  Views.fieldTypesToView(
                      jobId,
                      getJobResultsQuery(info.getResultDatasetPath()),
                      ViewFieldsHelper.getBatchSchemaFields(info.getBatchSchema()),
                      null);

              return new ViewTable(
                  new NamespaceKey(tableSchemaPath),
                  view,
                  CatalogUser.from(SystemUser.SYSTEM_USERNAME),
                  info.getBatchSchema());
            })
        .orElse(null);
  }

  private static String getJobResultsQuery(List<String> resultDatasetPath) {
    return String.format("SELECT * FROM %s", PathUtils.constructFullPath(resultDatasetPath));
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return SystemTableRulesFactory.class;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public void start() {}

  @Override
  public void close() {}

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
    return () -> Stream.of(SystemTable.values()).iterator();
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(
      EntityPath datasetPath, GetDatasetOption... options) {
    //noinspection unchecked
    return (Optional<DatasetHandle>) (Object) getDataset(datasetPath);
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options) {
    return datasetHandle.unwrap(SystemTable.class);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(
      DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
    return datasetHandle.unwrap(SystemTable.class);
  }

  @Override
  public boolean containerExists(EntityPath containerPath) {
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
    // returning INVALID allows us to refresh system source to apply schema updates.
    // system source is only refreshed once when CatalogService starts up
    // and won't be refreshed again since its refresh policy is NEVER_REFRESH_POLICY.
    return MetadataValidity.INVALID;
  }

  private static EntityPath canonicalize(EntityPath entityPath) {
    return new EntityPath(
        entityPath.getComponents().stream().map(String::toLowerCase).collect(Collectors.toList()));
  }

  public static Optional<SystemTable> getDataset(EntityPath datasetPath) {
    return Optional.ofNullable(DATASET_MAP.get(canonicalize(datasetPath)));
  }
}
