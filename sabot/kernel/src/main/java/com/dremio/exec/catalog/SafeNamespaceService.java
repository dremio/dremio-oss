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

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyKVStore.LegacyFindByRange;
import com.dremio.exec.catalog.ManagedStoragePlugin.SafeRunner;
import com.dremio.service.namespace.BoundedDatasetCount;
import com.dremio.service.namespace.DatasetConfigAndEntitiesOnPath;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceType;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.PartitionChunkId.SplitOrphansRetentionPolicy;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.DatasetMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.SourceMetadata;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A decorator for namespace service that only does operations underneath a safe runner to avoid
 * making changes when we shouldn't.
 */
class SafeNamespaceService implements NamespaceService {
  private final NamespaceService delegate;
  private final SafeRunner runner;

  public SafeNamespaceService(NamespaceService delegate, SafeRunner runner) {
    super();
    this.delegate = delegate;
    this.runner = runner;
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey arg0, DatasetConfig arg1, NamespaceAttribute... arg2)
      throws NamespaceException {
    runner.doSafe(() -> delegate.addOrUpdateDataset(arg0, arg1, arg2));
  }

  @Override
  public void addOrUpdateFolder(NamespaceKey arg0, FolderConfig arg1, NamespaceAttribute... arg2)
      throws NamespaceException {
    runner.doSafe(() -> delegate.addOrUpdateFolder(arg0, arg1, arg2));
  }

  @Override
  public void addOrUpdateHome(NamespaceKey arg0, HomeConfig arg1) throws NamespaceException {
    runner.doSafe(() -> delegate.addOrUpdateHome(arg0, arg1));
  }

  @Override
  public void addOrUpdateSource(NamespaceKey arg0, SourceConfig arg1, NamespaceAttribute... arg2)
      throws NamespaceException {
    runner.doSafe(() -> delegate.addOrUpdateSource(arg0, arg1, arg2));
  }

  @Override
  public void addOrUpdateSpace(NamespaceKey arg0, SpaceConfig arg1, NamespaceAttribute... arg2)
      throws NamespaceException {
    runner.doSafe(() -> delegate.addOrUpdateSpace(arg0, arg1, arg2));
  }

  @Override
  public void addOrUpdateFunction(
      NamespaceKey arg0, FunctionConfig arg1, NamespaceAttribute... arg2)
      throws NamespaceException {
    runner.doSafe(() -> delegate.addOrUpdateFunction(arg0, arg1, arg2));
  }

  @Override
  public void canSourceConfigBeSaved(
      SourceConfig arg0, SourceConfig arg1, NamespaceAttribute... arg2)
      throws ConcurrentModificationException, NamespaceException {
    runner.doSafe(() -> delegate.canSourceConfigBeSaved(arg0, arg1, arg2));
  }

  @Override
  public void deleteDataset(NamespaceKey arg0, String arg1, NamespaceAttribute... arg2)
      throws NamespaceException {
    runner.doSafe(() -> delegate.deleteDataset(arg0, arg1, arg2));
  }

  @Override
  public void deleteEntity(NamespaceKey arg0) throws NamespaceException {
    runner.doSafe(() -> delegate.deleteEntity(arg0));
  }

  @Override
  public void deleteFolder(NamespaceKey arg0, String arg1) throws NamespaceException {
    runner.doSafe(() -> delegate.deleteFolder(arg0, arg1));
  }

  @Override
  public void deleteHome(NamespaceKey arg0, String arg1) throws NamespaceException {
    runner.doSafe(() -> delegate.deleteHome(arg0, arg1));
  }

  @Override
  public void deleteSource(NamespaceKey arg0, String arg1) throws NamespaceException {
    runner.doSafe(() -> delegate.deleteSource(arg0, arg1));
  }

  @Override
  public void deleteSourceWithCallBack(NamespaceKey arg0, String arg1, DeleteCallback arg2)
      throws NamespaceException {
    runner.doSafe(() -> delegate.deleteSourceWithCallBack(arg0, arg1, arg2));
  }

  @Override
  public void deleteSourceChildren(NamespaceKey arg0, String arg1, DeleteCallback arg2)
      throws NamespaceException {
    runner.doSafe(() -> delegate.deleteSourceChildren(arg0, arg1, arg2));
  }

  @Override
  public void deleteSpace(NamespaceKey arg0, String arg1) throws NamespaceException {
    runner.doSafe(() -> delegate.deleteSpace(arg0, arg1));
  }

  @Override
  public void deleteFunction(NamespaceKey arg0) throws NamespaceException {
    runner.doSafe(() -> delegate.deleteFunction(arg0));
  }

  @Override
  public int deleteSplitOrphans(SplitOrphansRetentionPolicy arg0, boolean arg1) {
    return runner.doSafe(() -> delegate.deleteSplitOrphans(arg0, arg1));
  }

  @Override
  public void deleteSplits(Iterable<PartitionChunkId> arg0) {
    runner.doSafe(() -> delegate.deleteSplits(arg0));
  }

  @Override
  public boolean exists(NamespaceKey arg0) {
    return runner.doSafe(() -> delegate.exists(arg0));
  }

  @Override
  public boolean hasChildren(NamespaceKey key) {
    return runner.doSafe(() -> delegate.hasChildren(key));
  }

  @Override
  public boolean exists(NamespaceKey arg0, Type arg1) {
    return runner.doSafe(() -> delegate.exists(arg0, arg1));
  }

  @Override
  public Iterable<Entry<NamespaceKey, NameSpaceContainer>> find(LegacyFindByCondition arg0) {
    return runner.doSafeIterable(() -> delegate.find(arg0));
  }

  @Override
  public DatasetConfig findDatasetByUUID(String arg0) {
    return runner.doSafe(() -> delegate.findDatasetByUUID(arg0));
  }

  @Override
  public Map<NamespaceKey, NamespaceType> getDatasetNamespaceTypes(NamespaceKey... datasetPaths) {
    return runner.doSafe(() -> delegate.getDatasetNamespaceTypes(datasetPaths));
  }

  @Override
  public Iterable<PartitionChunkMetadata> findSplits(LegacyFindByCondition arg0) {
    return runner.doSafeIterable(() -> delegate.findSplits(arg0));
  }

  @Override
  public Iterable<PartitionChunkMetadata> findSplits(LegacyFindByRange<PartitionChunkId> arg0) {
    return runner.doSafeIterable(() -> delegate.findSplits(arg0));
  }

  @Override
  public Iterable<NamespaceKey> getAllDatasets(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafeIterable(() -> delegate.getAllDatasets(arg0));
  }

  @Override
  public int getAllDatasetsCount(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getAllDatasetsCount(arg0));
  }

  @Override
  public Iterable<NameSpaceContainer> getAllDescendants(NamespaceKey root) {
    return runner.doSafe(() -> delegate.getAllDescendants(root));
  }

  @Override
  public List<Integer> getCounts(SearchQuery... arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getCounts(arg0));
  }

  @Override
  @WithSpan
  public DatasetConfig getDataset(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getDataset(arg0));
  }

  @Override
  @WithSpan
  public DatasetMetadata getDatasetMetadata(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafe(() -> DatasetMetadata.from(delegate.getDataset(arg0)));
  }

  @Override
  public DatasetConfigAndEntitiesOnPath getDatasetAndEntitiesOnPath(NamespaceKey arg0)
      throws NamespaceException {
    return runner.doSafe(() -> delegate.getDatasetAndEntitiesOnPath(arg0));
  }

  @Override
  public BoundedDatasetCount getDatasetCount(NamespaceKey arg0, long arg1, int arg2)
      throws NamespaceException {
    return runner.doSafe(() -> delegate.getDatasetCount(arg0, arg1, arg2));
  }

  @Override
  public List<NameSpaceContainer> getEntities(List<NamespaceKey> arg0)
      throws NamespaceNotFoundException {
    return runner.doSafe(() -> delegate.getEntities(arg0));
  }

  @Override
  public NameSpaceContainer getEntityById(String arg0) throws NamespaceNotFoundException {
    return runner.doSafe(() -> delegate.getEntityById(arg0));
  }

  @Override
  public List<NameSpaceContainer> getEntitiesByIds(List<String> arg0)
      throws NamespaceNotFoundException {
    return runner.doSafe(() -> delegate.getEntitiesByIds(arg0));
  }

  @Override
  public String getEntityIdByPath(NamespaceKey arg0) throws NamespaceNotFoundException {
    return runner.doSafe(() -> delegate.getEntityIdByPath(arg0));
  }

  @Override
  public NameSpaceContainer getEntityByPath(NamespaceKey entityPath) throws NamespaceException {
    return runner.doSafe(() -> delegate.getEntityByPath(entityPath));
  }

  @Override
  @WithSpan
  public FolderConfig getFolder(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getFolder(arg0));
  }

  @Override
  public HomeConfig getHome(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getHome(arg0));
  }

  @Override
  public List<HomeConfig> getHomeSpaces() {
    return runner.doSafe(() -> delegate.getHomeSpaces());
  }

  @Override
  public int getPartitionChunkCount(LegacyFindByCondition arg0) {
    return runner.doSafe(() -> delegate.getPartitionChunkCount(arg0));
  }

  @Override
  @WithSpan
  public SourceConfig getSource(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getSource(arg0));
  }

  @Override
  @WithSpan
  public SourceMetadata getSourceMetadata(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafe(() -> SourceMetadata.from(delegate.getSource(arg0)));
  }

  @Override
  public SourceConfig getSourceById(String arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getSourceById(arg0));
  }

  @Override
  public List<SourceConfig> getSources() {
    return runner.doSafe(() -> delegate.getSources());
  }

  @Override
  public List<DatasetConfig> getDatasets() {
    return runner.doSafe(() -> delegate.getDatasets());
  }

  @Override
  public SpaceConfig getSpace(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getSpace(arg0));
  }

  @Override
  public FunctionConfig getFunction(NamespaceKey arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getFunction(arg0));
  }

  @Override
  public SpaceConfig getSpaceById(String arg0) throws NamespaceException {
    return runner.doSafe(() -> delegate.getSpaceById(arg0));
  }

  @Override
  public List<SpaceConfig> getSpaces() {
    return runner.doSafe(() -> delegate.getSpaces());
  }

  @Override
  public List<FunctionConfig> getFunctions() {
    return runner.doSafe(() -> delegate.getFunctions());
  }

  @Override
  public List<NameSpaceContainer> list(NamespaceKey entityPath) throws NamespaceException {
    return runner.doSafe(() -> delegate.list(entityPath));
  }

  @Override
  public DatasetMetadataSaver newDatasetMetadataSaver(
      NamespaceKey arg0, EntityId arg1, SplitCompression arg2, long arg3, boolean arg4) {
    final DatasetMetadataSaver delegate =
        runner.doSafe(() -> this.delegate.newDatasetMetadataSaver(arg0, arg1, arg2, arg3, arg4));
    return new DatasetMetadataSaver() {

      @Override
      public void close() {
        delegate.close();
      }

      @Override
      public void savePartitionChunk(PartitionChunk arg0) throws IOException {
        runner.doSafe(() -> delegate.savePartitionChunk(arg0));
      }

      @Override
      public void saveDatasetSplit(DatasetSplit arg0) {
        runner.doSafe(() -> delegate.saveDatasetSplit(arg0));
      }

      @Override
      public void saveDataset(DatasetConfig arg0, boolean arg1, NamespaceAttribute... arg2)
          throws NamespaceException {
        runner.doSafe(() -> delegate.saveDataset(arg0, arg1, arg2));
      }
    };
  }

  @Override
  public DatasetConfig renameDataset(NamespaceKey arg0, NamespaceKey arg1)
      throws NamespaceException {
    return runner.doSafe(() -> delegate.renameDataset(arg0, arg1));
  }

  @Override
  public boolean tryCreatePhysicalDataset(
      NamespaceKey arg0, DatasetConfig arg1, NamespaceAttribute... arg2) throws NamespaceException {
    return runner.doSafe(() -> delegate.tryCreatePhysicalDataset(arg0, arg1, arg2));
  }
}
