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

import java.util.Iterator;
import java.util.List;

import com.dremio.datastore.SearchTypes;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Objects;

/**
 * A lazy TableMetadata which is not fully materialized.
 */
public class PartialTableMetadata implements TableMetadata {
  private final SourceTableDefinition datasetAccessor;
  private final NamespaceService ns;
  private final StoragePluginId pluginId;
  private final String user;

  private DatasetConfig datasetConfig;
  private BatchSchema schema;
  private TableMetadataImpl datasetPointer;


  public PartialTableMetadata(StoragePluginId plugin, DatasetConfig config, String user, NamespaceService namespace, SourceTableDefinition datasetAccessor) {
    this.datasetAccessor = datasetAccessor;
    this.datasetConfig = config;
    this.pluginId = plugin;
    this.user = user;
    this.ns = namespace;
  }

  private void loadIfNecessary() {
    if(datasetPointer != null){
      return;
    }

    SplitsPointer splitsPointer;
    if(datasetConfig.getReadDefinition() != null) {
      splitsPointer = new SplitsPointerImpl(datasetConfig, ns);
    } else {
      try{
        final DatasetConfig newDatasetConfig = datasetAccessor.getDataset();
        newDatasetConfig.setId(datasetConfig.getId());
        newDatasetConfig.setVersion(datasetConfig.getVersion());
        List<DatasetSplit> splits = datasetAccessor.getSplits();
        ns.addOrUpdateDataset(getName(), newDatasetConfig, splits);
        datasetConfig = newDatasetConfig;
        splitsPointer = new SplitsPointerImpl(splits, splits.size());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    datasetPointer = new TableMetadataImpl(pluginId, datasetConfig, user, splitsPointer);
  }

  @Override
  public NamespaceKey getName() {
    return new NamespaceKey(datasetConfig.getFullPathList());
  }

  @Override
  public StoragePluginId getStoragePluginId() {
    return pluginId;
  }

  @Override
  public String getUser() {
    return user;
  }

  // splits
  @Override
  public String computeDigest() {
    loadIfNecessary();
    return datasetPointer.computeDigest();
  }

  @Override
  public Iterator<DatasetSplit> getSplits() {
    loadIfNecessary();
    return datasetPointer.getSplits();
  }

  @Override
  public double getSplitRatio() throws NamespaceException {
    loadIfNecessary();
    return datasetPointer.getSplitRatio();
  }

  @Override
  public TableMetadata prune(SearchTypes.SearchQuery partitionFilterQuery) throws NamespaceException {
    loadIfNecessary();
    return datasetPointer.prune(partitionFilterQuery);
  }

  @Override
  public TableMetadata prune(List<DatasetSplit> newSplits) throws NamespaceException {
    loadIfNecessary();
    return datasetPointer.prune(newSplits);
  }

  @Override
  public int getSplitCount() {
    loadIfNecessary();
    return datasetPointer.getSplitCount();
  }

  @Override
  public FileConfig getFormatSettings() {
    loadIfNecessary();
    return datasetConfig.getPhysicalDataset().getFormatSettings();
  }

  @Override
  public ReadDefinition getReadDefinition() {
    return datasetConfig.getReadDefinition();
  }

  @Override
  public DatasetType getType() {
    return datasetConfig.getType();
  }

  @Override
  public BatchSchema getSchema() {
    loadIfNecessary();
    return datasetPointer.getSchema();
  }

  @Override
  public long getApproximateRecordCount() {
    loadIfNecessary();
    return datasetPointer.getApproximateRecordCount();
  }

  @Override
  public boolean isPruned() throws NamespaceException {
    loadIfNecessary();
    return datasetPointer.isPruned();
  }

  @Override
  public boolean equals(final Object other) {
    loadIfNecessary();
    if (!(other instanceof PartialTableMetadata)) {
      return false;
    }
    PartialTableMetadata castOther = (PartialTableMetadata) other;
    return Objects.equal(datasetPointer, castOther.datasetPointer);
  }

  @Override
  public int hashCode() {
    loadIfNecessary();
    return Objects.hashCode(datasetPointer);
  }



}