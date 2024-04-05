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
package com.dremio.exec.store;

import com.dremio.datastore.SearchTypes;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Predicate;
import java.util.Iterator;
import java.util.List;

/**
 * A delegating TableMetadata class that allows for overriding of specific methods whilst being able
 * to ignore the actual implementing class.
 */
public class DelegatingTableMetadata implements TableMetadata {

  private final TableMetadata tableMetadata;

  public DelegatingTableMetadata(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  @Override
  public NamespaceKey getName() {
    return tableMetadata.getName();
  }

  @Override
  public StoragePluginId getStoragePluginId() {
    return tableMetadata.getStoragePluginId();
  }

  @Override
  public FileConfig getFormatSettings() {
    return tableMetadata.getFormatSettings();
  }

  @Override
  public ReadDefinition getReadDefinition() {
    return tableMetadata.getReadDefinition();
  }

  @Override
  public DatasetType getType() {
    return tableMetadata.getType();
  }

  @Override
  public String computeDigest() {
    return tableMetadata.computeDigest();
  }

  @Override
  public String getUser() {
    return tableMetadata.getUser();
  }

  @Override
  public TableMetadata prune(SearchTypes.SearchQuery partitionFilterQuery) {
    return tableMetadata.prune(partitionFilterQuery);
  }

  @Override
  public TableMetadata prune(Predicate<PartitionChunkMetadata> partitionPredicate) {
    return tableMetadata.prune(partitionPredicate);
  }

  @Override
  public TableMetadata prune(List<PartitionChunkMetadata> newSplits) {
    return tableMetadata.prune(newSplits);
  }

  @Override
  public SplitsKey getSplitsKey() {
    return tableMetadata.getSplitsKey();
  }

  @Override
  public Iterator<PartitionChunkMetadata> getSplits() {
    return tableMetadata.getSplits();
  }

  @Override
  public double getSplitRatio() throws NamespaceException {
    return tableMetadata.getSplitRatio();
  }

  @Override
  public int getSplitCount() {
    return tableMetadata.getSplitCount();
  }

  @Override
  public BatchSchema getSchema() {
    return tableMetadata.getSchema();
  }

  @Override
  public long getApproximateRecordCount() {
    return tableMetadata.getApproximateRecordCount();
  }

  @Override
  public boolean isPruned() throws NamespaceException {
    return tableMetadata.isPruned();
  }

  @Override
  public String getVersion() {
    return tableMetadata.getVersion();
  }

  @Override
  public DatasetConfig getDatasetConfig() {
    return tableMetadata.getDatasetConfig();
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  @Override
  public List<String> getPrimaryKey() {
    return tableMetadata.getPrimaryKey();
  }
}
