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

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitsKey;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import java.util.Iterator;
import java.util.List;

/** A pointer to a table exposed by the namespace service. May load lazily. */
public class TableMetadataImpl implements TableMetadata {
  private final StoragePluginId pluginId;
  private final DatasetConfig config;
  private final SplitsPointer splits;
  private final String user;
  private final List<String> primaryKey;

  private BatchSchema schema;

  public TableMetadataImpl(
      StoragePluginId pluginId,
      DatasetConfig config,
      String user,
      SplitsPointer splits,
      List<String> primaryKey) {
    this.pluginId = Preconditions.checkNotNull(pluginId);
    this.config = config;
    this.splits = splits;
    this.user = user;
    this.primaryKey = primaryKey;
  }

  @Override
  public String getVersion() {
    if (config.getTag() == null) {
      return null;
    }

    return config.getTag();
  }

  @Override
  public StoragePluginId getStoragePluginId() {
    return pluginId;
  }

  @Override
  public NamespaceKey getName() {
    return new NamespaceKey(config.getFullPathList());
  }

  @Override
  public SplitsKey getSplitsKey() {
    return splits;
  }

  @Override
  public Iterator<PartitionChunkMetadata> getSplits() {
    return splits.getPartitionChunks().iterator();
  }

  @Override
  public TableMetadata prune(SearchQuery partitionFilterQuery) {
    SplitsPointer splits2 = splits.prune(partitionFilterQuery);
    if (splits2 != splits) {
      return new TableMetadataImpl(pluginId, config, user, splits2, primaryKey);
    }
    return this;
  }

  @Override
  public TableMetadata prune(Predicate<PartitionChunkMetadata> partitionPredicate) {
    return new TableMetadataImpl(
        pluginId, config, user, splits.prune(partitionPredicate), primaryKey);
  }

  @Override
  public TableMetadata prune(List<PartitionChunkMetadata> newPartitionChunks) {
    return new TableMetadataImpl(
        pluginId,
        config,
        user,
        MaterializedSplitsPointer.prune(splits, newPartitionChunks),
        primaryKey);
  }

  @Override
  public String computeDigest() {
    return String.format(
        "%s|%s|%s|%s",
        splits.computeDigest(), pluginId.getName(), config.getId().getId(), getVersionContext());
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public double getSplitRatio() throws NamespaceException {
    return splits.getSplitRatio();
  }

  @Override
  public int getSplitCount() {
    return splits.getSplitsCount();
  }

  @Override
  public FileConfig getFormatSettings() {
    return config.getPhysicalDataset().getFormatSettings();
  }

  @Override
  public ReadDefinition getReadDefinition() {
    return config.getReadDefinition();
  }

  @Override
  public DatasetType getType() {
    return config.getType();
  }

  @Override
  public BatchSchema getSchema() {
    if (schema == null) {
      schema = CalciteArrowHelper.fromDataset(config);
    }

    return schema;
  }

  @Override
  public long getApproximateRecordCount() {
    return config.getReadDefinition().getScanStats().getRecordCount();
  }

  @Override
  public boolean isPruned() throws NamespaceException {
    return splits.isPruned();
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof TableMetadataImpl)) {
      return false;
    }
    TableMetadataImpl castOther = (TableMetadataImpl) other;
    return Objects.equal(schema, castOther.schema)
        && Objects.equal(pluginId, castOther.pluginId)
        && Objects.equal(config, castOther.config)
        && Objects.equal(splits, castOther.splits);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schema, pluginId, config, splits);
  }

  @Override
  public DatasetConfig getDatasetConfig() {
    return config;
  }

  @Override
  public List<String> getPrimaryKey() {
    return primaryKey;
  }
}
