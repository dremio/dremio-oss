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

import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.PartitionSpec;

import com.dremio.datastore.SearchTypes;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

/**
 * TableMetadata interface. This is how a table is exposed to the planning environment.
 */
public interface TableMetadata {
  NamespaceKey getName();

  StoragePluginId getStoragePluginId();

  /**
   * Should be moved to ReadDefinition.
   * @return
   */
  @Deprecated
  FileConfig getFormatSettings();

  /**
   * Specific configuration associated with a particular type of reader.
   * @return
   */
  ReadDefinition getReadDefinition();

  DatasetType getType();

  String computeDigest();

  String getUser();

  TableMetadata prune(SearchTypes.SearchQuery partitionFilterQuery) throws NamespaceException;

  TableMetadata prune(Predicate<PartitionChunkMetadata> partitionPredicate) throws NamespaceException;

  TableMetadata prune(List<PartitionChunkMetadata> newSplits) throws NamespaceException;

  /**
   * Get an opaque key to perform comparison on splits
   *
   * @return the splits key
   */
  SplitsKey getSplitsKey();

  Iterator<PartitionChunkMetadata> getSplits();

  double getSplitRatio() throws NamespaceException;

  int getSplitCount();

  BatchSchema getSchema();

  long getApproximateRecordCount();

  boolean isPruned() throws NamespaceException;

  String getVersion();

  DatasetConfig getDatasetConfig();

  /**
   * Get primary key
   *
   * @return Primary key
   */
  List<String> getPrimaryKey();


  default TableVersionContext getVersionContext() { return null; }

  default Set<String> getInvalidPartitionColumns() {
    if (null == getDatasetConfig().getPhysicalDataset().getIcebergMetadata()) {
      return ImmutableSet.of();
    } else if (null != getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionSpecsJsonMap()) {
      Map<Integer, PartitionSpec> partitionSpecMap =
        IcebergSerDe.deserializeJsonPartitionSpecMap(
          deserializedJsonAsSchema(getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getJsonSchema()),
          getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionSpecsJsonMap().toByteArray());
      return IcebergUtils.getInvalidColumnsForPruning(partitionSpecMap);
    } else if (null != getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionSpecs()) {
      Map<Integer, PartitionSpec> partitionSpecMap = IcebergSerDe.deserializePartitionSpecMap(getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionSpecs().toByteArray());
      return IcebergUtils.getInvalidColumnsForPruning(partitionSpecMap);
    } else {
      return ImmutableSet.of();
    }
  }

  default PartitionFilterGranularity getPartitionFilterGranularity() {
    if(getDatasetConfig().getPhysicalDataset().getIcebergMetadata() != null
      || DatasetHelper.isIcebergDataset(getDatasetConfig())) {
      return PartitionFilterGranularity.FINE_GRAIN;
    } else {
      return PartitionFilterGranularity.RANGE;
    }
  }

  enum PartitionFilterGranularity {
    FINE_GRAIN, RANGE
  }
}
