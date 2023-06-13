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

package com.dremio.connector.sample;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.PartitionValue;

/**
 *
 * implementation of class DatasetHandle
 *
 */
final class SampleHandleImpl implements DatasetHandle, DatasetMetadata, PartitionChunkListing {
  private final DatasetMetadata datasetMetadata;
  private final EntityPath datasetPath;
  private final List<PartitionChunk> partitionChunks;
  private final List<String> partitionColumns;

  @Override
  public DatasetStats getDatasetStats() {
    return (datasetMetadata == null ?
      null :
      datasetMetadata.getDatasetStats());
  }

  @Override
  public Schema getRecordSchema() {
    return (datasetMetadata == null) ?
    null :
    datasetMetadata.getRecordSchema();
  }

  @Override
  public EntityPath getDatasetPath() {
    return datasetPath;
  }

  @Override
  public Iterator<? extends PartitionChunk> iterator() {
    return partitionChunks.iterator();
  }

  private SampleHandleImpl(DatasetMetadata datasetMetadata, EntityPath datasetPath,
                           List<PartitionChunk> partitionChunks, List<String> partitionColumns) {
    this.datasetMetadata = datasetMetadata;
    this.datasetPath = datasetPath;
    this.partitionChunks = partitionChunks;
    this.partitionColumns = partitionColumns;
  }

  static SampleHandleImpl of(DatasetMetadata datasetMetadata, EntityPath entityPath) {
    return new SampleHandleImpl(datasetMetadata,
      entityPath,
      new ArrayList<>(),
      (datasetMetadata.getPartitionColumns() == null ?
        new ArrayList<>() : datasetMetadata.getPartitionColumns()));
  }

  static SampleHandleImpl of(DatasetMetadata datasetMetadata, EntityPath entityPath, List<PartitionChunk> partitionChunks) {
    return new SampleHandleImpl(datasetMetadata,
      entityPath,
      partitionChunks,
      (datasetMetadata.getPartitionColumns() == null ?
        new ArrayList<>() : datasetMetadata.getPartitionColumns()));
  }

  static SampleHandleImpl of(DatasetMetadata datasetMetadata, EntityPath entityPath,
                             List<PartitionChunk> partitionChunks, List<String> partitionColumns) {
    return new SampleHandleImpl(datasetMetadata,
      entityPath,
      partitionChunks,
      partitionColumns);
  }

  DatasetMetadata getDatasetMetadata() {
    return datasetMetadata;
  }

  void addPartitionChunk() {
    this.partitionChunks.add(PartitionChunk.of(new ArrayList<PartitionValue>(), new ArrayList<DatasetSplit>()));
  }

  void addPartitionChunk(PartitionChunk partitionChunk) {
    this.partitionChunks.add(partitionChunk);
  }

  void addPartitionChunk(List<PartitionValue> partitionValues, List<DatasetSplit> datasetSplits) {
    this.partitionChunks.add(PartitionChunk.of(partitionValues, datasetSplits));
  }

  List<PartitionChunk> getPartitionChunks() {
    return partitionChunks;
  }

  @Override
  public List<String> getPartitionColumns() {
    return datasetMetadata == null ?
      partitionColumns :
      datasetMetadata.getPartitionColumns();
  }
}
