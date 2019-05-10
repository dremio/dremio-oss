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

package com.dremio.connector.sample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitListing;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.google.common.collect.ImmutableList;

/**
 *
 * Sample test connector for SourceMetadata
 *
 * Provides a set of private sample class implementations for interfaces
 * and classes referred to by the interface SourceMetadata
 *
 */
public class SampleSourceMetadata implements SourceMetadata, SupportsListingDatasets {
  private final List<DatasetHandle> allDatasetHandles;

  public SampleSourceMetadata(List<DatasetHandle> datasetHandles) {
    this.allDatasetHandles = datasetHandles;
  }

  public SampleSourceMetadata(DatasetHandle datasetHandle) {
    this(Arrays.asList(datasetHandle));
  }

  public SampleSourceMetadata() {
    this(new ArrayList<>());
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
    return () -> allDatasetHandles.iterator();
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
    for (DatasetHandle handle : allDatasetHandles) {
      if (datasetPath.equals(handle.getDatasetPath())) {
        return Optional.of(handle);
      }
    }

    return Optional.empty();
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options
  ) {
    return ((SampleHandleImpl) datasetHandle).getDatasetMetadata();
  }

  @Override
  public PartitionChunkListing listPartitionChunks
    (DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
    return ((SampleHandleImpl) datasetHandle).unwrap(PartitionChunkListing.class);
  }

  @Override
  public boolean containerExists(EntityPath containerPath) {
    return true;
  }

  /**
   * return list of datasetMetadata
   * @return a list of dataset metadata
   */
  List<DatasetMetadata> getAllDatasetMetadata() {
    List<DatasetMetadata> allDatasetMetadata = new ArrayList<>();
    for (DatasetHandle handle : allDatasetHandles) {
      allDatasetMetadata.add(((SampleHandleImpl) handle).getDatasetMetadata());
    }

    return allDatasetMetadata;
  }

  /**
   * return a list of all existing dataset handles
   *
   * @param options options for the dataset
   * @return a list of dataset handles
   */
  List<DatasetHandle> getAllDatasetHandles(GetDatasetOption options) {
    Iterator<? extends DatasetHandle> handleListingIterator = listDatasetHandles(options).iterator();
    List<DatasetHandle> datasetHandles = new ArrayList<>();

    while (handleListingIterator.hasNext()) {
      datasetHandles.add(handleListingIterator.next());
    }

    return  datasetHandles;
  }

  /**
   * return a list of all entity paths associated with the metadata
   *
   * @return a list of entity paths
   *
   */
  List<EntityPath> getAllEntityPaths() {
    List<EntityPath> entityPaths = new ArrayList<>();
    for (DatasetHandle datasetHandle : allDatasetHandles) {
      entityPaths.add(datasetHandle.getDatasetPath());
    }

    return entityPaths;
  }

  /**
   *
   * add a handle to the existing list of dataset handles
   *
   * @param handle a new dataset handle
   */
  public void addDatasetHandle(DatasetHandle handle) {
    this.allDatasetHandles.add(handle);
  }

  /**
   *
   * given some number n and n pathnames, generate those many datasets
   *
   * @param numDatasets number of datasets to add
   * @param pathNames   list of pathnames
   */
  public void addNDatasets(int numDatasets, List<List<String>> pathNames) {
    if (numDatasets != pathNames.size()) {
      throw new UnsupportedOperationException();
    }

    DatasetStats datasetStats = DatasetStats.of(0, 0);
    Schema schema = new Schema(new ArrayList<>());

    for (int i = 0; i < numDatasets; i++) {
      EntityPath entityPath = new EntityPath(pathNames.get(i));
      DatasetMetadata datasetMetadata = DatasetMetadata.of(datasetStats, schema);

      addDatasetHandle(SampleHandleImpl.of(datasetMetadata, entityPath));
    }
  }


  /**
   * add n datasets to the namespace
   *
   * @param numDatasets number of datasets to add
   * @param numPartitionChunks number of partition chunks per dataset
   * @param numSplitsPerPartitionChunk number of splits per partition chunk
   */
  public void addNDatasets(int numDatasets, int numPartitionChunks, int numSplitsPerPartitionChunk) {
    DatasetStats datasetStats = DatasetStats.of(0, 0);
    Schema schema = new Schema(new ArrayList<>());

    for (int datasetI = 0 ; datasetI < numDatasets; datasetI++) {
      DatasetMetadata datasetMetadata = DatasetMetadata.of(datasetStats, schema);
      EntityPath entityPath = new EntityPath(ImmutableList.of("a", String.format("%05d", datasetI)));
      List<PartitionChunk> partitionChunks = new ArrayList<>();

      for (int partitionJ = 0; partitionJ < numPartitionChunks; partitionJ++) {
        List<DatasetSplit> datasetSplitList = new ArrayList<>();

        for (int splitK = 0; splitK < numSplitsPerPartitionChunk; splitK++) {
          final int chunkNum = partitionJ;
          final int splitNum = splitK;
          BytesOutput extraInfo = os -> os.write(String.format("p%d_s%d", chunkNum, splitNum).getBytes());

          DatasetSplit datasetSplit = DatasetSplit.of(new ArrayList<>(), 10, 10, extraInfo);

          datasetSplitList.add(datasetSplit);
        }

        partitionChunks.add(PartitionChunk.of(datasetSplitList));
      }

      addDatasetHandle(SampleHandleImpl.of(datasetMetadata, entityPath, partitionChunks));
    }
  }

  /**
   * given a list of paths, generate datasets that associate with those paths
   *
   * @param pathNames a list of pathnames
   */
  public void addManyDatasets(List<List<String>> pathNames) {
    DatasetStats datasetStats = DatasetStats.of(0, 0);
    Schema schema = new Schema(new ArrayList<>());

    for (int i = 0; i < pathNames.size(); i++) {
      DatasetMetadata datasetMetadata = DatasetMetadata.of(datasetStats, schema);

      addDatasetHandle(SampleHandleImpl.of(
          datasetMetadata, new EntityPath(pathNames.get(i))));
    }
  }

  /**
   * given an entity path + number of partition columns, generate dataset
   *
   * @param entityPaths a list of entity path
   * @param numPartitionColumns number of partition columns for each dataset
   */
  public void addManyDatasets(List<EntityPath> entityPaths, int numPartitionColumns) {
    DatasetStats datasetStats = DatasetStats.of(0, 0);
    Schema schema = new Schema(new ArrayList<>());

    for (EntityPath entityPath : entityPaths) {
      DatasetMetadata datasetMetadata = DatasetMetadata.of(datasetStats, schema);
      addDatasetHandle(
        SampleHandleImpl.of(
          datasetMetadata,
          entityPath,
          new ArrayList<>(),
          new ArrayList<>(numPartitionColumns)));
    }
  }

  /**
   * given a path, generate a dataset associated with that path
   *
   * @param pathName a path name in the form of a list
   */
  public void addSingleDataset(List<String> pathName) {
    DatasetStats datasetStats = DatasetStats.of(0, 0);
    Schema schema = new Schema(new ArrayList<>());

    addDatasetHandle(
      SampleHandleImpl.of(
        DatasetMetadata.of(datasetStats, schema),
        new EntityPath(pathName)));
  }

  /**
   * given an entity path, generate a dataset associated with that path
   *
   * @param entityPath an entity path
   */
  public void addSingleDataset(EntityPath entityPath) {
    DatasetStats datasetStats = DatasetStats.of(0, 0);
    Schema schema = new Schema(new ArrayList<>());

    addDatasetHandle(
      SampleHandleImpl.of(
        DatasetMetadata.of(datasetStats, schema),
        entityPath));
  }

  /**
   * given a dataset handle, add a partition chunk associated with it
   *
   * @param datasetHandle a dataset handle
   */
  public void addPartitionChunk(DatasetHandle datasetHandle) {
    ((SampleHandleImpl) datasetHandle).addPartitionChunk();
  }

  /**
   * given a dataset handle and a value n, add n partition chunks associated with this handle
   *
   * @param datasetHandle a dataset handle
   * @param n             number of partition chunks to associate with this handle
   */
  public void addNPartitionChunks(DatasetHandle datasetHandle, int n) {
    for (int i = 0; i < n; i++) {
      ((SampleHandleImpl) datasetHandle).addPartitionChunk();
    }
  }

  /**
   * return an iterator over a list of partition chunks associated with a
   * dataset handle
   *
   * @param datasetHandle a datasetHandle
   * @return an iterator over partition chunks associated with this handle
   */
  public Iterator<? extends PartitionChunk> getPartitionChunks(DatasetHandle datasetHandle) {
    return (listPartitionChunks((SampleHandleImpl) datasetHandle)).iterator();
  }

  // impl - object conversion

  /**
   * given a datasetHandle, return the dataset that owns this handle
   *
   * @param datasetHandle a dataset handle
   * @return the associated metadata
   */
  public DatasetMetadata getDatasetMetadataFromHandle(DatasetHandle datasetHandle) {
    return ((SampleHandleImpl) datasetHandle).getDatasetMetadata();
  }

  /**
   * given a datasetHandle, return the handle associated with this dataset
   *
   * @param datasetMetadata datasetMetadata
   * @return the associated handle
   */
  public SampleHandleImpl getHandleFromDatasetMetadata(DatasetMetadata datasetMetadata) {
    for (DatasetHandle datasetHandle : allDatasetHandles) {
      SampleHandleImpl sampleHandle = (SampleHandleImpl) datasetHandle;

      if (datasetMetadata.equals(sampleHandle.getDatasetMetadata())) {
        return sampleHandle;
      }
    }

    return null;
  }

  /**
   * given a dataset, return its associated entityPath
   *
   * @param datasetMetadata a dataset
   * @return the associated entity path
   */
  public EntityPath getEntityPathFromDataset(DatasetMetadata datasetMetadata) {
    for (DatasetHandle datasetHandle : allDatasetHandles) {
      SampleHandleImpl sampleHandle = (SampleHandleImpl) datasetHandle;

      if (datasetMetadata.equals(sampleHandle.getDatasetMetadata())) {
        return sampleHandle.getDatasetPath();
      }
    }

    return null;
  }

  /**
   * given an entityPath, return the datasetHandle associated with it through
   * their datasetMetadata
   *
   * @param datasetHandle a dataset handle
   * @return the associated entityPath
   */
  public EntityPath getEntityPathFromHandle(DatasetHandle datasetHandle) {
    return ((SampleHandleImpl) datasetHandle).getDatasetPath();
  }

  /**
   * given a dataset, return the associated partition chunks
   *
   * @param datasetMetadata a datasetMetadata object
   * @return associated partition chunks
   */
  public List<PartitionChunk> getPartitionChunksForDataset(DatasetMetadata datasetMetadata) {
    for (DatasetHandle datasetHandle : allDatasetHandles) {
      if (datasetMetadata.equals(((SampleHandleImpl) datasetHandle).getDatasetMetadata())) {
        return ((SampleHandleImpl) datasetHandle).getPartitionChunks();
      }
    }

    return null;
  }

  /**
   * given a dataset handle, return its associated partition chunks
   *
   * @param datasetHandle a dataset handle
   * @return associated partition chunks
   */
  public List<PartitionChunk> getPartitionChunksForDataset(DatasetHandle datasetHandle) {
    return ((SampleHandleImpl) datasetHandle).getPartitionChunks();
  }

  /**
   * given a dataset handle, return its associated dataset splits
   *
   * @param datasetHandle a dataset handle
   * @return
   */
  public List<List<DatasetSplit> > getDatasetSplitsForDataset(DatasetHandle datasetHandle) {
    List<List<DatasetSplit>> allDatasetSplits = new ArrayList<>();
    List<PartitionChunk> partitionChunks = ((SampleHandleImpl) datasetHandle).getPartitionChunks();

    if (partitionChunks == null) {
      return allDatasetSplits;
    }

    for (PartitionChunk partitionChunk : partitionChunks) {
      List<DatasetSplit> datasetSplits = new ArrayList<>();
      DatasetSplitListing datasetSplitListing = (partitionChunk.getSplits());
      while (datasetSplitListing.iterator().hasNext()) {
        datasetSplits.add(datasetSplitListing.iterator().next());
      }

      allDatasetSplits.add(datasetSplits);
    }

    return allDatasetSplits;
  }
}
