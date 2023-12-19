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
package com.dremio.service.namespace.dataset;

import java.util.List;

import com.dremio.service.namespace.BoundedDatasetCount;
import com.dremio.service.namespace.DatasetConfigAndEntitiesOnPath;
import com.dremio.service.namespace.DatasetMetadataSaver;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.EntityId;

/**
 * Namespace operations for Datasets.
 */
public interface DatasetNamespaceService {
  //// CREATE or UPDATE
  void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset, NamespaceAttribute... attributes) throws NamespaceException;

  // Create physical dataset if it doesn't exist.
  // TODO: DX-4493 No one is checking the return value. Not sure of the purpose of the return value.
  boolean tryCreatePhysicalDataset(NamespaceKey datasetPath, DatasetConfig config, NamespaceAttribute... attributes) throws NamespaceException;
  DatasetConfig renameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath) throws NamespaceException;

  /**
   * Create a dataset metadata saver for the given dataset.
   * @param datasetPath              dataset path
   * @param datasetId                dataset id
   * @param splitCompression         compression to be used on the (multi-)splits in the K/V store
   * @param maxSinglePartitionChunks maximum number of single split partition chunks allowed to be saved together
   * @return                         dataset metadata saver
   */
  DatasetMetadataSaver newDatasetMetadataSaver(NamespaceKey datasetPath, EntityId datasetId, NamespaceService.SplitCompression splitCompression, long maxSinglePartitionChunks, boolean datasetMetadataConsistencyValidate);

  //// READ
  /**
   * Returns {@link DatasetConfig configuration} corresponding to given path.
   *
   * @param datasetPath  path whose config will be returned
   * @throws NamespaceException  if a namespace or a dataset cannot be found for the given key
   */
  DatasetConfig getDataset(NamespaceKey datasetPath) throws NamespaceException;

  /**
   * Returns {@link DatasetConfigAndEntitiesOnPath} corresponding to given path.
   *
   * @param datasetPath  path whose config will be returned
   * @throws NamespaceException  if a namespace or a dataset cannot be found for the given key
   */
  DatasetConfigAndEntitiesOnPath getDatasetAndEntitiesOnPath(NamespaceKey datasetPath) throws NamespaceException;

  List<DatasetConfig> getDatasets();

  //// LIST or COUNT datasets under folder/space/home/source
  //// Note: use sparingly!
  Iterable<NamespaceKey> getAllDatasets(final NamespaceKey parent) throws NamespaceException;
  int getAllDatasetsCount(NamespaceKey path) throws NamespaceException;

  /**
   * Get the list of datasets under the given path with bounds to stop searching.
   *
   * @param root path to container of search start
   * @param searchTimeLimitMillis Time (wall clock) limit for searching. Count stops when this limit is reached and
   *                              returns the count so far
   * @param countLimitToStopSearch Limit to stop searching. If we reach this number of datasets in count, stop
   *                               searching and return.
   * @return
   */
  BoundedDatasetCount getDatasetCount(NamespaceKey root, long searchTimeLimitMillis, int countLimitToStopSearch)
    throws NamespaceException;
  /**
   * finds a dataset using UUID
   * @param uuid
   * @return a dataset, or null if not found.
   */
  DatasetConfig findDatasetByUUID(String uuid);

  //// DELETE
  void deleteDataset(NamespaceKey datasetPath, String version, NamespaceAttribute... attributes) throws NamespaceException;
}
