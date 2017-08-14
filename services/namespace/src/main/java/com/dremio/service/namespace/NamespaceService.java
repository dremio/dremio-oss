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
package com.dremio.service.namespace;

import java.util.List;
import java.util.Map;

import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStore.FindByRange;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;

/**
 * Namespace operations from DAC
 */
public interface NamespaceService {
  /**
   * Factory to create namespace service for a given user
   */
  interface Factory {
    /**
     * Return a namespace service for a given user
     *
     * @param userName a valid user name
     * @return a namespace service instance
     * @throws NullPointerException if {@code userName} is null
     * @throws IllegalArgumentException if {@code userName} is invalid
     */
    NamespaceService get(String userName);
  }

  //// Create
  void addOrUpdateSource(NamespaceKey sourcePath, SourceConfig sourceConfig) throws NamespaceException;

  void addOrUpdateSpace(NamespaceKey spacePath, SpaceConfig spaceConfig) throws NamespaceException;

  void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset) throws NamespaceException;

  void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset, List<DatasetSplit> splits) throws NamespaceException;

  void addOrUpdateFolder(NamespaceKey folderPath, FolderConfig folderConfig) throws NamespaceException;

  void addOrUpdateHome(NamespaceKey homePath, HomeConfig homeConfig) throws NamespaceException;

  //// GET
  boolean exists(NamespaceKey key, Type type);
  boolean exists(NamespaceKey key);

  SourceConfig getSource(NamespaceKey sourcePath) throws NamespaceException;

  SpaceConfig getSpace(NamespaceKey spacePath) throws NamespaceException;

  /**
   * Returns {@link DatasetConfig configuration} corresponding to given path.
   *
   * @param datasetPath  path whose config will be returned
   * @throws NamespaceException  if a namespace or a dataset cannot be found for the given key
   */
  DatasetConfig getDataset(NamespaceKey datasetPath) throws NamespaceException;

  /**
   * Get multiple entities of given type
   * @param lookupKeys namespace keys
   * @return list of namespace containers with null if no value found for a key.
   *         Order of returned list matches with order of lookupKeys.
   * @throws NamespaceException
   */
  List<NameSpaceContainer> getEntities(List<NamespaceKey> lookupKeys) throws NamespaceException;

  List<SpaceConfig> getSpaces();

  List<HomeConfig> getHomeSpaces();

  List<SourceConfig> getSources();

  FolderConfig getFolder(NamespaceKey folderPath) throws NamespaceException;

  HomeConfig getHome(NamespaceKey homePath) throws NamespaceException;

  /**
   * Return list of counts matching each query
   * @param queries list of queries to perform search on
   * @return list of counts. Order of returned counts is same as order of queries.
   * @throws NamespaceException
   */
  List<Integer> getCounts(SearchQuery... queries) throws NamespaceException;

  //// LIST
  List<NameSpaceContainer> list(NamespaceKey folderPath) throws NamespaceException;

  //// LIST or COUNT datasets under folder/space/home/source
  List<NamespaceKey> getAllDatasets(final NamespaceKey parent) throws NamespaceException;

  List<FolderConfig> getAllFolders(final NamespaceKey parent) throws NamespaceException;

  int getAllDatasetsCount(NamespaceKey path) throws NamespaceException;

  //// DELETE
  void deleteSource(NamespaceKey sourcePath, long version) throws NamespaceException;

  void deleteSpace(NamespaceKey spacePath, long version) throws NamespaceException;

  void deleteEntity(NamespaceKey entityPath) throws NamespaceException;

  void deleteDataset(NamespaceKey datasetPath, long version) throws NamespaceException;

  void deleteFolder(NamespaceKey folderPath, long version) throws NamespaceException;

  //// RENAME
  DatasetConfig renameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath) throws NamespaceException;

  // Create physical dataset if it doesn't exist.
  // TODO: DX-4493 No one is checking the return value. Not sure of the purpose of the return value.
  // Also rewrite this function as utility that uses the
  boolean tryCreatePhysicalDataset(NamespaceKey datasetPath, DatasetConfig config) throws NamespaceException;

  // SEARCH
  Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> find(FindByCondition condition);

  String dump();

  String dumpSplits();

  /**
   * Search for splits for given condition.
   */
  Iterable<Map.Entry<DatasetSplitId, DatasetSplit>> findSplits(FindByCondition condition);

  Iterable<Map.Entry<DatasetSplitId, DatasetSplit>> findSplits(FindByRange<DatasetSplitId> range);

  /**
   * Count total number of splits for given condition
   * @param condition
   * @return
   */
  int getSplitCount(FindByCondition condition);

  /**
   * Delete given splits
   * @param datasetSplits list of split ids to be removed.
   */
  void deleteSplits(Iterable<DatasetSplitId> datasetSplits);

  /**
   * finds a dataset using UUID
   * @param uuid
   * @return a dataset, or null if not found.
   */
  DatasetConfig findDatasetByUUID(String uuid);

}
