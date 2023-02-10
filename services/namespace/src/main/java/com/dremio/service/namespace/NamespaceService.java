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
package com.dremio.service.namespace;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyKVStore.LegacyFindByRange;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;

/**
 * Namespace operations from DAC
 */
@Options
public interface NamespaceService {

  /**
   * Compression of (multi)splits in the K/V store
   */
  enum SplitCompression {
    UNCOMPRESSED,  // splits stored uncompressed
    SNAPPY         // splits stored using snappy compression
  }

  // never expire  = Monday, September 1, 3017 9:38:18 PM
  long INFINITE_REFRESH_PERIOD = 33061210698000L ;

  TypeValidators.BooleanValidator DATASET_METADATA_CONSISTENCY_VALIDATE = new TypeValidators.BooleanValidator("store.dataset.metadata_consistency.validate", false);

  /**
   * Factory to create namespace service for a given user
   */
  interface Factory {
    /**
     * Return a namespace service for a given user. Note that this is for usernames
     * and users only, if roles are to be supported, use #get(NamespaceIdentity) instead.
     *
     * @param userName a valid user name
     * @return a namespace service instance
     * @throws NullPointerException if {@code userName} is null
     * @throws IllegalArgumentException if {@code userName} is invalid
     */
    NamespaceService get(String userName);

    NamespaceService get(NamespaceIdentity identity);
  }

  //// Create
  void addOrUpdateSource(NamespaceKey sourcePath, SourceConfig sourceConfig, NamespaceAttribute... attributes) throws NamespaceException;

  void addOrUpdateSpace(NamespaceKey spacePath, SpaceConfig spaceConfig, NamespaceAttribute... attributes) throws NamespaceException;

  void addOrUpdateFunction(NamespaceKey udfPath, FunctionConfig udfConfig, NamespaceAttribute... attributes) throws NamespaceException;

  void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset, NamespaceAttribute... attributes) throws NamespaceException;

  void addOrUpdateFolder(NamespaceKey folderPath, FolderConfig folderConfig, NamespaceAttribute... attributes) throws NamespaceException;

  void addOrUpdateHome(NamespaceKey homePath, HomeConfig homeConfig) throws NamespaceException;

  /**
   * Create a dataset metadata saver for the given dataset.
   * @param datasetPath              dataset path
   * @param datasetId                dataset id
   * @param splitCompression         compression to be used on the (multi-)splits in the K/V store
   * @param maxSinglePartitionChunks maximum number of single split partition chunks allowed to be saved together
   * @return                         dataset metadata saver
   */
  DatasetMetadataSaver newDatasetMetadataSaver(NamespaceKey datasetPath, EntityId datasetId, SplitCompression splitCompression, long maxSinglePartitionChunks, boolean datasetMetadataConsistencyValidate);

  //// GET
  boolean exists(NamespaceKey key, Type type);
  boolean exists(NamespaceKey key);

  boolean hasChildren(NamespaceKey key);

  SourceConfig getSource(NamespaceKey sourcePath) throws NamespaceException;

  SourceConfig getSourceById(String id) throws NamespaceException;

  SpaceConfig getSpace(NamespaceKey spacePath) throws NamespaceException;

  FunctionConfig getFunction(NamespaceKey functionPath) throws NamespaceException;

  SpaceConfig getSpaceById(String id) throws NamespaceException;

  NameSpaceContainer getEntityById(String id) throws NamespaceNotFoundException;

  List<NameSpaceContainer> getEntitiesByIds(List<String> ids) throws NamespaceNotFoundException;

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
   * @throws NamespaceNotFoundException
   */
  List<NameSpaceContainer> getEntities(List<NamespaceKey> lookupKeys) throws NamespaceNotFoundException;

  List<SpaceConfig> getSpaces();

  List<FunctionConfig> getFunctions();

  List<HomeConfig> getHomeSpaces();

  List<SourceConfig> getSources();

  List<DatasetConfig> getDatasets();

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
  //// Note: use sparingly!
  Iterable<NamespaceKey> getAllDatasets(final NamespaceKey parent) throws NamespaceException;

  int getAllDatasetsCount(NamespaceKey path) throws NamespaceException;

  Iterable<NameSpaceContainer> getAllDescendants(final NamespaceKey root);

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

  //// DELETE
  void deleteSource(NamespaceKey sourcePath, String version) throws NamespaceException;

  /**
   * Callback for dataset deletion
   */
  @FunctionalInterface
  interface DeleteCallback {

    void onDatasetDelete(DatasetConfig datasetConfig);

  }
  /**
   * Delete a source and all of its children.
   * @param sourcePath
   * @param version
   * @throws NamespaceException
   */
  void deleteSourceWithCallBack(NamespaceKey sourcePath, String version, DeleteCallback callback) throws NamespaceException;

  /**
   * Delete all of a sources children but leave the source intact.
   * @param sourcePath
   * @param version
   * @param callback
   * @throws NamespaceException
   */
  void deleteSourceChildren(final NamespaceKey sourcePath, String version, DeleteCallback callback) throws NamespaceException;

  void deleteSpace(NamespaceKey spacePath, String version) throws NamespaceException;

  void deleteFunction(NamespaceKey udfPath) throws NamespaceException;

  void deleteEntity(NamespaceKey entityPath) throws NamespaceException;

  void deleteDataset(NamespaceKey datasetPath, String version, NamespaceAttribute... attributes) throws NamespaceException;

  void deleteFolder(NamespaceKey folderPath, String version) throws NamespaceException;

  void deleteHome(final NamespaceKey sourcePath, String version) throws NamespaceException;

  //// RENAME
  DatasetConfig renameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath) throws NamespaceException;

  // Create physical dataset if it doesn't exist.
  // TODO: DX-4493 No one is checking the return value. Not sure of the purpose of the return value.
  // Also rewrite this function as utility that uses the
  boolean tryCreatePhysicalDataset(NamespaceKey datasetPath, DatasetConfig config, NamespaceAttribute... attributes) throws NamespaceException;

  /**
   * Find entries by condition. If condition is not provided, returns all items.
   * @param condition
   * @return List of Key/Container entries.
   */
  Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> find(LegacyFindByCondition condition);

  String dump();

  String dumpSplits();

  /**
   * Search for splits for given condition.
   */
  Iterable<PartitionChunkMetadata> findSplits(LegacyFindByCondition condition);
  Iterable<PartitionChunkMetadata> findSplits(LegacyFindByRange<PartitionChunkId> range);

  /**
   * Count total number of partition chunks for a given condition
   * @param condition
   * @return
   */
  int getPartitionChunkCount(LegacyFindByCondition condition);

  /**
   * Delete any orphaned splits from the Namespace.
   *
   * NOTE: this cannot be run in parallel with any other metadata updates as that may cause
   * generation of split orphans while the dataset is initially getting setup.
   *
   * @param policy the expiration policy. Note: choosing an aggresive policy while running
   * other metadata updates or planning queries may cause generation of split orphans while the
   * dataset is initially getting setup, or query errors
   * @return The number of splits deleted.
   */
  int deleteSplitOrphans(PartitionChunkId.SplitOrphansRetentionPolicy policy, boolean datasetMetadataConsistencyValidate);

  /**
   * Delete given splits
   * @param datasetSplits list of split ids to be removed.
   */
  void deleteSplits(Iterable<PartitionChunkId> datasetSplits);

  /**
   * finds a dataset using UUID
   * @param uuid
   * @return a dataset, or null if not found.
   */
  DatasetConfig findDatasetByUUID(String uuid);


  /**
   * Checks if a sourceConfig can be saved.  Currently does concurrency checks for the config and any passed in attributes
   *
   * @param newConfig the new config we want to save
   * @param attributes
   */
  void canSourceConfigBeSaved(SourceConfig newConfig, SourceConfig existingConfig, NamespaceAttribute... attributes) throws ConcurrentModificationException, NamespaceException;

  /**
   * Returns entity id by path
   *
   * @param datasetPath
   * @return a data set entity id or null, if there is no dataset by provided path
   */
  String getEntityIdByPath(NamespaceKey datasetPath) throws NamespaceNotFoundException;

  /**
   * Returns an entity given its path.
   *
   * @param datasetPath namespace key
   * @return dataset associated with this path or null, if there is no dataset.
   */
  NameSpaceContainer getEntityByPath(NamespaceKey datasetPath) throws NamespaceException;
}
