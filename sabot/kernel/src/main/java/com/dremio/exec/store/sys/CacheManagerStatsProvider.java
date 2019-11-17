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
package com.dremio.exec.store.sys;

import java.util.List;

import org.rocksdb.RocksIterator;

import com.dremio.exec.work.CacheManagerDatasetInfo;
import com.dremio.exec.work.CacheManagerFilesInfo;
import com.dremio.exec.work.CacheManagerMountPointInfo;
import com.dremio.exec.work.CacheManagerStoragePluginInfo;
/**
 * This interface provides way to get the data for the below system tables.
 * sys."cache_manager_mount_points", sys."cache_manager_storage_plugins",
 * sys."cache_manager_datasets" and sys."cache_manager_files"
 *
 * Due to the lazy instantiation of cache manager, the handle to cache manager instance is
 * buried in the CacheFileSystemCreator object. Sabot context does not have handle to the cache manager
 * instance but has the handle to file system creator. At the system table iterator creation,
 * we have handle only to the sabot context. So need to have the below interface implemented by
 * file system creator and get the cache manager statistics.
 */
public interface CacheManagerStatsProvider {
  /**
   * Retrieves the statistics about the mount points configured for the executor node.
   * @return
   */
  List<CacheManagerMountPointInfo> getMountPointStats();

  /**
   * Retrieves the statistics about the storage plugins cached by cache manager.
   * @return
   */
  List<CacheManagerStoragePluginInfo> getStoragePluginStats();

  /**
   * Gets the iterator for the datasets information in the cache manager.
   * @return
   */
  RocksIterator getDatasetIterator();

  /**
   * Retrieves the statistics about the datasets cached in the executor node.
   * @param dsIterator
   * @return
   */
  List<CacheManagerDatasetInfo> getDatasetStats(RocksIterator dsIterator);

  /**
   * Gets the iterator for the cached files in the executor node.
   * @return
   */
  RocksIterator getCachedFilesIterator();

  /**
   * Retrives the statistics about the cached files in the executor node.
   * @param fileIterator
   * @return
   */
  List<CacheManagerFilesInfo> getCachedFilesStats(RocksIterator fileIterator);
}
