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

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.work.CacheManagerDatasetInfo;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.rocksdb.RocksIterator;

/** Iterator which returns cache manager dataset information */
public class CacheManagerDatasetIterator implements Iterator<Object> {
  private final boolean isCachedFileSystem;
  private List<CacheManagerDatasetInfo> datasetInfoList = new ArrayList<>();
  private int curPos;
  private CacheManagerStatsProvider cacheManagerStatsProvider;
  private RocksIterator dsIterator;

  CacheManagerDatasetIterator(SabotContext sabotContext) {
    isCachedFileSystem =
        sabotContext.getFileSystemWrapper().isWrapperFor(CacheManagerStatsProvider.class);

    if (isCachedFileSystem) {
      cacheManagerStatsProvider =
          sabotContext.getFileSystemWrapper().unwrap(CacheManagerStatsProvider.class);
      dsIterator = cacheManagerStatsProvider.getDatasetIterator();
      if (dsIterator != null) {
        datasetInfoList = cacheManagerStatsProvider.getDatasetStats(dsIterator);
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (!isCachedFileSystem || dsIterator == null) {
      return false;
    }

    if (curPos == datasetInfoList.size()) {
      datasetInfoList = cacheManagerStatsProvider.getDatasetStats(dsIterator);
      if (datasetInfoList.isEmpty()) {
        dsIterator.close();
        return false;
      }
      curPos = 0;
    }
    return true;
  }

  @Override
  public Object next() {
    if (!isCachedFileSystem || dsIterator == null) {
      return null;
    }

    return datasetInfoList.get(curPos++);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
