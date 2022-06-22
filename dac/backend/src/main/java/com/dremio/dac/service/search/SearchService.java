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
package com.dremio.dac.service.search;

import java.util.Collections;
import java.util.List;

import com.dremio.service.Service;
import com.dremio.service.namespace.NamespaceException;

/**
 * Search service
 */
public interface SearchService extends Service {
  /**
   * Performs a dataset search using the provided query string
   *
   * @param query search query
   * @param username username
   * @return list of search results
   * @throws NamespaceException
   */
  List<SearchContainer> search(String query, String username) throws NamespaceException;

  /**
   * Wakes up the search manager manually, which will cause any modified entities to be reindexed for search
   *
   * @param reason why the manager was woken up
   */
  void wakeupManager(String reason);

  SearchService UNSUPPORTED = new SearchService() {
    @Override
    public void close() throws Exception {
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public List<SearchContainer> search(String query, String username) throws NamespaceException {
      throw new UnsupportedOperationException("non-master coordinators or executors do not support search");
    }

    @Override
    public void wakeupManager(String reason) {
      throw new UnsupportedOperationException("non-master coordinators or executors do not support Search");
    }
  };

  SearchService NOOP = new SearchService() {
    @Override
    public void close() throws Exception {
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public List<SearchContainer> search(String query, String username) throws NamespaceException {
      return Collections.emptyList();
    }

    @Override
    public void wakeupManager(String reason) {
    }
  };
}
