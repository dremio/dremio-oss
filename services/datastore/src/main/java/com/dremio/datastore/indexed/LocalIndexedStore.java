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
package com.dremio.datastore.indexed;

import java.util.List;

import com.dremio.datastore.CoreIndexedStore;
import com.dremio.datastore.LocalKVStore;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.IndexedStore;
import com.google.common.collect.Iterables;

/**
 * Index store implementation (runs on master node).
 */
public class LocalIndexedStore<K, V> extends LocalKVStore<K, V> implements IndexedStore<K, V> {

  private final CoreIndexedStore<K, V> coreIndexedStore;

  public LocalIndexedStore(CoreIndexedStore<K, V> coreIndexedStore) {
    super(coreIndexedStore);
    this.coreIndexedStore = coreIndexedStore;
  }
  @Override
  public Iterable<Document<K, V>> find(FindByCondition find, FindOption... options) {
    return Iterables.transform(coreIndexedStore.find(find, options), this::fromDocument);
  }

  @Override
  public List<Integer> getCounts(SearchQuery... conditions) {
    return coreIndexedStore.getCounts(conditions);
  }
}
