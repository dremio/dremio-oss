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
package com.dremio.datastore.adapter;

import java.util.List;
import java.util.Map;

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore;

/**
 * Bridges LegacyIndexedStore to new IndexedStore API.
 *
 * @param <K> key of type K.
 * @param <V> value of type V.
 */
public class LegacyIndexedStoreAdapter<K, V> extends LegacyKVStoreAdapter<K, V> implements LegacyIndexedStore<K,V> {
  private IndexedStore<K, V> underlyingStore;

  public LegacyIndexedStoreAdapter(IndexedStore<K, V> underlyingStore, VersionExtractor<V> versionExtractor) {
    super(underlyingStore, versionExtractor);
    this.underlyingStore = underlyingStore;
  }

  @Override
  public Iterable<Map.Entry<K, V>> find(LegacyFindByCondition find) {
    FindByCondition findByCondition = new ImmutableFindByCondition.Builder()
      .setPageSize(find.getPageSize())
      .setOffset(find.getOffset())
      .setLimit(find.getLimit())
      .setCondition(find.getCondition())
      .setSort(find.getSort()).build();
    return convertToMapEntry(underlyingStore.find(findByCondition));
  }

  @Override
  public List<Integer> getCounts(SearchTypes.SearchQuery... conditions) {
    return underlyingStore.getCounts(conditions);
  }
}
