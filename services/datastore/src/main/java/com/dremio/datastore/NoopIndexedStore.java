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
package com.dremio.datastore;

import java.util.List;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.IndexedStore;

/**
 * Noop indexed store.
 */
public class NoopIndexedStore<K, V> extends NoopKVStore<K, V> implements IndexedStore<K, V> {
  private final Integer version;

  public NoopIndexedStore() {
    super();
    this.version = 0;
  }

  public NoopIndexedStore(StoreBuilderHelper helper) {
    super(helper);
    this.version = helper.getVersion();
  }

  @Override
  public Iterable<Document<K, V>> find(FindByCondition find, FindOption... options) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public List<Integer> getCounts(SearchQuery... conditions) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public Integer version() {
    return version;
  }
}
