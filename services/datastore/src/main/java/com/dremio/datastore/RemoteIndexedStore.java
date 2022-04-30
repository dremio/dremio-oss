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

import static java.lang.String.format;

import java.io.IOException;
import java.util.List;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.IndexedStore;
import com.google.common.collect.Iterables;

/**
 * Remote indexed store.
 */
public class RemoteIndexedStore<K, V> extends RemoteKVStore<K, V> implements IndexedStore<K, V> {
  private final Integer version;

  public RemoteIndexedStore(DatastoreRpcClient client, String storeId, StoreBuilderHelper<K, V> helper) {
    super(client, storeId, helper);
    this.version = helper.getVersion();
  }

  @Override
  public Iterable<Document<K, V>> find(FindByCondition find, FindOption ... options) {
    try {
      return Iterables.transform(getClient().find(getStoreId(), find), this::convertDocument);
    } catch (IOException e) {
      throw new DatastoreException(format("Failed to search on store id: %s", getStoreId()), e);
    }
  }

  @Override
  public List<Integer> getCounts(SearchQuery... conditions) {
    try {
      return getClient().getCounts(getStoreId(), conditions);
    } catch (IOException e) {
      throw new DatastoreException(format("Failed to get counts on store id: %s", getStoreId()), e);
    }
  }

  @Override
  public Integer version() {
    return version;
  }
}
