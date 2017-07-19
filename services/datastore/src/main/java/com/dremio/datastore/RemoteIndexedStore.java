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
package com.dremio.datastore;

import static java.lang.String.format;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;

import com.dremio.datastore.SearchTypes.SearchQuery;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

/**
 * Remote indexed store.
 */
public class RemoteIndexedStore<K, V> extends RemoteKVStore<K, V> implements IndexedStore<K, V> {

  public RemoteIndexedStore(DatastoreRpcClient client, String storeId, StoreBuilderConfig config) {
    super(client, storeId, config);
  }

  @Override
  public Iterable<Entry<K, V>> find(FindByCondition find) {
    try {
      return Iterables.transform(getClient().find(getStoreId(), find), new Function<Entry<ByteString, ByteString>, Entry<K, V>>() {
        @Override
        public Entry<K, V> apply(Entry<ByteString, ByteString> input) {
          return new AbstractMap.SimpleEntry<>(
            getKeySerializer().deserialize(input.getKey().toByteArray()),
            getValueSerializer().deserialize(input.getValue().toByteArray()));
        }
      });
    } catch (IOException e) {
      throw new DatastoreException(format("Failed to search on store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  @Override
  public List<Integer> getCounts(SearchQuery... conditions) {
    try {
      return getClient().getCounts(getStoreId(), conditions);
    } catch (IOException e) {
      throw new DatastoreException(format("Failed to get counts on store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }
}
