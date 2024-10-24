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

import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.ImmutableDocument;
import com.dremio.datastore.api.IncrementCounter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import io.protostuff.LinkedBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Caching namespace KV-store.
 *
 * <p>Caches get(), put(). The cache is not automatically invalidated. To avoid stale cache records,
 * invalidateCache() need be called by the client to remove a record from the cache.
 */
public class CachingNamespaceStore implements IndexedStore<String, NameSpaceContainer> {

  private final IndexedStore<String, NameSpaceContainer> underlyingIndexedStore;

  private final Map<String, Document<String, NameSpaceContainer>> cache;

  public CachingNamespaceStore(IndexedStore<String, NameSpaceContainer> underlyingIndexedStore) {
    this.underlyingIndexedStore = underlyingIndexedStore;
    this.cache = new HashMap<>();
  }

  @Override
  public Iterable<Document<String, NameSpaceContainer>> find(
      FindByCondition find, FindOption... options) {
    return underlyingIndexedStore.find(find, options);
  }

  @Override
  public List<Integer> getCounts(SearchTypes.SearchQuery... conditions) {
    return underlyingIndexedStore.getCounts(conditions);
  }

  @Override
  public Integer version() {
    return underlyingIndexedStore.version();
  }

  @Override
  public Document<String, NameSpaceContainer> get(String key, GetOption... options) {
    Document<String, NameSpaceContainer> cached = cache.get(key);

    if (cached != null) {
      return cloneNameSpaceContainer(cached);
    } else {
      Document<String, NameSpaceContainer> result = underlyingIndexedStore.get(key, options);
      putToCacheIfNotNull(result);
      return cloneNameSpaceContainer(result);
    }
  }

  @Override
  public List<Document<String, NameSpaceContainer>> get(List<String> keys, GetOption... options) {
    List<String> keysToGet = new ArrayList<>();

    // Find not cached keys
    for (String key : keys) {
      if (!cache.containsKey(key)) {
        keysToGet.add(key);
      }
    }

    // Get from KV Store if not found in cache
    if (!keysToGet.isEmpty()) {
      underlyingIndexedStore.get(keysToGet, options).forEach(this::putToCacheIfNotNull);
    }

    // Assemble result
    List<Document<String, NameSpaceContainer>> result = new ArrayList<>(keys.size());
    for (String key : keys) {
      result.add(cloneNameSpaceContainer(cache.get(key)));
    }

    return result;
  }

  @Override
  public Document<String, NameSpaceContainer> put(
      String key, NameSpaceContainer container, PutOption... options) {
    Document<String, NameSpaceContainer> document = underlyingIndexedStore.put(key, container);
    putToCacheIfNotNull(document);
    return document;
  }

  @Override
  public boolean contains(String key, ContainsOption... options) {
    if (cache.containsKey(key)) {
      return true;
    }

    return underlyingIndexedStore.contains(key, options);
  }

  @Override
  public void delete(String key, DeleteOption... options) {
    underlyingIndexedStore.delete(key, options);
    cache.remove(key);
  }

  @Override
  public Iterable<Document<String, NameSpaceContainer>> find(
      FindByRange<String> find, FindOption... options) {
    return underlyingIndexedStore.find(find, options);
  }

  @Override
  public void bulkIncrement(
      Map<String, List<IncrementCounter>> keysToIncrement, IncrementOption option) {
    underlyingIndexedStore.bulkIncrement(keysToIncrement, option);
  }

  @Override
  public void bulkDelete(List<String> keysToDelete, DeleteOption... deleteOptions) {
    keysToDelete.forEach(cache::remove);
    underlyingIndexedStore.bulkDelete(keysToDelete, deleteOptions);
  }

  @Override
  public Iterable<Document<String, NameSpaceContainer>> find(FindOption... options) {
    return underlyingIndexedStore.find(options);
  }

  @Override
  public KVAdmin getAdmin() {
    return underlyingIndexedStore.getAdmin();
  }

  @Override
  public String getName() {
    return underlyingIndexedStore.getName();
  }

  private Document<String, NameSpaceContainer> cloneNameSpaceContainer(
      Document<String, NameSpaceContainer> document) {
    if (document == null) {
      return null;
    }

    return new ImmutableDocument.Builder<String, NameSpaceContainer>()
        .setKey(document.getKey())
        .setValue(NameSpaceContainer.from(document.getValue().clone(LinkedBuffer.allocate())))
        .setTag(document.getTag())
        .build();
  }

  private void putToCacheIfNotNull(Document<String, NameSpaceContainer> document) {
    if (document != null) {
      cache.put(document.getKey(), document);
    }
  }

  protected void invalidateCache(final String key) {
    cache.remove(key);
  }
}
