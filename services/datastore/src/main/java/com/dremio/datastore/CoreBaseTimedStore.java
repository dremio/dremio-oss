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

import static com.dremio.common.perf.Timer.time;

import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.IncrementCounter;
import java.util.List;
import java.util.Map;

/** Adds timing instrumentation to KVStore interface */
abstract class CoreBaseTimedStore<K, V> implements CoreKVStore<K, V> {
  private final String name;
  private final CoreKVStore<K, V> kvStore;

  CoreBaseTimedStore(String name, CoreKVStore<K, V> kvStore) {
    this.name = name;
    this.kvStore = kvStore;
  }

  protected CoreKVStore<K, V> getStore() {
    return kvStore;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Document<KVStoreTuple<K>, KVStoreTuple<V>> get(KVStoreTuple<K> key, GetOption... options) {
    try (TimedBlock b = time(name + ".get")) {
      return kvStore.get(key, options);
    }
  }

  @Override
  public KVAdmin getAdmin() {
    return kvStore.getAdmin();
  }

  @Override
  public Iterable<Document<KVStoreTuple<K>, KVStoreTuple<V>>> find(
      FindByRange<KVStoreTuple<K>> range, FindOption... options) {
    try (TimedBlock b = time(getName() + ".find(FindByRange)")) {
      return kvStore.find(range, options);
    }
  }

  @Override
  public Iterable<Document<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindOption... options) {
    try (TimedBlock b = time(name + ".find()")) {
      return kvStore.find(options);
    }
  }

  @Override
  public Document<KVStoreTuple<K>, KVStoreTuple<V>> put(
      KVStoreTuple<K> key, KVStoreTuple<V> v, PutOption... options) {
    try (TimedBlock b = time(name + ".put")) {
      return kvStore.put(key, v, options);
    }
  }

  @Override
  public boolean contains(KVStoreTuple<K> key, ContainsOption... options) {
    try (TimedBlock b = time(name + ".contains")) {
      return kvStore.contains(key, options);
    }
  }

  @Override
  public void delete(KVStoreTuple<K> key, DeleteOption... options) {
    try (TimedBlock b = time(name + ".delete")) {
      kvStore.delete(key, options);
    }
  }

  @Override
  public Iterable<Document<KVStoreTuple<K>, KVStoreTuple<V>>> get(
      List<KVStoreTuple<K>> keys, GetOption... options) {
    try (TimedBlock b = time(name + ".get(List)")) {
      return kvStore.get(keys, options);
    }
  }

  @Override
  public void bulkIncrement(
      Map<KVStoreTuple<K>, List<IncrementCounter>> keysToIncrement, IncrementOption option) {
    try (TimedBlock b = time(name + ".bulkIncrement")) {
      kvStore.bulkIncrement(keysToIncrement, option);
    }
  }

  @Override
  public void bulkDelete(List<KVStoreTuple<K>> keysToDelete, DeleteOption... deleteOptions) {
    try (TimedBlock b = time(name + ".bulkDelete")) {
      kvStore.bulkDelete(keysToDelete, deleteOptions);
    }
  }

  /**
   * Basic timed store.
   *
   * @param <KEY>
   * @param <VALUE>
   */
  public static class TimedStoreImplCore<KEY, VALUE> extends CoreBaseTimedStore<KEY, VALUE> {
    public TimedStoreImplCore(String name, CoreKVStore<KEY, VALUE> kvStore) {
      super(name, kvStore);
    }
  }

  /**
   * An indexed and versioned KVStore that handles concurrent udpates.
   *
   * @param <KEY>
   * @param <VALUE>
   */
  public static class TimedIndexedStoreImplCore<KEY, VALUE> extends CoreBaseTimedStore<KEY, VALUE>
      implements CoreIndexedStore<KEY, VALUE> {

    private final CoreIndexedStore<KEY, VALUE> kvStore;

    public TimedIndexedStoreImplCore(String name, CoreIndexedStore<KEY, VALUE> kvStore) {
      super(name, kvStore);
      this.kvStore = kvStore;
    }

    @Override
    public Iterable<Document<KVStoreTuple<KEY>, KVStoreTuple<VALUE>>> find(
        FindByCondition find, FindOption... options) {
      try (TimedBlock b = time(getName() + ".find(FindByCondition)")) {
        return kvStore.find(find, options);
      }
    }

    @Override
    public List<Integer> getCounts(SearchQuery... conditions) {
      try (TimedBlock b = time(getName() + ".getCounts")) {
        return kvStore.getCounts(conditions);
      }
    }

    @Override
    public int reindex() {
      return kvStore.reindex();
    }

    @Override
    public Integer version() {
      return kvStore.version();
    }
  }

  @Override
  public KVStoreTuple<K> newKey() {
    return kvStore.newKey();
  }

  @Override
  public KVStoreTuple<V> newValue() {
    return kvStore.newValue();
  }
}
