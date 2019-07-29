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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.SearchTypes.SearchQuery;

/**
 * Adds timing instrumentation to KVStore interface
 */
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

  protected String getName() {
    return name;
  }

  @Override
  public KVStoreTuple<V> get(KVStoreTuple<K> key) {
    try (TimedBlock b = time(name + ".get")) {
      return kvStore.get(key);
    }
  }

  @Override
  public KVAdmin getAdmin() {
    return kvStore.getAdmin();
  }

  @Override
  public Iterable<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindByRange<KVStoreTuple<K>> range) {
    try (TimedBlock b = time(getName() + ".find(FindByRange)")) {
      return kvStore.find(range);
    }
  }

  @Override
  public Iterable<Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>>> find() {
    try (TimedBlock b = time(name + ".find()")) {
      return kvStore.find();
    }
  }

  @Override
  public void put(KVStoreTuple<K> key, KVStoreTuple<V> v) {
    try (TimedBlock b = time(name + ".put")) {
      kvStore.put(key, v);
    }
  }

  @Override
  public boolean contains(KVStoreTuple<K> key) {
    try (TimedBlock b = time(name + ".contains")) {
      return kvStore.contains(key);
    }
  }

  @Override
  public void delete(KVStoreTuple<K> key) {
    try (TimedBlock b = time(name + ".delete")) {
      kvStore.delete(key);
    }
  }

  @Override
  public void delete(KVStoreTuple<K> key, String previousVersion) {
    try (TimedBlock b = time(name + ".delete(K, long)")) {
      kvStore.delete(key, previousVersion);
    }
  }

  @Override
  public List<KVStoreTuple<V>> get(List<KVStoreTuple<K>> keys) {
    try (TimedBlock b = time(name + ".get(List)")) {
      return kvStore.get(keys);
    }
  }

  @Override
  public boolean validateAndPut(KVStoreTuple<K> key, KVStoreTuple<V> newValue, ValueValidator<V> validator) {
    try (TimedBlock b = time(name + ".validateAndPut")) {
      return kvStore.validateAndPut(key, newValue, validator);
    }
  }

  @Override
  public boolean validateAndDelete(KVStoreTuple<K> key, ValueValidator<V> validator) {
    try (TimedBlock b = time(name + ".validateAndPut")) {
      return kvStore.validateAndDelete(key, validator);
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
  *
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
    public Iterable<Entry<KVStoreTuple<KEY>, KVStoreTuple<VALUE>>> find(FindByCondition find) {
      try (TimedBlock b = time(getName() + ".find(FindByCondition)")) {
        return kvStore.find(find);
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
