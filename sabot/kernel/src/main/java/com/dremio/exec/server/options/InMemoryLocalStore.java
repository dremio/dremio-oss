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
package com.dremio.exec.server.options;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.api.LegacyKVStore;
import com.google.common.collect.Maps;

class InMemoryLocalStore<V> implements LegacyKVStore<String, V> {
  private final ConcurrentMap<String, V> store = Maps.newConcurrentMap();

  @Override
  public V get(String key) {
    return store.get(key);
  }

  @Override
  public List<V> get(List<String> keys) {
    return keys.stream()
      .map(store::get)
      .collect(Collectors.toList());
  }

  @Override
  public void put(String key, V value) {
    store.put(key, value);
  }

  @Override
  public boolean contains(String key) {
    return store.containsKey(key);
  }

  @Override
  public void delete(String key) {
    store.remove(key);
  }

  @Override
  public Iterable<Map.Entry<String, V>> find() {
    return store.entrySet();
  }

  @Override
  public void delete(String key, String previousVersion) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<Map.Entry<String, V>> find(LegacyFindByRange<String> find) {
    throw new UnsupportedOperationException();
  }

  @Override
  public KVAdmin getAdmin() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException();
  }
}
