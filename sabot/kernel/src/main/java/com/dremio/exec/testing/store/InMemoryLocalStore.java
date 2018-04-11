/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.testing.store;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.dremio.exec.store.sys.PersistentStore;
import com.google.common.collect.Maps;

public class InMemoryLocalStore<V> implements PersistentStore<V> {
  private final ConcurrentMap<String, V> store = Maps.newConcurrentMap();

  public void delete(final String key) {
    store.remove(key);
  }

  @Override
  public V get(final String key) {
    return store.get(key);
  }

  @Override
  public void put(final String key, final V value) {
    store.put(key, value);
  }

  @Override
  public boolean putIfAbsent(final String key, final V value) {
    final V old = store.putIfAbsent(key, value);
    return value != old;
  }

  @Override
  public Iterator<Map.Entry<String, V>> getAll() {
    return store.entrySet().iterator();
  }

  @Override
  public void close() throws Exception {
    store.clear();
  }
}
