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
import java.util.Map.Entry;

/**
 * Noop KVStore, shouldn't ever be accessed.
 */
public class NoopKVStore<K, V> implements KVStore<K, V> {

  @SuppressWarnings("unchecked")
  public NoopKVStore() {

  }

  @Override
  public V get(K key) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public KVAdmin getAdmin() {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public List<V> get(List<K> keys) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public void put(K key, V value) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public boolean contains(K key) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public void delete(K key) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public Iterable<Entry<K, V>> find(FindByRange<K> find) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public Iterable<Entry<K, V>> find() {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public void delete(K key, String previousVersion) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

}
