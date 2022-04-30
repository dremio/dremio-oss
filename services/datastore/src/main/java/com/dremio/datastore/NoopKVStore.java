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

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.KVStore;

/**
 * Noop KVStore, shouldn't ever be accessed.
 */
public class NoopKVStore<K, V> implements KVStore<K, V> {
  private static final String NOOP_NAME = "NoopKVStore";
  private final String name;

  @SuppressWarnings("unchecked")
  public NoopKVStore() {
    this.name = NOOP_NAME;
  }

  @SuppressWarnings("unchecked")
  public NoopKVStore(StoreBuilderHelper helper) {
    this.name = helper.getName();
  }

  @Override
  public Document<K, V> get(K key, GetOption ... options) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public KVAdmin getAdmin() {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public List<Document<K, V>> get(List<K> keys, GetOption ... options) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public Document<K, V> put(K key, V value, PutOption ... options) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public boolean contains(K key, ContainsOption ... options) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public void delete(K key, DeleteOption ... options) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public Iterable<Document<K, V>> find(FindByRange<K> find, FindOption ... options) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public Iterable<Document<K, V>> find(FindOption ... options) {
    throw new UnsupportedOperationException("Operation unsupported on this type of node.");
  }

  @Override
  public String getName() {
    return name;
  }

}
