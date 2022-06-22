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


import java.util.Set;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.utility.StoreLoader;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Provides a TimedKVStore provider, that wraps the underlying provider &
 * measures timing of ops.
 */
public class TimedKVStoreProvider implements KVStoreProvider {

  private final KVStoreProvider kvProvider;
  private ImmutableMap<Class<? extends StoreCreationFunction<?, ?, ?>>, KVStore<?, ?>> stores;
  private ScanResult scan;

  public TimedKVStoreProvider(KVStoreProvider delegate, ScanResult scan) {
    this.kvProvider = delegate;
    this.scan = scan;
  }

  @Override
  public <K, V> KVStoreProvider.StoreBuilder<K, V> newStore() {
    return new TimedKVStoreProvider.TimedStoreBuilder<>(kvProvider.newStore());
  }

  /**
   * TimedKVStoreProvider's implementation of the StoreBuilder class.
   * TimedStoreBuilder provides the underlying StoreBuilder,
   * for creating TimedStores
   *
   * @param <K> key type K.
   * @param <V> value type V.
   */
  private static class TimedStoreBuilder<K, V> implements KVStoreProvider.StoreBuilder<K, V> {

    private final KVStoreProvider.StoreBuilder<K, V> delegate;

    TimedStoreBuilder(KVStoreProvider.StoreBuilder<K, V> delegate) {
      this.delegate = delegate;
    }

    @Override
    public KVStoreProvider.StoreBuilder<K, V> name(String name) {
      delegate.name(name);
      return this;
    }

    @Override
    public KVStoreProvider.StoreBuilder<K, V> keyFormat(Format<K> format) {
      delegate.keyFormat(format);
      return this;
    }

    @Override
    public KVStoreProvider.StoreBuilder<K, V> valueFormat(Format<V> format) {
      delegate.valueFormat(format);
      return this;
    }

    @Override
    public KVStoreProvider.StoreBuilder<K, V> permitCompoundKeys(boolean permitCompoundKeys) {
      delegate.permitCompoundKeys(permitCompoundKeys);
      return this;
    }

    @Override
    public KVStore<K, V> build() {
      return TimedKVStore.of(delegate.build());
    }


    @Override
    public IndexedStore<K, V> buildIndexed(DocumentConverter<K, V> documentConverter) {
      return TimedKVStore.TimedIndexedStore.of(delegate.buildIndexed(documentConverter));
    }
  }

  @Override
  public Set<KVStore<?, ?>> stores() {
    return new ImmutableSet.Builder<KVStore<?,?>>().addAll(stores.values().iterator()).build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V, T extends KVStore<K, V>> T getStore(Class<? extends StoreCreationFunction<K, V, T>> creator) {
    return (T) Preconditions.checkNotNull(stores.get(creator), "Unknown store creator %s",
      creator.getName());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(kvProvider)) {
      return (T) kvProvider;
    }
    return kvProvider.unwrap(clazz);
  }

  @Override
  public void start() throws Exception {
    kvProvider.start();
    stores = StoreLoader.buildStores(scan, TimedKVStoreProvider.this::newStore);
  }

  @Override
  public void close() throws Exception {
    kvProvider.close();
  }
}
