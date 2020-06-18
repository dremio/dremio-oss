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

import com.dremio.datastore.TracingKVStore.TracingIndexedStore;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.datastore.format.Format;

import io.opentracing.Tracer;

/**
 * Provider of KVStores. Wraps all stores in a tracing decorator.
 */
public class TracingKVStoreProvider implements KVStoreProvider {


  private final KVStoreProvider kvProvider;
  private final Tracer tracer;

  public TracingKVStoreProvider(KVStoreProvider delegate, Tracer tracer) {
    this.kvProvider = delegate;
    this.tracer = tracer;
  }

  @Override
  public <K, V> StoreBuilder<K, V> newStore() {
    return new TracingStoreBuilder<>(kvProvider.newStore(), tracer);
  }

  /**
   * TracingKVStoreProvider's implementation of the StoreBuilder class.
   * TracingStoreBuilder provides the underlying StoreBuilder, tracer and
   * basic configurations along side build and buildIndexed methods
   * for creating TracingKVStores and TracingIndexedStores.
   *
   * @param <K> key type K.
   * @param <V> value type V.
   */
  private static class TracingStoreBuilder<K, V> implements StoreBuilder<K, V> {

    private final StoreBuilder<K, V> delegate;
    private final Tracer tracer;
    private String name;

    TracingStoreBuilder(StoreBuilder<K, V> delegate, Tracer tracer) {
      this.delegate = delegate;
      this.tracer = tracer;
    }

    @Override
    public StoreBuilder<K, V> name(String name) {
      this.name = name;
      delegate.name(name);
      return this;
    }

    @Override
    public StoreBuilder<K, V> keyFormat(Format<K> format) {
      delegate.keyFormat(format);
      return this;
    }

    @Override
    public StoreBuilder<K, V> valueFormat(Format<V> format) {
      delegate.valueFormat(format);
      return this;
    }

    @Override
    public StoreBuilder<K, V> permitCompoundKeys(boolean permitCompoundKeys) {
      delegate.permitCompoundKeys(permitCompoundKeys);
      return this;
    }

    @Override
    public KVStore<K, V> build() {
      return TracingKVStore.of(name, tracer, delegate.build());
    }


    @Override
    public IndexedStore<K, V> buildIndexed(DocumentConverter<K, V> documentConverter) {
      return TracingIndexedStore.of(name, tracer, delegate.buildIndexed(documentConverter));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V, T extends KVStore<K, V>> T getStore(Class<? extends StoreCreationFunction<K, V, T>> creator) {
    final KVStore<?, ?> ret = kvProvider.getStore(creator);

    if (ret instanceof IndexedStore) {
      return (T) TracingIndexedStore.of(creator.getName(), tracer, (IndexedStore) ret);
    }

    return (T) TracingKVStore.of(creator.getName(), tracer, ret);
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
  }

  @Override
  public void close() throws Exception {
    kvProvider.close();
  }
}
