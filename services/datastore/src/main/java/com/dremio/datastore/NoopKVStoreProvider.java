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
package com.dremio.datastore;

import java.util.Map;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Noop KVStoreProvider for Executor nodes.
 */
public class NoopKVStoreProvider implements KVStoreProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NoopKVStoreProvider.class);

  private final ScanResult scan;
  private ImmutableMap<Class<? extends StoreCreationFunction<?>>, KVStore<?, ?>> stores;

  public NoopKVStoreProvider(
    ScanResult scan,
    Provider<FabricService> fabricService,
    Provider<NodeEndpoint> masterNode,
    BufferAllocator allocator,
    Map<String, Object> config
  ) {
    this.scan = scan;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends KVStore<?, ?>> T getStore(Class<? extends StoreCreationFunction<T>> creator) {
    return (T) Preconditions.checkNotNull(stores.get(creator), "Unknown store creator %s", creator.getName());
  }

  @VisibleForTesting
  <K, V> StoreBuilder<K, V> newStore(){
    return new ExecutorStoreBuilder<>();
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting NoopKVStoreProvider");
    stores = StoreLoader.buildStores(scan, new StoreBuildingFactory() {

      @Override
      public <K, V> StoreBuilder<K, V> newStore() {
        return NoopKVStoreProvider.this.newStore();
      }
    });

    logger.info("NoopKVStoreProvider is up");
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopped NoopKVStoreProvider");
  }

  /**
   * Store builder for noop kvstore provider.
   * @param <K>
   * @param <V>
   */
  public class ExecutorStoreBuilder<K, V> implements StoreBuilder<K, V> {
    @Override
    public StoreBuilder<K, V> name(String name) {
      return this;
    }

    @Override
    public StoreBuilder<K, V> keySerializer(Class<? extends Serializer<K>> keySerializerClass) {
      return this;
    }

    @Override
    public StoreBuilder<K, V> valueSerializer(Class<? extends Serializer<V>> valueSerializerClass) {
      return this;
    }

    @Override
    public StoreBuilder<K, V> versionExtractor(Class<? extends VersionExtractor<V>> versionExtractorClass) {
      return this;
    }

    @Override
    public KVStore<K, V> build() {
      return new NoopKVStore<>();
    }

    @Override
    public IndexedStore<K, V> buildIndexed(Class<? extends DocumentConverter<K, V>> documentConverterClass) {
      return new NoopIndexedStore<>();
    }
  }

}
