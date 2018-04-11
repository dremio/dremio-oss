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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.CoreStoreProvider.CoreStoreBuilder;
import com.dremio.datastore.CoreStoreProviderImpl.StoreWithId;
import com.dremio.datastore.indexed.LocalIndexedStore;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcException;
import com.dremio.service.DirectProvider;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Datastore provider for master node.
 */
public class LocalKVStoreProvider implements KVStoreProvider, Iterable<CoreStoreProviderImpl.StoreWithId> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalKVStoreProvider.class);
  private final CoreStoreProviderImpl coreStoreProvider;
  private final Provider<FabricService> fabricService;
  private final BufferAllocator allocator;
  private final String hostName;
  private final ScanResult scan;
  private ImmutableMap<Class<? extends StoreCreationFunction<?>>, KVStore<?, ?>> stores;

  @VisibleForTesting
  public LocalKVStoreProvider(ScanResult scan, String baseDirectory, boolean inMemory, boolean timed) {
    this(scan, baseDirectory, inMemory, timed, true, false);
  }

  @VisibleForTesting
  public LocalKVStoreProvider(ScanResult scan, String baseDirectory, boolean inMemory, boolean timed, boolean validateOCC) {
    this(scan, null, null, null, baseDirectory, inMemory, timed, validateOCC, false);
  }

  @VisibleForTesting
  public LocalKVStoreProvider(ScanResult scan, String baseDirectory, boolean inMemory, boolean timed, boolean validateOCC, boolean disableOCC) {
    this(scan, null, null, null, baseDirectory, inMemory, timed, validateOCC, disableOCC);
  }

  public LocalKVStoreProvider(ScanResult scan, Provider<FabricService> fabricService, BufferAllocator allocator, String hostName,
                              String baseDirectory, boolean inMemory, boolean timed, boolean validateOCC, boolean disableOCC) {
    coreStoreProvider = new CoreStoreProviderImpl(baseDirectory, inMemory, timed, validateOCC, disableOCC);
    this.fabricService = fabricService;
    this.allocator = allocator;
    this.hostName = hostName;
    this.scan = scan;
  }

  @VisibleForTesting
  <K, V> StoreBuilder<K, V> newStore(){
    return new LocalStoreBuilder<>(coreStoreProvider.<K, V>newStore());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends KVStore<?, ?>> T getStore(Class<? extends StoreCreationFunction<T>> creator) {
    return (T) Preconditions.checkNotNull(stores.get(creator), "Unknown store creator %s", creator.getName());
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting LocalKVStoreProvider");
    coreStoreProvider.start();
    if (fabricService != null) {
      final DefaultDataStoreRpcHandler rpcHandler = new LocalDataStoreRpcHandler(hostName, coreStoreProvider);
      final NodeEndpoint thisNode = NodeEndpoint.newBuilder().setAddress(hostName).setFabricPort(fabricService.get().getPort()).build();
      try {
        new DatastoreRpcService(DirectProvider.wrap(thisNode), fabricService.get(), allocator, rpcHandler);
      } catch (RpcException e) {
        throw new DatastoreException("Failed to start rpc service", e);
      }
    }

    stores = StoreLoader.buildStores(scan, new StoreBuildingFactory() {

      @Override
      public <K, V> StoreBuilder<K, V> newStore() {
        return LocalKVStoreProvider.this.newStore();
      }
    });

    logger.info("LocalKVStoreProvider is up");
  }

  public void scan() throws Exception {
    coreStoreProvider.scan();
  }


  @Override
  public Iterator<StoreWithId> iterator() {
    return coreStoreProvider.iterator();
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping LocalKVStoreProvider");
    coreStoreProvider.close();
    logger.info("Stopped LocalKVStoreProvider");
  }

  public Map<StoreBuilderConfig, CoreKVStore<?, ?>> getStores() {
    return coreStoreProvider.getStores();
  }

  public CoreKVStore<?, ?> getOrCreateStore(StoreBuilderConfig config) {
    final String storeId = (coreStoreProvider).getOrCreateStore(config);
    return coreStoreProvider.getStore(storeId);
  }

  public void deleteEverything(String... skipNamesArray) throws IOException {
    coreStoreProvider.deleteEverything(skipNamesArray);
  }

  /**
   * reIndex a specific store
   * @param id
   */
  public int reIndex(String id) {
    return coreStoreProvider.reIndex(id);
  }

  /**
   * Store builder for master store provider.
   * @param <K>
   * @param <V>
   */
  public class LocalStoreBuilder<K, V> implements StoreBuilder<K, V> {

    private CoreStoreBuilder<K, V> coreStoreBuilder;

    public LocalStoreBuilder(CoreStoreBuilder<K, V> coreStoreBuilder) {
      this.coreStoreBuilder = coreStoreBuilder;
    }

    @Override
    public StoreBuilder<K, V> name(String name) {
      coreStoreBuilder = coreStoreBuilder.name(name);
      return this;
    }

    @Override
    public StoreBuilder<K, V> keySerializer(Class<? extends Serializer<K>> keySerializerClass) {
      coreStoreBuilder = coreStoreBuilder.keySerializer(keySerializerClass);
      return this;
    }

    @Override
    public StoreBuilder<K, V> valueSerializer(Class<? extends Serializer<V>> valueSerializerClass) {
      coreStoreBuilder = coreStoreBuilder.valueSerializer(valueSerializerClass);
      return this;
    }

    @Override
    public StoreBuilder<K, V> versionExtractor(Class<? extends VersionExtractor<V>> versionExtractorClass) {
      coreStoreBuilder = coreStoreBuilder.versionExtractor(versionExtractorClass);
      return this;
    }

    @Override
    public KVStore<K, V> build() {
      return new LocalKVStore<>(coreStoreBuilder.build());
    }

    @Override
    public IndexedStore<K, V> buildIndexed(Class<? extends DocumentConverter<K, V>> documentConverterClass) {
      return new LocalIndexedStore<>(coreStoreBuilder.buildIndexed(documentConverterClass));
    }
  }
}
