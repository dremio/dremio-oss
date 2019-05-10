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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.CoreStoreProvider.CoreStoreBuilder;
import com.dremio.datastore.CoreStoreProviderImpl.StoreWithId;
import com.dremio.datastore.indexed.AuxiliaryIndex;
import com.dremio.datastore.indexed.AuxiliaryIndexImpl;
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
@KVStoreProviderType(type="LocalDB")
public class LocalKVStoreProvider implements KVStoreProvider, Iterable<StoreWithId> {
  private static final Logger logger = LoggerFactory.getLogger(LocalKVStoreProvider.class);
  public static final String CONFIG_HOSTNAME = "hostName";
  public static final String CONFIG_BASEDIRECTORY = "baseDirectory";
  public static final String CONFIG_TIMED = "timed";
  public static final String CONFIG_VALIDATEOCC = "validateOCC";
  public static final String CONFIG_DISABLEOCC = "disableOCC";
  public static final String ERR_FMT = "Missing services.datastore.config.%s in dremio.conf";

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

  @VisibleForTesting
  public LocalKVStoreProvider(ScanResult scan, String baseDirectory, boolean inMemory, boolean timed, boolean validateOCC, boolean disableOCC, boolean noDBOpenRetry) {
    this(scan, null, null, null, baseDirectory, inMemory, timed, validateOCC, disableOCC, noDBOpenRetry);
  }

  public LocalKVStoreProvider(
      ScanResult scan,
      Provider<FabricService> fabricService,
      BufferAllocator allocator,
      String hostName,
      String baseDirectory,
      boolean inMemory,
      boolean timed,
      boolean validateOCC,
      boolean disableOCC) {
    this(scan, fabricService, allocator, hostName, baseDirectory, inMemory, timed, validateOCC, disableOCC, false);
  }

  public LocalKVStoreProvider(
      ScanResult scan,
      Provider<FabricService> fabricService,
      BufferAllocator allocator,
      String hostName,
      String baseDirectory,
      boolean inMemory,
      boolean timed,
      boolean validateOCC,
      boolean disableOCC,
      boolean noDBOpenRetry
  ) {

    coreStoreProvider = new CoreStoreProviderImpl(baseDirectory, inMemory, timed, validateOCC, disableOCC, noDBOpenRetry);
    this.fabricService = fabricService;
    this.allocator = allocator;
    this.hostName = hostName;
    this.scan = scan;
  }

  public LocalKVStoreProvider(
    ScanResult scan,
    Provider<FabricService> fabricService,
    Provider<NodeEndpoint> endpoint,        // unused
    BufferAllocator allocator,
    Map<String, Object> config
  ) {
    this(scan,
      fabricService,
      allocator,
      String.valueOf(Preconditions.checkNotNull(config.get(CONFIG_HOSTNAME), String.format(ERR_FMT, CONFIG_HOSTNAME))),
      String.valueOf(Preconditions.checkNotNull(config.get(CONFIG_BASEDIRECTORY), String.format(ERR_FMT, CONFIG_BASEDIRECTORY))),
      Boolean.valueOf(Preconditions.checkNotNull(config.get(DremioConfig.DEBUG_USE_MEMORY_STRORAGE_BOOL),
        String.format("Missing %s in dremio.conf", DremioConfig.DEBUG_USE_MEMORY_STRORAGE_BOOL)).toString()),
      Boolean.valueOf(Preconditions.checkNotNull(config.get(CONFIG_TIMED), String.format(ERR_FMT, CONFIG_TIMED)).toString()),
      Boolean.parseBoolean(Preconditions.checkNotNull(config.get(CONFIG_VALIDATEOCC), String.format(ERR_FMT, CONFIG_VALIDATEOCC)).toString()),
      Boolean.parseBoolean(Preconditions.checkNotNull(config.get(CONFIG_DISABLEOCC), String.format(ERR_FMT, CONFIG_DISABLEOCC)).toString()),
      false
    );
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

  public <K, V, T> AuxiliaryIndex<K, V, T> getAuxiliaryIndex(String name, String kvStoreName, Class<? extends KVStoreProvider.DocumentConverter<K, T>> converter) throws InstantiationException, IllegalAccessException {
    CoreKVStore<K, V> store = (CoreKVStore<K, V>) coreStoreProvider.getStore(kvStoreName);
    return new AuxiliaryIndexImpl<>(name, store, coreStoreProvider.getIndex(name), converter);
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting LocalKVStoreProvider");
    coreStoreProvider.start();
    if (fabricService != null) {
      final DefaultDataStoreRpcHandler rpcHandler = new LocalDataStoreRpcHandler(hostName, coreStoreProvider);
      final NodeEndpoint thisNode = NodeEndpoint.newBuilder()
          .setAddress(hostName)
          .setFabricPort(fabricService.get().getPort())
          .build();
      try {
        // DatastoreRpcService registers itself with fabric
        //noinspection ResultOfObjectAllocationIgnored
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

    // recover after the stores are built
    coreStoreProvider.recoverIfPreviouslyCrashed();

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
   * Reindex store with the given id.
   *
   * @param id store id
   * @return number of re-indexed entries
   */
  public int reIndex(String id) {
    return coreStoreProvider.reIndex(id);
  }

  /**
   * Store builder for master store provider.
   * @param <K>
   * @param <V>
   */
  public static class LocalStoreBuilder<K, V> implements StoreBuilder<K, V> {

    private CoreStoreBuilder<K, V> coreStoreBuilder;

    LocalStoreBuilder(CoreStoreBuilder<K, V> coreStoreBuilder) {
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
