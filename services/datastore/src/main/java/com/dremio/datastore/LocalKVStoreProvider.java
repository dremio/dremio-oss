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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.CoreStoreProvider.CoreStoreBuilder;
import com.dremio.datastore.CoreStoreProviderImpl.StoreWithId;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.AbstractStoreBuilder;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.datastore.indexed.AuxiliaryIndex;
import com.dremio.datastore.indexed.AuxiliaryIndexImpl;
import com.dremio.datastore.indexed.LocalIndexedStore;
import com.dremio.datastore.utility.StoreLoader;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcException;
import com.dremio.service.DirectProvider;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Datastore provider for master node.
 */
@KVStoreProviderType(type="LocalDB")
public class LocalKVStoreProvider implements KVStoreProvider, Iterable<StoreWithId<?, ?>> {
  private static final Logger logger = LoggerFactory.getLogger(LocalKVStoreProvider.class);
  public static final String CONFIG_HOSTNAME = "hostName";
  public static final String CONFIG_BASEDIRECTORY = "baseDirectory";
  public static final String CONFIG_TIMED = "timed";
  public static final String CONFIG_VALIDATEOCC = "validateOCC";
  public static final String CONFIG_DISABLEOCC = "disableOCC";
  public static final String ERR_FMT = "Missing services.datastore.config.%s in dremio.conf";
  public static final String ERR_EMPTY_SCANRESULT = "ScanResult can not be null";

  private final CoreStoreProviderImpl coreStoreProvider;
  private final Provider<FabricService> fabricService;
  private final BufferAllocator allocator;
  private final String hostName;
  private final ScanResult scan;
  // To provide compatibility with older code
  private final LegacyKVStoreProvider legacyProvider;
  private long remoteRpcTimeout;
  private StoreBuildingFactory storeBuildingFactory;

  private ImmutableMap<Class<? extends StoreCreationFunction<?, ?, ?>>, KVStore<?, ?>> stores;
  private Supplier<ImmutableMap<Class<? extends StoreCreationFunction<?, ?, ?>>, KVStore<?, ?>>> storesProvider;

  @VisibleForTesting
  public LocalKVStoreProvider(ScanResult scan, String baseDirectory, boolean inMemory, boolean timed) {
    this(scan, null, null, null, baseDirectory, inMemory, timed, false);
  }

  public LocalKVStoreProvider(ScanResult scan, String baseDirectory, boolean inMemory, boolean timed,
    boolean noDBOpenRetry, boolean noDBLogMessages, boolean readOnly) {
    this(scan, null, null, null, baseDirectory, inMemory, timed, noDBOpenRetry, noDBLogMessages, readOnly);
  }

  public LocalKVStoreProvider(
    ScanResult scan,
    Provider<FabricService> fabricService,
    BufferAllocator allocator,
    String hostName,
    String baseDirectory,
    boolean inMemory,
    boolean timed,
    boolean readOnly) {
    this(scan, fabricService, allocator, hostName, baseDirectory, inMemory, timed, false, false, readOnly);
  }

  public LocalKVStoreProvider(
    ScanResult scan,
    Provider<FabricService> fabricService,
    BufferAllocator allocator,
    String hostName,
    String baseDirectory,
    boolean inMemory,
    boolean timed,
    boolean noDBOpenRetry,
    boolean noDBLogMessages,
    boolean readOnly) {

    coreStoreProvider = new CoreStoreProviderImpl(baseDirectory, inMemory, timed, noDBOpenRetry, false,
      noDBLogMessages, readOnly);
    this.fabricService = fabricService;
    this.allocator = allocator;
    this.hostName = hostName;
    Preconditions.checkNotNull(scan, ERR_EMPTY_SCANRESULT);
    this.scan = scan;
    this.legacyProvider = new LegacyKVStoreProviderAdapter(this);
    this.storeBuildingFactory = this::newStore;
    this.storesProvider = getStoreProvider();
  }

  protected Supplier<ImmutableMap<Class<? extends StoreCreationFunction<?, ?, ?>>, KVStore<?, ?>>> getStoreProvider(){
    return () -> StoreLoader.buildStores(scan, storeBuildingFactory);
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
      String.valueOf(Preconditions.checkNotNull(config.get(CONFIG_BASEDIRECTORY), String.format(ERR_FMT,
        CONFIG_BASEDIRECTORY))),
      Boolean.valueOf(Preconditions.checkNotNull(config.get(DremioConfig.DEBUG_USE_MEMORY_STRORAGE_BOOL),
        String.format("Missing %s in dremio.conf", DremioConfig.DEBUG_USE_MEMORY_STRORAGE_BOOL)).toString()),
      Boolean.valueOf(Preconditions.checkNotNull(config.get(CONFIG_TIMED), String.format(ERR_FMT, CONFIG_TIMED)).toString()),
      false, false, false
    );

    this.remoteRpcTimeout = (Long)config.get(DremioConfig.REMOTE_DATASTORE_RPC_TIMEOUT_SECS);
  }

  @VisibleForTesting
  public void setStoresProvider(Supplier<ImmutableMap<Class<? extends StoreCreationFunction<?, ?, ?>>, KVStore<?, ?>>> storesProvider) {
    this.storesProvider = storesProvider;
  }

  @Override
  @VisibleForTesting
  public <K, V> StoreBuilder<K, V> newStore(){
    return new LocalStoreBuilder<>(coreStoreProvider.<K, V>newStore());
  }

  @VisibleForTesting
  public void setStoreBuildingFactory(StoreBuildingFactory storeBuildingFactory) {
    this.storeBuildingFactory = storeBuildingFactory;
  }

  @Override
  public Set<KVStore<?, ?>> stores() {
    return new ImmutableSet.Builder<KVStore<?,?>>().addAll(stores.values().iterator()).build();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V, T extends KVStore<K, V>> T getStore(Class<? extends StoreCreationFunction<K, V, T>> creator) {
    return (T) Preconditions.checkNotNull(stores.get(creator), "Unknown store creator %s", creator.getName());
  }

  public <K, V, T> AuxiliaryIndex<K, V, T> getAuxiliaryIndex(String name, String kvStoreName, Class<? extends DocumentConverter<K, T>> converter) throws InstantiationException, IllegalAccessException {
    CoreKVStore<K, V> store = (CoreKVStore<K, V>) coreStoreProvider.getStore(kvStoreName);
    return new AuxiliaryIndexImpl<>(name, store, coreStoreProvider.getIndex(name), converter);
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting LocalKVStoreProvider");
    coreStoreProvider.start();

    // Build all stores before starting up the DatastoreRpcService.
    stores = storesProvider.get();

    // recover after the stores are built
    coreStoreProvider.recoverIfPreviouslyCrashed();

    if (fabricService != null) {
      final DefaultDataStoreRpcHandler rpcHandler = new LocalDataStoreRpcHandler(hostName, coreStoreProvider);
      final NodeEndpoint thisNode = NodeEndpoint.newBuilder()
        .setAddress(hostName)
        .setFabricPort(fabricService.get().getPort())
        .build();
      try {
        // DatastoreRpcService registers itself with fabric
        //noinspection ResultOfObjectAllocationIgnored
        new DatastoreRpcService(DirectProvider.wrap(thisNode), fabricService.get(), allocator, rpcHandler, remoteRpcTimeout);
      } catch (RpcException e) {
        throw new DatastoreException("Failed to start rpc service", e);
      }
    }

    legacyProvider.start();

    logger.info("LocalKVStoreProvider is up");
  }

  public void scan() throws Exception {
    coreStoreProvider.scan();
  }

  @Override
  public Iterator<StoreWithId<?, ?>> iterator() {
    return coreStoreProvider.iterator();
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping LocalKVStoreProvider");
    legacyProvider.close();
    coreStoreProvider.close();
    logger.info("Stopped LocalKVStoreProvider");
  }

  public Map<KVStoreInfo, CoreKVStore<?, ?>> getStores() {
    return coreStoreProvider.getStores();
  }

  public CoreKVStore<?, ?> getStore(KVStoreInfo config) {
    final String storeId = (coreStoreProvider).getStoreID(config.getTablename());
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
   * Get a {@link LegacyKVStoreProvider} view of this provider
   *
   * Note that the provider has to be started first
   *
   * @return
   */
  @Deprecated
  public LegacyKVStoreProvider asLegacy() {
    return legacyProvider;
  }

  public CheckpointInfo newCheckpoint(Path backupDir) {
    return this.coreStoreProvider.newCheckpoint(backupDir);
  }

  /**
   * Store builder for master/Raas store provider.
   *
   * @param <K>
   * @param <V>
   */
  public static class LocalStoreBuilder<K, V> extends AbstractStoreBuilder<K, V> {

    private CoreStoreBuilder<K, V> coreStoreBuilder;

    public LocalStoreBuilder(CoreStoreBuilder<K, V> coreStoreBuilder) {
      this.coreStoreBuilder = coreStoreBuilder;
    }

    @Override
    public KVStore<K, V> doBuild() {
      return new LocalKVStore<>(coreStoreBuilder.build(getStoreBuilderHelper()));
    }

    @Override
    public IndexedStore<K, V> doBuildIndexed(DocumentConverter<K, V> documentConverter) {
      getStoreBuilderHelper().documentConverter(documentConverter);
      return new LocalIndexedStore<>(coreStoreBuilder.buildIndexed(getStoreBuilderHelper()));
    }
  }
}
