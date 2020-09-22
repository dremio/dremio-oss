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

import static com.dremio.datastore.LocalKVStoreProvider.CONFIG_HOSTNAME;

import java.util.Map;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.AbstractStoreBuilder;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.api.StoreCreationFunction;
import com.dremio.datastore.utility.StoreLoader;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.RpcException;
import com.dremio.service.DirectProvider;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Remote KVStore Provider.
 */
@KVStoreProviderType(type="RemoteDB")
public class RemoteKVStoreProvider implements KVStoreProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemoteKVStoreProvider.class);
  public static final String HOSTNAME = "thisHostName";

  private DatastoreRpcClient rpcClient;
  private final Provider<NodeEndpoint> masterNode;
  private final Provider<FabricService> fabricService;
  private final BufferAllocator allocator;
  private final String hostName;
  private final ScanResult scan;
  private ImmutableMap<Class<? extends StoreCreationFunction<?, ?, ?>>, KVStore<?, ?>> stores;
  private String masterHostName;
  private long remoteRpcTimeout;

  public RemoteKVStoreProvider(ScanResult scan, Provider<NodeEndpoint> masterNode, Provider<FabricService> fabricService, BufferAllocator allocator, String hostName) {
    this.masterNode = masterNode;
    this.fabricService = fabricService;
    this.allocator = allocator;
    this.hostName = hostName;
    this.scan = scan;
  }

  public RemoteKVStoreProvider(
    ScanResult scan,
    Provider<FabricService> fabricService,
    Provider<NodeEndpoint> masterNode,
    BufferAllocator allocator,
    Map<String, Object> config
  ) {
    this(scan,
         masterNode,
         fabricService,
         allocator,
         String.valueOf(Preconditions.checkNotNull(
           config.get(HOSTNAME), String.format("Missing services.datastore.config.%s in dremio.conf", HOSTNAME))
         )
    );
    // in case of test configuration in masterless mode specify master directly
    // either masterNode should be specified or masterHostName
    masterHostName = String.valueOf(config.get(CONFIG_HOSTNAME));
    this.remoteRpcTimeout = (Long)config.get(DremioConfig.REMOTE_DATASTORE_RPC_TIMEOUT_SECS);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V, T extends KVStore<K, V>> T getStore(Class<? extends StoreCreationFunction<K, V, T>> creator) {
    return (T) Preconditions.checkNotNull(stores.get(creator), "Unknown store creator %s", creator.getName());
  }

  @Override
  @VisibleForTesting
  public <K, V> StoreBuilder<K, V> newStore(){
    return new RemoteStoreBuilder<>();
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting RemoteKVStoreProvider");
    DefaultDataStoreRpcHandler rpcHandler = new DefaultDataStoreRpcHandler(hostName);
    try {
      final Provider<NodeEndpoint> masterHost;
      if (masterHostName != null && !masterHostName.equals(hostName)) {
        masterHost = DirectProvider.wrap(NodeEndpoint.newBuilder()
          .setAddress(masterHostName)
          .setFabricPort(fabricService.get().getPort())
          .build());
      } else {
        masterHost = masterNode;
      }

      DatastoreRpcService rpcService = new DatastoreRpcService(masterHost, fabricService.get(), allocator, rpcHandler,
        remoteRpcTimeout);
      rpcClient = new DatastoreRpcClient(rpcService);
    } catch (RpcException e) {
      throw new DatastoreFatalException("Failed to start rpc service", e);
    }

    stores = StoreLoader.buildStores(scan, new StoreBuildingFactory() {

      @Override
      public <K, V> StoreBuilder<K, V> newStore() {
        return RemoteKVStoreProvider.this.newStore();
      }
    });

    logger.info("RemoteKVStoreProvider is up");
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopped RemoteKVStoreProvider");
  }

  /**
   * Store builder for remote kvstore provider.
   * @param <K>
   * @param <V>
   */
  public class RemoteStoreBuilder<K, V> extends AbstractStoreBuilder<K, V> {
    @Override
    public KVStore<K, V> doBuild() {
      return new RemoteKVStore<>(rpcClient, rpcClient.getStoreId(getStoreBuilderHelper().getName()), getStoreBuilderHelper());
    }

    @Override
    public IndexedStore<K, V> doBuildIndexed(DocumentConverter<K, V> documentConverter) {
      getStoreBuilderHelper().documentConverter(documentConverter);
      return new RemoteIndexedStore<>(rpcClient, rpcClient.getStoreId(getStoreBuilderHelper().getName()), getStoreBuilderHelper());
    }
  }

}
