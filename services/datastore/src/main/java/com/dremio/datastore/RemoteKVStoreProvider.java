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
import com.dremio.exec.rpc.RpcException;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Remote kvstore provider.
 */
@KVStoreProviderType(type="RemoteDB")
public class RemoteKVStoreProvider implements KVStoreProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RemoteKVStoreProvider.class);
  public static final String CONFIG_HOSTNAME = "hostName";

  private DatastoreRpcClient rpcClient;
  private final Provider<NodeEndpoint> masterNode;
  private final Provider<FabricService> fabricService;
  private final BufferAllocator allocator;
  private final String hostName;
  private final ScanResult scan;
  private ImmutableMap<Class<? extends StoreCreationFunction<?>>, KVStore<?, ?>> stores;

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
           config.get(CONFIG_HOSTNAME), String.format("Missing services.datastore.config.%s in dremio.conf", CONFIG_HOSTNAME))
         )
    );
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends KVStore<?, ?>> T getStore(Class<? extends StoreCreationFunction<T>> creator) {
    return (T) Preconditions.checkNotNull(stores.get(creator), "Unknown store creator %s", creator.getName());
  }

  @VisibleForTesting
  <K, V> StoreBuilder<K, V> newStore(){
    return new RemoteStoreBuilder<>();
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting RemoteKVStoreProvider");
    DefaultDataStoreRpcHandler rpcHandler = new DefaultDataStoreRpcHandler(hostName);
    try {
      DatastoreRpcService rpcService = new DatastoreRpcService(masterNode, fabricService.get(), allocator, rpcHandler);
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
  public class RemoteStoreBuilder<K, V> implements StoreBuilder<K, V> {
    private final StoreBuilderConfig config = new StoreBuilderConfig();

    @Override
    public StoreBuilder<K, V> name(String name) {
      config.setName(name);
      return this;
    }

    @Override
    public StoreBuilder<K, V> keySerializer(Class<? extends Serializer<K>> keySerializerClass) {
      config.setKeySerializerClassName(keySerializerClass.getName());
      return this;
    }

    @Override
    public StoreBuilder<K, V> valueSerializer(Class<? extends Serializer<V>> valueSerializerClass) {
      config.setValueSerializerClassName(valueSerializerClass.getName());
      return this;
    }

    @Override
    public StoreBuilder<K, V> versionExtractor(Class<? extends VersionExtractor<V>> versionExtractorClass) {
      config.setVersionExtractorClassName(versionExtractorClass.getName());
      return this;
    }

    @Override
    public KVStore<K, V> build() {
      return new RemoteKVStore<>(rpcClient, rpcClient.buildStore(config), config);
    }

    @Override
    public IndexedStore<K, V> buildIndexed(Class<? extends DocumentConverter<K, V>> documentConverterClass) {
      config.setDocumentConverterClassName(documentConverterClass.getName());
      return new RemoteIndexedStore<>(rpcClient, rpcClient.buildStore(config), config);
    }
  }

}
