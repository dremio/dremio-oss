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

import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.service.DirectProvider;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;

/**
 * Remore kvstore test
 */
public class TestRemoteKVStore extends AbstractTestKVStore {

  private static final String HOSTNAME = "localhost";
  private static final int THREAD_COUNT = 2;
  private static final long RESERVATION = 0;
  private static final long MAX_ALLOCATION = Long.MAX_VALUE;
  private static final int TIMEOUT = 300;

  @ClassRule
  public static final TemporaryFolder tmpFolder = new TemporaryFolder();

  private FabricService localFabricService;
  private FabricService remoteFabricService;
  private LocalKVStoreProvider localKVStoreProvider;
  private RemoteKVStoreProvider remoteKVStoreProvider;
  private BufferAllocator allocator;
  private CloseableThreadPool pool;

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Override
  void initProvider() throws Exception {

    allocator = allocatorRule.newAllocator("test-remote-kvstore", 0, 20 * 1024 * 1024);
    pool = new CloseableThreadPool("test-remoteocckvstore");

    localFabricService = new FabricServiceImpl(HOSTNAME, 45678, true, THREAD_COUNT, allocator, RESERVATION,
        MAX_ALLOCATION, TIMEOUT, pool);
    localFabricService.start();

    remoteFabricService = new FabricServiceImpl(HOSTNAME, 45679, true, THREAD_COUNT, allocator, RESERVATION,
        MAX_ALLOCATION, TIMEOUT, pool);
    remoteFabricService.start();

    localKVStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT,
        DirectProvider.wrap(localFabricService), allocator, HOSTNAME, tmpFolder.getRoot().toString(),
        true, true, true, false);
    localKVStoreProvider.start();

    remoteKVStoreProvider = new RemoteKVStoreProvider(
        DremioTest.CLASSPATH_SCAN_RESULT,
        DirectProvider.wrap(NodeEndpoint.newBuilder()
            .setAddress(HOSTNAME)
            .setFabricPort(localFabricService.getPort())
            .build()),
        DirectProvider.wrap(remoteFabricService), allocator, HOSTNAME);
    remoteKVStoreProvider.start();
  }

  @Override
  void closeProvider() throws Exception {
    AutoCloseables.close(remoteKVStoreProvider, localKVStoreProvider, remoteFabricService, localFabricService, pool, allocator);
  }

  @Override
  Backend createBackEndForKVStore() {
    final String name = UUID.randomUUID().toString();
    final KVStore<String, String> localKVStore = localKVStoreProvider.<String, String>newStore()
      .name(name)
      .keySerializer(StringSerializer.class)
      .valueSerializer(StringSerializer.class).build();

    return new Backend() {
      @Override
      public String get(String key) {
        return localKVStore.get(key);
      }

      @Override
      public void put(String key, String value) {
        localKVStore.put(key, value);
      }

      @Override
      public String getName() {
        return name;
      }
    };
  }

  @Override
  KVStore<String, String> createKVStore(Backend backend) {
    return remoteKVStoreProvider.<String, String>newStore()
      .name(backend.getName())
      .keySerializer(StringSerializer.class)
      .valueSerializer(StringSerializer.class).build();
  }
}
