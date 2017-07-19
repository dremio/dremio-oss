/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.service.DirectProvider;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;

/**
 * Tests for remote occ kvstore.
 */
public class TestRemoteOCCKVStore extends AbstractTestOCCKVStore {
  private FabricService localFabricService;
  private FabricService remoteFabricService;
  private LocalKVStoreProvider localKVStoreProvider;
  private RemoteKVStoreProvider remoteKVStoreProvider;
  private BufferAllocator allocator;
  private CloseableThreadPool pool;

  @Override
  KVStoreProvider createKKStoreProvider() throws Exception {

    allocator = new RootAllocator(20 * 1024 * 1024);
    pool = new CloseableThreadPool("test-remoteocckvstore");
    localFabricService = new FabricServiceImpl("localhost", 45678, true, 300, 2, pool, allocator, 0, Long.MAX_VALUE);
    localFabricService.start();

    remoteFabricService = new FabricServiceImpl("localhost", 45679, true, 300, 2, pool, allocator, 0, Long.MAX_VALUE);
    remoteFabricService.start();

    localKVStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, DirectProvider.<FabricService>wrap(localFabricService), allocator, "localhost", null, true, true, true, false);
    localKVStoreProvider.start();
    remoteKVStoreProvider = new RemoteKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, DirectProvider.<FabricService>wrap(remoteFabricService), allocator, "localhost", "localhost", localFabricService.getPort());
    remoteKVStoreProvider.start();
    return remoteKVStoreProvider;
  }

  @After
  @Override
  public void after() throws Exception {
    AutoCloseables.close(remoteKVStoreProvider, localKVStoreProvider, remoteFabricService, localFabricService, pool, allocator);
  }
}
