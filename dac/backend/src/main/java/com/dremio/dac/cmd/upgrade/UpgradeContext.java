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
package com.dremio.dac.cmd.upgrade;

import javax.inject.Provider;

import com.dremio.datastore.KVStoreProvider;
import com.dremio.service.accelerator.store.AccelerationEntryStore;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;

/**
 * contains all stores required by all upgrade tasks
 */
class UpgradeContext {

  private final AccelerationStore accelerationStore;
  private final MaterializationStore materializationStore;
  private final AccelerationEntryStore entryStore;
  private final UpgradeStats stats;
  private final NamespaceService namespaceService;

  UpgradeContext(Provider<KVStoreProvider> kvStoreProvider) {
    accelerationStore = new AccelerationStore(kvStoreProvider);
    entryStore = new AccelerationEntryStore(kvStoreProvider);
    materializationStore = new MaterializationStore(kvStoreProvider);
    namespaceService = new NamespaceServiceImpl(kvStoreProvider.get());
    stats = new UpgradeStats();

    accelerationStore.start();
    entryStore.start();
    materializationStore.start();
  }

  AccelerationStore getAccelerationStore() {
    return accelerationStore;
  }

  AccelerationEntryStore getEntryStore() {
    return entryStore;
  }

  MaterializationStore getMaterializationStore() {
    return materializationStore;
  }

  UpgradeStats getUpgradeStats() {
    return stats;
  }

  public NamespaceService getNamespaceService() {
    return namespaceService;
  }
}
