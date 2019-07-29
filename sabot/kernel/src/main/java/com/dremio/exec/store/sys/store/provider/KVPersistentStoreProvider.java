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
package com.dremio.exec.store.sys.store.provider;

import javax.inject.Provider;

import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.exception.StoreException;
import com.dremio.exec.serialization.InstanceSerializer;
import com.dremio.exec.store.sys.PersistentStore;
import com.dremio.exec.store.sys.store.KVPersistentStore;
import com.dremio.exec.store.sys.store.KVPersistentStore.PersistentStoreCreator;
import com.dremio.exec.testing.store.InMemoryLocalStore;
import com.google.common.base.Preconditions;

/**
 * Provides KVStore-based persistent stores
 */
public class KVPersistentStoreProvider extends BasePersistentStoreProvider {

  private final Provider<KVStoreProvider> storeProvider;
  private final boolean inMemory;

  public KVPersistentStoreProvider(final Provider<KVStoreProvider> storeProvider) {
    this(storeProvider, false);
  }

  public KVPersistentStoreProvider(final Provider<KVStoreProvider> storeProvider, boolean inMemory) {
    this.storeProvider = Preconditions.checkNotNull(storeProvider, "store provider is required");
    this.inMemory = inMemory;
  }

  @Override
  public <V> PersistentStore<V> getOrCreateStore(final String storeName,
      final Class<? extends PersistentStoreCreator> storeCreator, InstanceSerializer<V> serializer) throws StoreException {
    if (inMemory) {
      return new InMemoryLocalStore<>();
    }
    final KVPersistentStore<V> store = new KVPersistentStore<>(storeProvider, storeCreator, serializer);
    try {
      store.start();
    } catch (Exception e) {
      throw new StoreException(String.format("Unable to start persistent store %s", storeName), e);
    }
    return store;
  }
}
