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
package com.dremio.exec.store.sys;

import com.dremio.exec.exception.StoreException;
import com.dremio.exec.serialization.InstanceSerializer;
import com.dremio.exec.store.sys.store.KVPersistentStore.PersistentStoreCreator;
import com.dremio.service.Service;

/**
 * A factory used to create {@link PersistentStore store} instances.
 *
 */
public interface PersistentStoreProvider extends Service {
  /**
   * Gets or creates a {@link PersistentStore persistent store} for the given configuration.
   *
   * Note that implementors have liberty to cache previous {@link PersistentStore store} instances.
   *
   * @param storeName persistent store name
   * @param storeCreator store creator class passed to the kvStore provider
   * @param serializer instance serializer V <--> byte[]
   * @param <V>  store value type
   */
  <V> PersistentStore<V> getOrCreateStore(final String storeName,
                                          final Class<? extends PersistentStoreCreator> storeCreator,
                                          final InstanceSerializer<V> serializer) throws StoreException;

}
