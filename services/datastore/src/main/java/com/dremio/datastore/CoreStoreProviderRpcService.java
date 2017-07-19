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

import com.dremio.service.Service;

/**
 * Datastore provider rpc interface.
 */
public interface CoreStoreProviderRpcService extends CoreStoreProvider, Service {

  /**
   * @param storeId store id
   * @return
   * @throws IllegalArgumentException on invalid store id
   */
  CoreKVStore<Object, Object> getStore(String storeId);

  /**
   * Create new store for a given config or return existing store.
   * @param config Store configuration
   * @return store id
   */
  String getOrCreateStore(StoreBuilderConfig config);

}
