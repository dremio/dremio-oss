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
package com.dremio.datastore.api;

/**
 * Function used to build a LegacyKVStore. Class is used as a key to later access the singleton
 * LegacyKVStore.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @param <T> The legacy KVStore class produced
 * @param <T> The KVStore class produced
 */
@Deprecated
public interface LegacyStoreCreationFunction<
        K, V, T extends LegacyKVStore<K, V>, U extends KVStore<K, V>>
    extends StoreCreationFunction<K, V, U> {

  /**
   * How to build this LegacyKVStore.
   *
   * @param factory LegacyStoreBuildingFactory to indicate how to build this store.
   * @return LegacyKVStore of type T.
   */
  @Deprecated
  T build(LegacyStoreBuildingFactory factory);
}
