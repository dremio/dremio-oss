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

import com.dremio.datastore.api.LegacyKVStoreProvider.LegacyStoreBuilder;

/** Internal Interface for defining how to build a legacy kv store. */
@Deprecated
@FunctionalInterface
public interface LegacyStoreBuildingFactory {
  /**
   * Create a new key-value store which preserves key ordering
   *
   * @param <K> key type K.
   * @param <V> value type V.
   * @return a hash based key value store
   */
  @Deprecated
  <K, V> LegacyStoreBuilder<K, V> newStore();
}
