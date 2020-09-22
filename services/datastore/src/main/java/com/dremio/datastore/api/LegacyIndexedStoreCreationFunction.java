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

import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter.LegacyStoreBuilderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider.LegacyStoreBuilder;
import com.dremio.service.Pointer;

/**
 * Function used to build a LegacyIndexedStore. Class is used as a key to later access the singleton LegacyIndexedStore.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@Deprecated
public interface LegacyIndexedStoreCreationFunction<K, V> extends LegacyStoreCreationFunction<K, V, LegacyIndexedStore<K, V>, IndexedStore<K, V>> {
  @Override
  default IndexedStore<K, V> build(StoreBuildingFactory factory) {
    final Pointer<IndexedStore<K, V>> pointer =  new Pointer<>();

    build(new LegacyStoreBuildingFactory() {
      @Override
      public <T, U> LegacyStoreBuilder<T, U> newStore() {

        return new LegacyStoreBuilderAdapter<T, U>(factory::newStore) {
          @Override
          public LegacyKVStore<T, U> build() {
            throw new UnsupportedOperationException();
          }

          @Override
          public LegacyIndexedStore<T, U> buildIndexed(DocumentConverter<T, U> documentConverter) {
            if (pointer.value != null) {
              throw new IllegalStateException("newStore can only be used once");
            }
            pointer.value = (IndexedStore<K, V>) this.doBuildIndexed(documentConverter);
            return null;
          }
        };
      }
    });

    if (pointer.value == null) {
      throw new IllegalStateException("newStore needs to be used once.");
    }
    return pointer.value;
  }
}
