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

import com.dremio.datastore.KVStoreProvider.DocumentConverter;

/**
 * Store provider for CoreKVStore and CoreIndexedStore.
 */
public interface CoreStoreProvider {

  <K, V> CoreStoreBuilder<K, V> newStore();

  /**
   * Interface to configure and construct different core store types.
   *
   * @param <K>
   * @param <V>
   */
  public interface CoreStoreBuilder<K, V> {
    public CoreStoreBuilder<K, V> name(String name);
    public CoreStoreBuilder<K, V> keySerializer(Class<? extends Serializer<K>> keySerializerClass);
    public CoreStoreBuilder<K, V> valueSerializer(Class<? extends Serializer<V>> valueSerializerClass);
    public CoreStoreBuilder<K, V>  versionExtractor(Class<? extends VersionExtractor<V>> versionExtractorClass);
    public CoreKVStore<K, V> build();
    public CoreIndexedStore<K, V> buildIndexed(Class<? extends DocumentConverter<K, V>> documentConverterClass);
  }

}
