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
package com.dremio.datastore;

import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.Service;

/**
 * Key-value store abstraction
 */
public interface KVStoreProvider extends Service {

  /**
   * Get the store associated with the provided creator class.
   * @param creator The creator function.
   * @return The associated kvstore, previously initialized.
   */
  <T extends KVStore<?, ?>> T getStore(Class<? extends StoreCreationFunction<T>> creator);

  /**
   * Interface to configure and construct different store types.
   *
   * @param <K>
   * @param <V>
   */
  public interface StoreBuilder<K, V> {
    public StoreBuilder<K, V> name(String name);
    public StoreBuilder<K, V> keySerializer(Class<? extends Serializer<K>> keySerializerClass);
    public StoreBuilder<K, V> valueSerializer(Class<? extends Serializer<V>> valueSerializerClass);
    public StoreBuilder<K, V> versionExtractor(Class<? extends VersionExtractor<V>> versionExtractorClass);
    public KVStore<K, V> build();
    public IndexedStore<K, V> buildIndexed(Class<? extends DocumentConverter<K, V>> documentConverterClass);
  }

  /**
   * Helper provided by the kvstore for adding document to the index
   */
  interface DocumentWriter {
    /**
     * Add the following string values to the index
     * @param key
     * @param values
     */
    void write(IndexKey key, String... values);
    void write(IndexKey key, byte[]... values);
    void write(IndexKey key, Long value);
    void write(IndexKey key, Double value);
    void write(IndexKey key, Integer value);
  }

  /**
   * Converter that converts a value into a indexable document.
   *
   * @param <V> The value type to convert.
   */
  interface DocumentConverter<K, V> {
    /**
     * Convert the key/value pair using the providing writer
     *
     * @param writer
     * @param key
     * @param record
     */
    void convert(DocumentWriter writer, K key, V record);
  }

}
