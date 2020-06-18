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

import com.dremio.datastore.DatastoreException;
import com.dremio.datastore.LegacyStoreBuilderHelper;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.format.visitor.BinaryFormatVisitor;
import com.dremio.service.Service;

/**
 * Legacy key-value store abstraction
 */
@Deprecated
public interface LegacyKVStoreProvider extends Service {

  /**
   * Get the store associated with the provided legacy creator class.
   * @param creator The creator function.
   * @return The associated kvstore, previously initialized.
   */
  <K, V, T extends LegacyKVStore<K, V>, U extends KVStore<K, V>>
  T getStore(Class<? extends LegacyStoreCreationFunction<K, V, T, U>> creator);

  /**
   * Interface to configure and construct different store types.
   *
   * @param <K>
   * @param <V>
   */
  interface LegacyStoreBuilder<K, V> {
    LegacyStoreBuilder<K, V> name(String name);
    LegacyStoreBuilder<K, V> keyFormat(Format<K> format);
    LegacyStoreBuilder<K, V> valueFormat(Format<V> format);
    LegacyStoreBuilder<K, V> versionExtractor(Class<? extends VersionExtractor<V>> versionExtractorClass);
    LegacyStoreBuilder<K, V> permitCompoundKeys(boolean permitCompoundKeys);
    LegacyKVStore<K, V> build();
    LegacyIndexedStore<K, V> buildIndexed(DocumentConverter<K, V> documentConverter);
  }

  /**
   * Most StoreBuilders collect data the same exact way.
   * @param <K>
   * @param <V>
   */
  abstract class LegacyAbstractStoreBuilder<K, V> implements LegacyStoreBuilder<K, V> {
    private final LegacyStoreBuilderHelper<K, V> helper = new LegacyStoreBuilderHelper<>();

    @Override
    public LegacyStoreBuilder<K, V> name(String name) {
      helper.name(name);
      return this;
    }

    @Override
    public LegacyStoreBuilder<K, V> keyFormat(Format<K> format) {
      if (format.apply(BinaryFormatVisitor.INSTANCE)) {
        throw new DatastoreException("Binary is not a supported key format.");
      }
      helper.keyFormat(format);
      return this;
    }

    @Override
    public LegacyStoreBuilder<K, V> valueFormat(Format<V> format) {
      helper.valueFormat(format);
      return this;
    }

    @Override
    public LegacyStoreBuilder<K, V> versionExtractor(Class<? extends VersionExtractor<V>> versionExtractorClass) {
      helper.versionExtractor(versionExtractorClass);
      return this;
    }

    protected LegacyStoreBuilderHelper<K, V> getStoreBuilderHelper() {
      return helper;
    }
  }

  /**
   * Method that allows decorators of kv store to be peeled off.
   * If unwrap cannot succeed, null is returned.
   */
  default <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return (T) this;
    }
    return null;
  }
}
