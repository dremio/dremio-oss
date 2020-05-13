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
import com.dremio.datastore.StoreBuilderHelper;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.format.visitor.BinaryFormatVisitor;
import com.dremio.datastore.format.visitor.CompoundFormatVisitor;

/**
 * An abstract implementation of KVStoreProvider.StoreBuilder to provide method implementations of
 * common basic functionality.
 *
 * @param <K> key type K.
 * @param <V> value type V.
 */
public abstract class AbstractStoreBuilder<K, V> implements KVStoreProvider.StoreBuilder<K, V> {
  private StoreBuilderHelper<K, V> helper = new StoreBuilderHelper<>();
  private boolean permitCompoundKeys = false;

  @Override
  public KVStoreProvider.StoreBuilder<K, V> name(String name) {
    helper.name(name);
    return this;
  }

  @Override
  public KVStoreProvider.StoreBuilder<K, V> keyFormat(Format<K> format) {
    if (format.apply(BinaryFormatVisitor.INSTANCE)) {
      throw new DatastoreException("Binary is not a supported key format.");
    }
    helper.keyFormat(format);
    return this;
  }

  @Override
  public KVStoreProvider.StoreBuilder<K, V> valueFormat(Format<V> format) {
    helper.valueFormat(format);
    return this;
  }

  @Override
  public KVStoreProvider.StoreBuilder<K, V> permitCompoundKeys(boolean permitCompoundKeys) {
    this.permitCompoundKeys = permitCompoundKeys;
    return this;
  }

  @Override
  public final KVStore<K, V> build() {
    checkCompoundKeyUsage();
    return doBuild();
  }

  @Override
  public final IndexedStore<K, V> buildIndexed(DocumentConverter<K, V> documentConverter) {
    checkCompoundKeyUsage();
    return doBuildIndexed(documentConverter);
  }

  protected StoreBuilderHelper<K, V> getStoreBuilderHelper() {
    return helper;
  }

  protected abstract KVStore<K, V> doBuild();
  protected abstract IndexedStore<K, V> doBuildIndexed(DocumentConverter<K, V> documentConverter);

  private void checkCompoundKeyUsage() {
    if (!permitCompoundKeys && helper.getKeyFormat().apply(CompoundFormatVisitor.INSTANCE)) {
      throw new DatastoreException("Compound keys are not permitted.");
    }
  }
}
