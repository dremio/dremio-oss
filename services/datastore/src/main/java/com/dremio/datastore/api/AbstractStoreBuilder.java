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

/**
 * An abstract implementation of KVStoreProvider.StoreBuilder to provide method implementations of
 * common basic functionality.
 *
 * @param <K> key type K.
 * @param <V> value type V.
 */
public abstract class AbstractStoreBuilder<K, V> implements KVStoreProvider.StoreBuilder<K, V> {
  private StoreBuilderHelper<K, V> helper = new StoreBuilderHelper<>();

  @Override
  public KVStoreProvider.StoreBuilder<K, V> name(String name) {
    helper.name(name);
    return this;
  }

  @Override
  public KVStoreProvider.StoreBuilder<K, V> keyFormat(Format<K> format) {
    if (format.apply(new BinaryFormatVisitor())) {
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

  protected StoreBuilderHelper<K, V> getStoreBuilderHelper() {
    return helper;
  }
}
