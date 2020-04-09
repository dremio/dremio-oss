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
package com.dremio.exec.server.options;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nullable;
import javax.inject.Provider;

import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreCreationFunction;
import com.dremio.exec.serialization.InstanceSerializer;
import com.dremio.options.OptionValue;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * KVStore wrapper for handling Jackson serialized OptionValue
 */
@Deprecated
class OptionValueStore {
//  private static final Logger logger = LoggerFactory.getLogger(OptionValueStore.class);

  public interface OptionValueStoreCreator extends LegacyStoreCreationFunction<LegacyKVStore<String, byte[]>> {}

  private final Provider<LegacyKVStoreProvider> kvStoreProvider;
  private final Class<? extends OptionValueStoreCreator> storeCreatorClass;
  private final InstanceSerializer<OptionValue> serializer;
  private LegacyKVStore<String, byte[]> store;

  public OptionValueStore(final Provider<LegacyKVStoreProvider> storeProvider,
                          final Class<? extends OptionValueStoreCreator> storeCreatorClass,
                          final InstanceSerializer<OptionValue> serializer) {
    this.kvStoreProvider = Preconditions.checkNotNull(storeProvider, "store provider is required");
    this.storeCreatorClass = Preconditions.checkNotNull(storeCreatorClass, "store creator is required");
    this.serializer = serializer;
  }

  public void start() {
    store = kvStoreProvider.get().getStore(storeCreatorClass);
  }

  private OptionValue deserialize(byte[] raw) {
    if (raw == null) {
      return null;
    }
    try {
      return serializer.deserialize(raw);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] serialize(OptionValue instance) {
    try {
      return serializer.serialize(instance);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public OptionValue get(String key) {
    return deserialize(store.get(key));
  }

  public void put(String key, OptionValue value) {
    store.put(key, serialize(value));
  }

  public void delete(String key) {
    store.delete(key);
  }

  public Iterator<Map.Entry<String, OptionValue>> getAll() {
    return Iterables.transform(store.find(), new Function<Map.Entry<String, byte[]>, Map.Entry<String, OptionValue>>() {
      @Override
      public Map.Entry<String, OptionValue> apply(@Nullable Map.Entry<String, byte[]> input) {
        return new AbstractMap.SimpleEntry<>(input.getKey(), deserialize(input.getValue()));
      }
    }).iterator();
  }
}
