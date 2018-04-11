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
package com.dremio.exec.store.sys.store;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nullable;
import javax.inject.Provider;

import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.exec.serialization.InstanceSerializer;
import com.dremio.exec.store.sys.PersistentStore;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * persistent store implementation that relies on the kv-store to persist the data
 */
public class KVPersistentStore<V> implements PersistentStore<V> {
//  private static final Logger logger = LoggerFactory.getLogger(KVPersistentStore.class);

  public interface PersistentStoreCreator extends StoreCreationFunction<KVStore<String, byte[]>> {}

  private final Provider<KVStoreProvider> kvStoreProvider;
  private final Class<? extends PersistentStoreCreator> storeCreatorClass;
  private final InstanceSerializer<V> serializer;
  private KVStore<String, byte[]> store;

  public KVPersistentStore(final Provider<KVStoreProvider> storeProvider,
                           final Class<? extends PersistentStoreCreator> storeCreatorClass,
                           final InstanceSerializer<V> serializer) {
    this.kvStoreProvider = Preconditions.checkNotNull(storeProvider, "store provider is required");
    this.storeCreatorClass = Preconditions.checkNotNull(storeCreatorClass, "store creator is required");
    this.serializer = serializer;
  }

  public void start() {
    store = kvStoreProvider.get().getStore(storeCreatorClass);
  }

  private V deserialize(byte[] raw) {
    if (raw == null) {
      return null;
    }
    try {
      return serializer.deserialize(raw);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] serialize(V instance) {
    try {
      return serializer.serialize(instance);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public V get(String key) {
    return deserialize(store.get(key));
  }

  @Override
  public void put(String key, V value) {
    store.put(key, serialize(value));
  }

  @Override
  public void delete(String key) {
    store.delete(key);
  }

  @Override
  public boolean putIfAbsent(String key, V value) {
    return store.checkAndPut(key, null, serialize(value));
  }

  @Override
  public Iterator<Map.Entry<String, V>> getAll() {
    return Iterables.transform(store.find(), new Function<Map.Entry<String, byte[]>, Map.Entry<String, V>>() {
      @Override
      public Map.Entry<String, V> apply(@Nullable Map.Entry<String, byte[]> input) {
        return new AbstractMap.SimpleEntry<>(input.getKey(), deserialize(input.getValue()));
      }
    }).iterator();
  }

  @Override
  public void close() throws Exception {
  }

}
