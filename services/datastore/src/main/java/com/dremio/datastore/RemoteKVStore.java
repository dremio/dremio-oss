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

import static java.lang.String.format;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;

import com.dremio.exec.rpc.RpcException;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * Remote KV Store. Caches store id received from master.
 */
public class RemoteKVStore <K, V> implements KVStore<K, V> {

  private final String storeId;
  private StoreBuilderConfig config;
  private final DatastoreRpcClient client;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final VersionExtractor<V> versionExtractor;

  @SuppressWarnings("unchecked")
  public RemoteKVStore(DatastoreRpcClient client, String storeId, StoreBuilderConfig config) {
    this.client = client;
    this.storeId = storeId;
    this.config = config;

    try {
      Constructor<?> constructor = Class.forName(config.getKeySerializerClassName()).getDeclaredConstructor();
      constructor.setAccessible(true);
      this.keySerializer = (Serializer<K>)constructor.newInstance();
    } catch (ClassNotFoundException | NoSuchMethodException| InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new DatastoreFatalException("Failed to create key serializer for class " + config.getKeySerializerClassName(), e);
    }

    try {
      Constructor<?> constructor = Class.forName(config.getValueSerializerClassName()).getDeclaredConstructor();
      constructor.setAccessible(true);
      this.valueSerializer = (Serializer<V>)constructor.newInstance();
    } catch (ClassNotFoundException | NoSuchMethodException| InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new DatastoreFatalException("Failed to create value serializer for class " + config.getValueSerializerClassName(), e);
    }

    if (config.getVersionExtractorClassName() != null && !config.getVersionExtractorClassName().isEmpty()) {
      try {
        Constructor<?> constructor = Class.forName(config.getVersionExtractorClassName()).getDeclaredConstructor();
        constructor.setAccessible(true);
        this.versionExtractor = (VersionExtractor<V>) constructor.newInstance();
      } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new DatastoreFatalException("Failed to create version extractor for class " + config.getValueSerializerClassName(), e);
      }
    } else {
      versionExtractor = null;
    }
  }

  @Override
  public V get(K key) {
    try {
      ByteString value = client.get(storeId, ByteString.copyFrom(keySerializer.serialize(key)));
      if (value != null && !value.isEmpty()) {
        return valueSerializer.deserialize(value.toByteArray());
      } else {
        return null;
      }
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to get from store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  public String getStoreId() {
    return storeId;
  }


  @Override
  public KVAdmin getAdmin() {
    throw new UnsupportedOperationException("KV administration can only be done on master node.");
  }

  public StoreBuilderConfig getConfig() {
    return config;
  }

  public DatastoreRpcClient getClient() {
    return client;
  }

  public Serializer<K> getKeySerializer() {
    return keySerializer;
  }

  public Serializer<V> getValueSerializer() {
    return valueSerializer;
  }

  @Override
  public List<V> get(List<K> keys) {
    try {
      List<ByteString> keyLists = Lists.newArrayList();
      for (K key : keys) {
        keyLists.add(ByteString.copyFrom(keySerializer.serialize(key)));
      }

      return Lists.transform(client.get(storeId, keyLists), new Function<ByteString, V>() {
        @Override
        public V apply(ByteString input) {
          if (input.isEmpty()) {
            return null;
          }
          return valueSerializer.deserialize(input.toByteArray());
        }
      });
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to get mutiple values from store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  @Override
  public void put(K key, V value) {
    try {
      Long version = client.put(storeId, ByteString.copyFrom(keySerializer.serialize(key)), ByteString.copyFrom(valueSerializer.serialize(value)));
      if (versionExtractor != null) {
        versionExtractor.setVersion(value, version);
      }
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to put in store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  @Override
  public boolean checkAndPut(K key, V oldValue, V newValue) {
    try {
      Pair<Boolean, Long> response = client.checkAndPut(storeId,
        ByteString.copyFrom(keySerializer.serialize(key)),
        oldValue == null? null : ByteString.copyFrom(valueSerializer.serialize(oldValue)),
        ByteString.copyFrom(valueSerializer.serialize(newValue)));

      if (versionExtractor != null) {
        versionExtractor.setVersion(newValue, response.getRight());
      }
      return response.getLeft();
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to checkAndPut in store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  @Override
  public boolean contains(K key) {
    try {
      return client.contains(storeId, ByteString.copyFrom(keySerializer.serialize(key)));
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to check contains for store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  @Override
  public void delete(K key) {
    try {
      client.delete(storeId, ByteString.copyFrom(keySerializer.serialize(key)));
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to delete from store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  @Override
  public boolean checkAndDelete(K key, V value) {
    try {
      return client.checkAndDelete(storeId,
        ByteString.copyFrom(keySerializer.serialize(key)),
        ByteString.copyFrom(valueSerializer.serialize(value)));
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to checkAndDelete from store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  @Override
  public Iterable<Entry<K, V>> find(FindByRange<K> find) {
    FindByRange<ByteString> findByRange = new FindByRange<ByteString>()
      .setStart(ByteString.copyFrom(keySerializer.serialize(find.getStart())), find.isStartInclusive())
      .setEnd(ByteString.copyFrom(keySerializer.serialize(find.getEnd())), find.isEndInclusive());
    try {
      return Iterables.transform(client.find(storeId, findByRange), new Function<Entry<ByteString, ByteString>, Entry<K, V>>() {
        @Override
        public Entry<K, V> apply(Entry<ByteString, ByteString> input) {
          return new AbstractMap.SimpleEntry<>(keySerializer.deserialize(input.getKey().toByteArray()),
            valueSerializer.deserialize(input.getValue().toByteArray()));
        }
      });
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to find by range for store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  @Override
  public Iterable<Entry<K, V>> find() {
    try {
      return Iterables.transform(client.find(storeId), new Function<Entry<ByteString, ByteString>, Entry<K, V>>() {
        @Override
        public Entry<K, V> apply(Entry<ByteString, ByteString> input) {
          return new AbstractMap.SimpleEntry<>(keySerializer.deserialize(input.getKey().toByteArray()),
            valueSerializer.deserialize(input.getValue().toByteArray()));
        }
      });
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to find all for store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }

  @Override
  public void delete(K key, long previousVersion) {
    try {
      client.delete(storeId, ByteString.copyFrom(keySerializer.serialize(key)), previousVersion);
    } catch (RpcException e) {
      throw new DatastoreException(format("Failed to delete previous version from store id: %s, config: %s", getStoreId(), getConfig().toString()), e);
    }
  }
}
