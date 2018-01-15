/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Local KVStore implementation. (runs on master node)
 */
public class LocalKVStore<K, V> implements KVStore<K, V> {

  private final CoreKVStore<K, V> coreKVStore;

  public LocalKVStore(CoreKVStore<K, V> coreKVStore) {
    this.coreKVStore = coreKVStore;
  }

  private KVStoreTuple<K> buildKey(K key) {
    return coreKVStore.newKey().setObject(key);
  }

  @Override
  public KVAdmin getAdmin() {
    return coreKVStore.getAdmin();
  }

  private KVStoreTuple<V> buildValue(V  value) {
    return coreKVStore.newValue().setObject(value);
  }

  private K extractKey(KVStoreTuple<K> tuple) {
    return tuple.getObject();
  }

  private V extractValue(KVStoreTuple<V> tuple) {
    return tuple.getObject();
  }

  @Override
  public V get(K key) {
    return extractValue(coreKVStore.get(buildKey(key)));
  }

  @Override
  public List<V> get(List<K> keys) {
    final List<KVStoreTuple<K>> convertedKeys = Lists.transform(keys, new Function<K, KVStoreTuple<K>>() {
      @Override
      public KVStoreTuple<K> apply(K key) {
        return buildKey(key);
      }
    });

    final List<KVStoreTuple<V>> convertedValues = coreKVStore.get(convertedKeys);
    return Lists.transform(convertedValues, new Function<KVStoreTuple<V>,  V>() {
      @Override
      public V apply(KVStoreTuple<V> value) {
        return extractValue(value);
      }
    });
  }

  @Override
  public void put(K key, V value) {
    coreKVStore.put(buildKey(key), buildValue(value));
  }

  @Override
  public boolean checkAndPut(K key, V oldValue, V newValue) {
    return coreKVStore.checkAndPut(buildKey(key), buildValue(oldValue), buildValue(newValue));
  }

  @Override
  public boolean contains(K key) {
    return coreKVStore.contains(buildKey(key));
  }

  @Override
  public void delete(K key) {
    coreKVStore.delete(buildKey(key));
  }

  @Override
  public boolean checkAndDelete(K key, V value) {
    return coreKVStore.checkAndDelete(buildKey(key), buildValue(value));
  }

  @Override
  public Iterable<Map.Entry<K, V>> find(FindByRange<K> find) {
    final FindByRange<KVStoreTuple<K>> convertedRange = new FindByRange<KVStoreTuple<K>>()
      .setStart(buildKey(find.getStart()), find.isStartInclusive())
      .setEnd(buildKey(find.getEnd()), find.isEndInclusive());

    final Iterable<Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>>> range = coreKVStore.find(convertedRange);
    return Iterables.transform(range, new Function<Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>>, Map.Entry<K, V>>() {
      public Map.Entry<K, V> apply(final Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>> input) {
        return new ConvertingEntry(input);
      }
    });
  }

  @Override
  public Iterable<Map.Entry<K, V>> find() {
    return Iterables.transform(coreKVStore.find(), new Function<Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>>, Map.Entry<K, V>>() {
      public Map.Entry<K, V> apply(final Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>> input) {
        return new ConvertingEntry(input);
      }
    });
  }

  @Override
  public void delete(K key, long previousVersion) {
    coreKVStore.delete(buildKey(key), previousVersion);
  }

  private class ConvertingEntry implements Map.Entry<K, V> {
    private final Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>> input;

    public ConvertingEntry(Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>> input) {
      super();
      this.input = input;
    }

    @Override
    public K getKey() {
      return extractKey(input.getKey());
    }

    @Override
    public V getValue() {
      return extractValue(input.getValue());
    }

    @Override
    public V setValue(Object value) {
      throw new UnsupportedOperationException();
    }

    public boolean equals(Object o) {
      if (!(o instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<?,?> e = (Map.Entry<?,?>) o;

      return getKey().equals(e.getKey())
        && getValue().equals(e.getValue());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(getKey(), getValue());
    }
  }

}
