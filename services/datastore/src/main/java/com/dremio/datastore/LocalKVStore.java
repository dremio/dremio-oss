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

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.ImmutableFindByRange;
import com.dremio.datastore.api.KVStore;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

/**
 * Local KVStore implementation. (runs on master node)
 */
public class LocalKVStore<K, V> implements KVStore<K, V> {

  private final CoreKVStore<K, V> coreKVStore;

  public LocalKVStore(CoreKVStore<K, V> coreKVStore) {
    this.coreKVStore = coreKVStore;
  }

  protected KVStoreTuple<K> buildKey(K key) {
    return coreKVStore.newKey().setObject(key);
  }

  @Override
  public KVAdmin getAdmin() {
    return coreKVStore.getAdmin();
  }

  protected KVStoreTuple<V> buildValue(V  value) {
    return coreKVStore.newValue().setObject(value);
  }

  protected K extractKey(KVStoreTuple<K> tuple) {
    return tuple.getObject();
  }

  protected V extractValue(KVStoreTuple<V> tuple) {
    return tuple.getObject();
  }

  @Override
  public Document<K, V> get(K key, GetOption... options) {
    return fromDocument(coreKVStore.get(buildKey(key), options));
  }

  @Override
  public Iterable<Document<K, V>> get(List<K> keys, GetOption... options) {
    final List<KVStoreTuple<K>> convertedKeys = keys.stream()
      .map(this::buildKey)
      .collect(Collectors.toList());

    final Iterable<Document<KVStoreTuple<K>, KVStoreTuple<V>>> convertedValues = coreKVStore.get(convertedKeys);

    return Iterables.transform(convertedValues, this::fromDocument);
  }

  @Override
  public Document<K,V> put(K key, V value, PutOption... options) {
    return fromDocument(coreKVStore.put(buildKey(key), buildValue(value), options));
  }

  @Override
  public boolean contains(K key, ContainsOption... options) {
    return coreKVStore.contains(buildKey(key), options);
  }

  @Override
  public void delete(K key, DeleteOption... options) {
    coreKVStore.delete(buildKey(key), options);
  }

  @Override
  public Iterable<Document<K, V>> find(FindByRange<K> find, FindOption... options) {
    final FindByRange<KVStoreTuple<K>> convertedRange = new ImmutableFindByRange.Builder<KVStoreTuple<K>>()
      .setStart(buildKey(find.getStart()))
      .setIsStartInclusive(find.isStartInclusive())
      .setEnd(buildKey(find.getEnd()))
      .setIsEndInclusive(find.isEndInclusive())
      .build();

    return Iterables.transform(coreKVStore.find(convertedRange, options), this::fromDocument);
  }

  @Override
  public Iterable<Document<K, V>> find(FindOption... options) {
    return Iterables.transform(coreKVStore.find(options), this::fromDocument);
  }

  @Override
  public String getName() {
    return coreKVStore.getName();
  }

  protected ConvertingDocument fromDocument(Document<KVStoreTuple<K>, KVStoreTuple<V>> input) {
    if (input == null) {
      return null;
    }
    return new ConvertingDocument(input);
  }

  protected class ConvertingDocument implements Document<K, V> {
    private final Document<KVStoreTuple<K>, KVStoreTuple<V>> input;

    public ConvertingDocument(Document<KVStoreTuple<K>, KVStoreTuple<V>> input) {
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
    public String getTag() {
      return input.getTag();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Document)) {
        return false;
      }

      Document<?,?> e = (Document<?,?>) o;

      return getKey().equals(e.getKey())
        && getValue().equals(e.getValue())
        && Objects.equal(getTag(), e.getTag());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(getKey(), getValue(), getTag());
    }
  }
}
