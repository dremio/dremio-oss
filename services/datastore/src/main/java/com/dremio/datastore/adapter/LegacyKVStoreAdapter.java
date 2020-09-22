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
package com.dremio.datastore.adapter;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.ImmutableFindByRange;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.options.ImmutableVersionOption;
import com.dremio.datastore.api.options.VersionOption;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

/**
 * Bridges legacy LegacyKVStore interface to the new KVStore interface.
 *
 * @param <K> key of type K.
 * @param <V> value of type V.
 */
public class LegacyKVStoreAdapter<K, V> implements LegacyKVStore<K, V> {

  private KVStore<K, V> underlyingStore;
  private VersionExtractor<V> versionExtractor;

  public LegacyKVStoreAdapter(KVStore<K, V> underlyingStore, VersionExtractor<V> versionExtractor) {
    this.underlyingStore = underlyingStore;
    this.versionExtractor = versionExtractor;
  }

  @Override
  public V get(K key) {
    Document<K, V> document = underlyingStore.get(key);
    return toValue(document);
  }

  @Override
  public List<V> get(List<K> keys) {
    Iterable<Document<K, V>> results = underlyingStore.get(keys);
    List<V> values = new ArrayList<>();
    results.forEach(document -> values.add(toValue(document)));
    return values;
  }

  @Override
  public void put(K key, V v) {
    if (versionExtractor != null) {
      String tag = versionExtractor.getTag(v);
      final KVStore.PutOption putOption;
      if (Strings.isNullOrEmpty(tag)) {
        putOption = KVStore.PutOption.CREATE;
      } else {
        putOption = new ImmutableVersionOption.Builder().setTag(tag).build();
      }
      // Update the tag on the original, because the underlying store
      // may create a new instance of the value, which our user would not
      // have a reference to.
      final Document<K, V> document = underlyingStore.put(key, v, putOption);
      versionExtractor.preCommit(v);
      versionExtractor.setTag(v, document.getTag());
    } else {
      underlyingStore.put(key, v);
    }
  }

  @Override
  public boolean contains(K key) {
    return underlyingStore.contains(key);
  }

  @Override
  public void delete(K key) {
    underlyingStore.delete(key);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterable<Map.Entry<K, V>> find(LegacyFindByRange<K> find) {
    FindByRange<K> findByRange =
      new ImmutableFindByRange.Builder()
        .setEnd(find.getEnd())
        .setStart(find.getStart())
        .setIsEndInclusive(find.isEndInclusive())
        .setIsStartInclusive(find.isStartInclusive()).build();
    return convertToMapEntry(underlyingStore.find(findByRange));
  }

  @Override
  public Iterable<Map.Entry<K, V>> find() {
    return convertToMapEntry(underlyingStore.find());
  }

  @Override
  public void delete(K key, String previousVersion) {
    // Interpret the absence of a previous version as an unvalidated delete.
    if (Strings.isNullOrEmpty(previousVersion)) {
      delete(key);
    } else {
      VersionOption versionOption = new ImmutableVersionOption.Builder().setTag(previousVersion).build();
      underlyingStore.delete(key, versionOption);
    }
  }

  @Override
  public KVAdmin getAdmin() {
    return underlyingStore.getAdmin();
  }

  @Override
  public String getName() {
    return underlyingStore.getName();
  }

  /**
   * Helper method to check whether the document is {@code null}, returns null if it is, otherwise it returns the value.
   * @param document the document to check for null and to obtain value from if not null.
   * @return null if {@param document} is {@code null}, otherwise, returns the value of the document.
   */
  protected V toValue(Document<K, V> document) {
    if (document == null) {
      return null;
    }
    if (versionExtractor != null) {
      versionExtractor.preCommit(document.getValue());
      versionExtractor.setTag(document.getValue(), document.getTag());
    }
    return document.getValue();
  }

  /**
   * Helper method to convert an Iterable of Document to an Iterable of Map.Entry
   * @param documents an Iterable of Documents to convert
   * @return an Iterable of Map.Entry converted from Documents
   */
  protected Iterable<Map.Entry<K, V>> convertToMapEntry(Iterable<Document<K, V>> documents) {
    return Iterables.transform(documents, new Function<Document<K, V>, Map.Entry<K, V>>() {
      @Override
      public Map.Entry<K, V> apply(Document<K, V> document) {
        return (document == null)? null : new AbstractMap.SimpleEntry<>(document.getKey(), toValue(document));
      }
    });
  }
}
