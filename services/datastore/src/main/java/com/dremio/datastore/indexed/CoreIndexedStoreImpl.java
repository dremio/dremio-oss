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
package com.dremio.datastore.indexed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import com.dremio.datastore.CoreIndexedStore;
import com.dremio.datastore.CoreKVStore;
import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.KVStoreProvider.DocumentConverter;
import com.dremio.datastore.KVStoreProvider.DocumentWriter;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SortOrder;

/**
 * Implementation of core Indexed Store based on a Lucene search index.
 *
 * @param <K>
 * @param <V>
 */
public class CoreIndexedStoreImpl<K, V> implements CoreIndexedStore<K, V> {

  private final CoreKVStore<K, V> base;
  private final DocumentConverter<K, V> converter;
  private final LuceneSearchIndex index;
  private final String name;

  public CoreIndexedStoreImpl(
    String name,
    CoreKVStore<K, V> base,
    LuceneSearchIndex index,
    KVStoreProvider.DocumentConverter<K, V> converter) {
    super();
    this.name = name;
    this.base = base;
    this.converter = converter;
    this.index = index;
  }

  @Override
  public KVStoreTuple<K> newKey() {
    return base.newKey();
  }

  @Override
  public KVStoreTuple<V> newValue() {
    return base.newValue();
  }

  @Override
  public KVStoreTuple<V> get(KVStoreTuple<K> key) {
    return base.get(key);
  }

  @Override
  public Iterable<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> find() {
    return base.find();
  }

  @Override
  public void put(KVStoreTuple<K> key, KVStoreTuple<V> v) {
    base.put(key, v);
    final Document document = toDoc(key, v);
    if (document != null) {
      index.update(keyAsTerm(key), document);
    }
  }

  private Document toDoc(KVStoreTuple<K> key, KVStoreTuple<V> value){
    final Document doc = new Document();
    converter.convert(new DocumentWriter() {

      @Override
      public void write(IndexKey key, Double value) {
        if(value != null){
          key.addToDoc(doc, value);
        }
      }

      @Override
      public void write(IndexKey key, Integer value) {
        if(value != null){
          key.addToDoc(doc, value);
        }
      }

      @Override
      public void write(IndexKey key, Long value) {
        if(value != null){
          key.addToDoc(doc, value);
        }
      }

      public void write(IndexKey key, byte[]... values) {
        key.addToDoc(doc, values);
      }

      @Override
      public void write(IndexKey key, String... values) {
        key.addToDoc(doc, values);
      }
    }, key.getObject(), value.getObject());

    if (doc.getFields().isEmpty()) {
      return null;
    }

    doc.add(new StringField(IndexedStore.ID_FIELD_NAME, new BytesRef(key.getSerializedBytes()), Store.YES));

    return doc;
  }

  @Override
  public boolean checkAndPut(KVStoreTuple<K> key, KVStoreTuple<V> oldValue, KVStoreTuple<V> newValue) {
    boolean changed = base.checkAndPut(key, oldValue, newValue);
    if (changed) {
      final Document d = toDoc(key, newValue);
      if (d != null) {
        if (oldValue == null) {
          index.add(d);
        } else {
          index.update(keyAsTerm(key), d);
        }
      }
    }
    return changed;
  }

  @Override
  public boolean contains(KVStoreTuple<K> key) {
    return base.contains(key);
  }

  @Override
  public boolean checkAndDelete(KVStoreTuple<K> key, KVStoreTuple<V> value) {
    boolean deleted = base.checkAndDelete(key, value);
    if(deleted){
      index.deleteDocuments(keyAsTerm(key));
    }
    return deleted;
  }

  private Term keyAsTerm(KVStoreTuple<K> key){
    final byte[] keyBytes = key.getSerializedBytes();
    return new Term(IndexedStore.ID_FIELD_NAME, new BytesRef(keyBytes));
  }

  @Override
  public void delete(KVStoreTuple<K> key) {
    base.delete(key);
    index.deleteDocuments(keyAsTerm(key));
  }

  @Override
  public List<KVStoreTuple<V>> get(List<KVStoreTuple<K>> keys) {
    return base.get(keys);
  }

  @Override
  public Iterable<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindByRange<KVStoreTuple<K>> find) {
    return base.find(find);
  }

  @Override
  public void delete(KVStoreTuple<K> key, long previousVersion) {
    base.delete(key, previousVersion);
    index.deleteDocuments(keyAsTerm(key));
  }

  @Override
  public List<Integer> getCounts(SearchQuery... queries) {
    List<Query> luceneQueries = new ArrayList<>(queries.length);
    for(SearchQuery query: queries) {
      luceneQueries.add(LuceneQueryConverter.INSTANCE.toLuceneQuery(query));
    }
    return index.count(luceneQueries);
  }

  @Override
  public Iterable<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindByCondition condition) {
    return new CoreSearchIterable<>(
      base,
      index,
      LuceneQueryConverter.INSTANCE.toLuceneQuery(condition.getCondition()),
      toLuceneSort(condition.getSort()),
      condition.getPageSize(),
      condition.getOffset(),
      condition.getLimit());
  }

  private Sort toLuceneSort(List<SearchFieldSorting> orderings) {
    if (orderings.isEmpty()) {
      return new Sort();
    }

    SortField[] sortFields = new SortField[orderings.size()];
    int i = 0;
    for(SearchFieldSorting ordering: orderings) {
      final SortField.Type fieldType;
      switch(ordering.getType()) {
      case STRING:
        fieldType = SortField.Type.STRING;
        break;
      case LONG:
        fieldType = SortField.Type.LONG;
        break;
      case DOUBLE:
        fieldType = SortField.Type.DOUBLE;
        break;
      case INTEGER:
        fieldType = SortField.Type.INT;
        break;
      default:
        throw new AssertionError("Unknown field type: " + ordering.getType());
      }
      sortFields[i++] = new SortField(ordering.getField(), fieldType, ordering.getOrder() != SortOrder.ASCENDING);
    }

    return new Sort(sortFields);
  }
}
