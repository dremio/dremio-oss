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
package com.dremio.datastore.indexed;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import com.dremio.datastore.CoreIndexedStore;
import com.dremio.datastore.CoreKVStore;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.FindByCondition;

/**
 * Implementation of {@link AuxiliaryIndex} that works with {@link CoreKVStore} and {@link LuceneSearchIndex}
 */
public class AuxiliaryIndexImpl<K, V, T> implements AuxiliaryIndex<K, V, T>{
  private final CoreKVStore<K, V> store;
  private final LuceneSearchIndex index;
  private final DocumentConverter<K, T> converter;
  private final CoreIndexedStore<K, V> coreIndexedStore;

  public AuxiliaryIndexImpl(
    String name, CoreKVStore<K, V> store,
    LuceneSearchIndex index,
    Class<? extends DocumentConverter<K, T>> converter
  ) throws IllegalAccessException, InstantiationException {
    this.store = store;
    this.index = index;
    this.converter = converter.newInstance();

    // we don't need to pass in a DocumentConverter as we handle our own indexing
    this.coreIndexedStore = new CoreIndexedStoreImpl<>(name, store, index, null, false);
  }

  @Override
  public void index(K key, T indexValue) {
    KVStoreTuple<K> newKey = store.newKey().setObject(key);
    final Document document = toDoc(newKey, indexValue);

    if (document != null) {
      index.update(keyAsTerm(newKey), document);
    }
  }

  @Override
  public Iterable<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindByCondition condition) {
    return coreIndexedStore.find(condition);
  }

  private Term keyAsTerm(KVStoreTuple<K> key){
    final byte[] keyBytes = key.getSerializedBytes();
    return new Term(CoreIndexedStore.ID_FIELD_NAME, new BytesRef(keyBytes));
  }

  private Document toDoc(KVStoreTuple<K> key, T value){
    final Document doc = new Document();
    final SimpleDocumentWriter documentWriter = new SimpleDocumentWriter(doc);
    converter.doConvert(documentWriter, key.getObject(), value);

    doc.add(new StringField(CoreIndexedStore.ID_FIELD_NAME, new BytesRef(key.getSerializedBytes()), Field.Store.YES));

    return doc;
  }
}
