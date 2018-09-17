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
package com.dremio.datastore.indexed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import com.dremio.common.VM;
import com.dremio.datastore.CoreIndexedStore;
import com.dremio.datastore.CoreKVStore;
import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.KVStoreProvider.DocumentConverter;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.google.common.base.Throwables;

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

  private static final IndexKey ID_KEY = new IndexKey(IndexedStore.ID_FIELD_NAME, IndexedStore.ID_FIELD_NAME,
      String.class, null, false, true);

  private class ReindexThread extends Thread {
    private final Iterator<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> iterator;
    private final Object lock;
    private final AtomicBoolean cancelled;

    private volatile Throwable error = null;
    private volatile int elementCount = 0;

    public ReindexThread(Iterator<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> iterator, Object lock, AtomicBoolean cancelled) {
      this.iterator = iterator;
      this.lock = lock;
      this.cancelled = cancelled;
    }

    @Override
    public void run() {
      try {
        while (!cancelled.get()) {
          final Entry<KVStoreTuple<K>, KVStoreTuple<V>> entry;
          // Get the next element
          synchronized (lock) {
            if (!iterator.hasNext()) {
              break;
            }
            entry = iterator.next();
          }

          elementCount++;

          final Document doc = toDoc(entry.getKey(), entry.getValue());

          if (doc == null) {
            continue;
          }

          index.add(doc);
        }
      } catch (Throwable t) {
        cancelled.set(true);
        this.error = t;
      }
    }
  }

  @Override
  public int reindex() {
    // Delete all previous documents
    index.delete();

    // Switching index writer to reindexing mode
    final int[] resultRef = { 0 };
    final Throwable[] errorRef = { null };
    index.forReindexing(() -> {
      final int numThreads = VM.availableProcessors();
      final Iterator<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> iterator = find().iterator();
      final Object lock = new Object();

      // Start all workers

      final AtomicBoolean cancelled = new AtomicBoolean(false);
      final List<ReindexThread> workers = new ArrayList<>(numThreads);
      for(int i = 0; i < numThreads; i++) {
        final ReindexThread reindexThread = new ReindexThread(iterator, lock, cancelled);
        workers.add(reindexThread);
        reindexThread.start();
      }

      // Read all data and transfer it to workers
      for(final ReindexThread reindexThread: workers) {
        try {
          reindexThread.join();
        } catch (InterruptedException e) {
          // Propagate
          Thread.currentThread().interrupt();
        }
        resultRef[0] += reindexThread.elementCount;

        // Look if thread threw some exception
        final Throwable error = reindexThread.error;
        if (error == null) {
          continue;
        }

        // Chaining exception
        if (errorRef[0] == null) {
          errorRef[0] = error;
        } else {
          errorRef[0].addSuppressed(error);
        }
      }
    });

    // Check for error
    Throwable error = errorRef[0];
    if (error != null) {
      Throwables.throwIfUnchecked(error);
      throw new RuntimeException(error);
    }
    return resultRef[0];
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
    index(key, v);
  }

  private void index(KVStoreTuple<K> key, KVStoreTuple<V> v) {
    final Document document = toDoc(key, v);
    if (document != null) {
      index.update(keyAsTerm(key), document);
    }
  }

  private Document toDoc(KVStoreTuple<K> key, KVStoreTuple<V> value){
    final Document doc = new Document();
    final SimpleDocumentWriter documentWriter = new SimpleDocumentWriter(doc);
    converter.convert(documentWriter, key.getObject(), value.getObject());

    if (doc.getFields().isEmpty()) {
      return null;
    }

    documentWriter.write(ID_KEY, key.getSerializedBytes());

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

  @Override
  public KVAdmin getAdmin() {
    return new IndexedKVAdmin(base.getAdmin());
  }

  private class IndexedKVAdmin extends KVAdmin {

    private final KVAdmin delegate;

    public IndexedKVAdmin(KVAdmin delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    public int reindex() {
      return CoreIndexedStoreImpl.this.reindex();
    }

    @Override
    public void compactKeyValues() throws IOException {
      delegate.compactKeyValues();
    }

    @Override
    public String getStats() {
      StringBuilder sb = new StringBuilder();
      sb.append(delegate.getStats());
      sb.append("\tIndex Stats\n");
      sb.append("\t\t* live records: ");
      sb.append(index.getLiveRecords());
      sb.append("\n\t\t* deleted records: ");
      sb.append(index.getDeletedRecords());
      sb.append("\n");
      return sb.toString();
    }


  }

}
