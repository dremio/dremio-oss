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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

import com.dremio.common.VM;
import com.dremio.common.utils.OptimisticByteOutput;
import com.dremio.datastore.CoreIndexedStore;
import com.dremio.datastore.CoreKVStore;
import com.dremio.datastore.KVAdmin;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.RemoteDataStoreProtobuf;
import com.dremio.datastore.RemoteDataStoreUtils;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.IncrementCounter;
import com.dremio.datastore.api.options.KVStoreOptionUtility;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

/**
 * Implementation of core Indexed Store based on a Lucene search index.
 *
 * @param <K>
 * @param <V>
 */
public class CoreIndexedStoreImpl<K, V> implements CoreIndexedStore<K, V> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoreIndexedStoreImpl.class);

  private final CoreKVStore<K, V> base;
  private final DocumentConverter<K, V> converter;
  private final LuceneSearchIndex index;
  private final String name;
  private final boolean indexesViaPutOption;

  public CoreIndexedStoreImpl(
    String name,
    CoreKVStore<K, V> base,
    LuceneSearchIndex index,
    DocumentConverter<K, V> converter,
    boolean indexesViaPutOption) {
    super();
    this.name = name;
    this.base = base;
    this.converter = converter;
    this.index = index;
    this.indexesViaPutOption = indexesViaPutOption;
  }

  public static final IndexKey ID_KEY = IndexKey.newBuilder(CoreIndexedStore.ID_FIELD_NAME, CoreIndexedStore.ID_FIELD_NAME, String.class)
    .setStored(true)
    .build();

  private class ReindexThread extends Thread {
    private final Iterator<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> iterator;
    private final Object lock;
    private final AtomicBoolean cancelled;

    private volatile Throwable error = null;
    private volatile int elementCount = 0;

    public ReindexThread(Iterator<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> iterator, Object lock, AtomicBoolean cancelled) {
      this.iterator = iterator;
      this.lock = lock;
      this.cancelled = cancelled;
    }

    @Override
    public void run() {
      try {
        while (!cancelled.get()) {
          final com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>> entry;
          // Get the next element
          synchronized (lock) {
            if (!iterator.hasNext()) {
              break;
            }
            entry = iterator.next();
            elementCount++;
          }

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
      final Iterator<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> iterator = find().iterator();
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
  public Integer version() {
    return converter != null ? converter.getVersion() : null;
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
  public com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>> get(KVStoreTuple<K> key, GetOption... options) {
    final com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>> document = base.get(key, options);

    logKVPairsWithNullValues(document);
    return document;
  }

  @Override
  public Iterable<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindOption... options) {
    final Iterable<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> documents = base.find(options);

    if (logger.isDebugEnabled()) {
      return Iterables.transform(documents, document -> {
        logKVPairsWithNullValues(document);
        return document;
      });
    }

    return documents;
  }

  private void logKVPairsWithNullValues(com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>> document) {
    if (document != null && (document.getValue() == null || document.getValue().getObject() == null)) {
      logger.debug("Key {} in index store has no associated value", document.getKey());
    }
  }

  @Override
  public com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>> put(KVStoreTuple<K> key, KVStoreTuple<V> v, PutOption... options) {
    final PutOption[] filteredOptions = KVStoreOptionUtility.removeIndexPutOption(options);
    final com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>> doc = base.put(key, v, filteredOptions);

    if (indexesViaPutOption) {
      index(key, options);
    } else {
      KVStoreOptionUtility.checkIndexPutOptionIsNotUsed(options);
      index(key, v);
    }
    return doc;
  }

  private void index(KVStoreTuple<K> key, PutOption... options) {
    for (PutOption option : options) {
      switch (option.getPutOptionInfo().getType()) {
        case REMOTE_INDEX:
          final Document document = toDoc(key, option);
          if (document != null) {
            index.update(keyAsTerm(key), document);
          }
          break;
        default:
          logger.debug("PutOption type {} ignored while performing remote index.", option.getPutOptionInfo().getType());
          break;
      }
    }
  }

  private Document toDoc(KVStoreTuple<K> key, PutOption option) {
    final Document doc = new Document();
    final SimpleDocumentWriter documentWriter = new SimpleDocumentWriter(doc);

    RemoteDataStoreProtobuf.PutOptionInfo putOptionInfo = option.getPutOptionInfo();

    for (RemoteDataStoreProtobuf.PutRequestIndexField indexField : putOptionInfo.getIndexFieldsList()) {
      IndexKey indexKey = RemoteDataStoreUtils.toIndexKey(indexField.getKey());

      if (indexField.getValueDoubleCount() > 0) {
        indexField.getValueDoubleList().forEach(v -> documentWriter.write(indexKey, v));
      } else if (indexField.getValueInt32Count() > 0) {
        indexField.getValueInt32List().forEach(v -> documentWriter.write(indexKey, v));
      } else if (indexField.getValueInt64Count() > 0) {
        indexField.getValueInt64List().forEach(v -> documentWriter.write(indexKey, v));
      } else if (indexField.getValueBytesCount() > 0) {
        final byte[][] byteArray = new byte[indexField.getValueBytesList().size()][];
        for (int i = 0; i < indexField.getValueBytesList().size(); i++) {
          byteArray[i] = indexField.getValueBytes(i).toByteArray();
          final ByteString byteString = indexField.getValueBytes(i);

          final OptimisticByteOutput byteOutput = new OptimisticByteOutput(byteString.size());
          try {
            UnsafeByteOperations.unsafeWriteTo(byteString, byteOutput);
          } catch (IOException e) {
            throw new IllegalStateException(String.format("Problem reading binary data from field: %s", indexKey.getIndexFieldName()), e);
          }
          byteArray[i] = byteOutput.toByteArray();
        }

        documentWriter.write(indexKey, byteArray);
      } else if (indexField.getValueStringCount() > 0) {
        documentWriter.write(indexKey, indexField.getValueStringList().toArray(new String[indexField.getValueStringList().size()]));
      } else {
        throw new IllegalStateException(String.format("Unknown index field type for field name: %s", indexField.getKey().getIndexFieldName()));
      }
    }

    if (doc.getFields().isEmpty()) {
      return null;
    }

    documentWriter.write(ID_KEY, key.getSerializedBytes());

    return doc;
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
    converter.doConvert(documentWriter, key.getObject(), value.getObject());

    if (doc.getFields().isEmpty()) {
      return null;
    }

    documentWriter.write(ID_KEY, key.getSerializedBytes());

    return doc;
  }

  @Override
  public boolean contains(KVStoreTuple<K> key, ContainsOption... options) {
    return base.contains(key, options);
  }

  public static Term keyAsTerm(KVStoreTuple<?> key) {
    final byte[] keyBytes = key.getSerializedBytes();
    return new Term(CoreIndexedStore.ID_FIELD_NAME, new BytesRef(keyBytes));
  }

  @Override
  public void delete(KVStoreTuple<K> key, DeleteOption... options) {
    base.delete(key, options);
    index.deleteDocuments(keyAsTerm(key));
  }

  @Override
  public Iterable<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> get(List<KVStoreTuple<K>> keys, GetOption... options) {
    final Iterable<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> documents = base.get(keys, options);

    if (logger.isDebugEnabled()) {
      return Iterables.transform(documents, document -> {
        logKVPairsWithNullValues(document);
        return document;
      });
    }

    return documents;
  }

  @Override
  public Iterable<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindByRange<KVStoreTuple<K>> find, FindOption... options) {
    final Iterable<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> documents = base.find(find, options);

    if (logger.isDebugEnabled()) {
      return Iterables.transform(documents, document -> {
        logKVPairsWithNullValues(document);
        return document;
      });
    }

    return documents;
  }

  @Override
  public void bulkIncrement(Map<KVStoreTuple<K>, List<IncrementCounter>> keysToIncrement, IncrementOption option) {
    base.bulkIncrement(keysToIncrement, option);
  }

  @Override
  public void bulkDelete(List<KVStoreTuple<K>> keysToDelete) {
    base.bulkDelete(keysToDelete);
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
  public Iterable<com.dremio.datastore.api.Document<KVStoreTuple<K>, KVStoreTuple<V>>> find(FindByCondition condition, FindOption... options) {
    // The FindOptions currently can't be propagated anywhere. This needs to be revisited when there are FindOptions, since they could be
    // made part of the Lucene search.
    return new CoreSearchIterable<K, V>(
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

  @Override
  public String getName() {
    return name;
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
