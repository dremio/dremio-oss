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

import com.dremio.datastore.CoreKVStore;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.ImmutableDocument;
import com.dremio.datastore.indexed.LuceneSearchIndex.Doc;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

/** An iterable over search items. */
public class CoreSearchIterable<K, V>
    implements Iterable<Document<KVStoreTuple<K>, KVStoreTuple<V>>> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CoreSearchIterable.class);

  private enum State {
    INIT,
    NEXT_PENDING,
    NEXT_USED,
    DONE
  }

  private final Query searchQuery;
  private final Sort sort;
  private final int pageSize;
  private final CoreKVStore<K, V> store;
  private final LuceneSearchIndex index;
  private final int offset;
  private final int limit;

  public CoreSearchIterable(
      CoreKVStore<K, V> store,
      LuceneSearchIndex index,
      Query searchQuery,
      Sort sort,
      int pageSize,
      int offset,
      int limit) {
    this.store = store;
    this.index = index;
    this.searchQuery = searchQuery;
    this.sort = sort;
    this.pageSize = pageSize;
    this.offset = offset;
    this.limit = limit;
  }

  @Override
  public Iterator<Document<KVStoreTuple<K>, KVStoreTuple<V>>> iterator() {
    return new SearchIterator();
  }

  private void logKVPairsWithNullValues(Document<KVStoreTuple<K>, KVStoreTuple<V>> document) {
    if (document != null
        && (document.getValue() == null || document.getValue().getObject() == null)) {
      logger.debug("Key {} in index store has no associated value", document.getKey());
    }
  }

  private class SearchIterator implements Iterator<Document<KVStoreTuple<K>, KVStoreTuple<V>>> {

    private int returned;
    private LuceneSearchIndex.SearchHandle searchHandle;
    private Iterator<Document<KVStoreTuple<K>, KVStoreTuple<V>>> docs;
    private Document<KVStoreTuple<K>, KVStoreTuple<V>> pendingDoc;
    private Doc endLastIterator;

    private State state = State.INIT;

    public SearchIterator() {}

    private Iterator<Document<KVStoreTuple<K>, KVStoreTuple<V>>> updateIterator(
        List<Doc> documents) {

      final List<KVStoreTuple<K>> keys =
          Lists.transform(
              documents,
              new Function<Doc, KVStoreTuple<K>>() {
                @Override
                public KVStoreTuple<K> apply(Doc input) {
                  return store.newKey().setSerializedBytes(input.getKey());
                }
              });

      final Iterable<Document<KVStoreTuple<K>, KVStoreTuple<V>>> values = store.get(keys);

      List<Document<KVStoreTuple<K>, KVStoreTuple<V>>> entries = new ArrayList<>(documents.size());
      for (final Document<KVStoreTuple<K>, KVStoreTuple<V>> doc : values) {
        if (doc != null && doc.getValue() != null && doc.getValue().getObject() != null) {
          // since the search index can be slightly behind the actual data, make sure to only
          // include non-null tuples.
          entries.add(
              new ImmutableDocument.Builder<KVStoreTuple<K>, KVStoreTuple<V>>()
                  .setKey(doc.getKey())
                  .setValue(doc.getValue())
                  .setTag(doc.getTag())
                  .build());
        }
        logKVPairsWithNullValues(doc);
      }

      return entries.iterator();
    }

    /**
     * Since we need to get the next value before we know it, we get it as part of hasNext() and
     * then memoize it on future invocations.
     */
    @Override
    public boolean hasNext() {
      try {
        if (state == State.DONE) {
          return false;
        }

        if (state == State.NEXT_PENDING) {
          return true;
        }

        if (state == State.INIT) {
          Preconditions.checkState(searchHandle == null);
          searchHandle = index.createSearchHandle();

          List<Doc> documents =
              index.search(searchHandle, searchQuery, Math.min(pageSize, limit), sort, offset);
          if (documents.isEmpty()) {
            changeStateToDone();
            return false;
          }
          docs = updateIterator(documents);
          endLastIterator = documents.get(documents.size() - 1);
        }

        /**
         * Needs to be a while because the documents returned from the index may no longer exist in
         * the kvstore (since it can be slightly behind). As such, we'll keep looking for matched
         * documents until we find one or finish iterating.
         */
        while (!docs.hasNext()) {
          assert endLastIterator != null;
          int searchPageSize = Math.min(pageSize, limit - returned);
          List<Doc> documents =
              index.searchAfter(searchHandle, searchQuery, searchPageSize, sort, endLastIterator);
          if (documents.isEmpty()) {
            changeStateToDone();
            return false;
          }
          docs = updateIterator(documents);
          endLastIterator = documents.get(documents.size() - 1);
        }

        pendingDoc = docs.next();
        state = State.NEXT_PENDING;
        return true;
      } catch (IOException ex) {
        throw Throwables.propagate(ex);
      }
    }

    @Override
    public Document<KVStoreTuple<K>, KVStoreTuple<V>> next() {
      Preconditions.checkArgument(state == State.NEXT_PENDING);
      state = State.NEXT_USED;
      assert pendingDoc != null;

      returned++;
      if (returned >= limit) {
        changeStateToDone();
      }
      Document<KVStoreTuple<K>, KVStoreTuple<V>> entry = pendingDoc;
      pendingDoc = null;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    private void changeStateToDone() {
      if (searchHandle != null) {
        searchHandle.close();
        searchHandle = null;
      }
      state = State.DONE;
    }
  }
}
