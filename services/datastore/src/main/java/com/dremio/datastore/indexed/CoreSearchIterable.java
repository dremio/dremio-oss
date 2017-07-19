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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import com.dremio.datastore.CoreKVStore;
import com.dremio.datastore.KVStoreTuple;
import com.dremio.datastore.indexed.LuceneSearchIndex.Doc;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * An iterable over search items.
 */
public class CoreSearchIterable<K, V> implements Iterable<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> {

  private enum State {
    INIT, NEXT_PENDING, NEXT_USED, DONE
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
    int limit
  ) {
    this.store = store;
    this.index = index;
    this.searchQuery = searchQuery;
    this.sort = sort;
    this.pageSize = pageSize;
    this.offset = offset;
    this.limit = limit;
  }

  @Override
  public Iterator<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> iterator() {
    return new SearchIterator();
  }

  private class SearchIterator implements Iterator<Entry<KVStoreTuple<K>, KVStoreTuple<V>>> {

    private long returned;
    private Iterator<DocEntry> docs;
    private DocEntry pendingDoc;
    private Doc endLastIterator;

    private State state = State.INIT;

    public SearchIterator() {
    }

    private Iterator<DocEntry> updateIterator(List<Doc> documents){

      final List<KVStoreTuple<K>> keys = Lists.transform(documents, new Function<Doc, KVStoreTuple<K>>(){
        @Override
        public KVStoreTuple<K> apply(Doc input) {
          return store.newKey().setSerializedBytes(input.getKey());
        }});

      final List<KVStoreTuple<V>> values = store.get(keys);

      List<DocEntry> entries = new ArrayList<>(documents.size());
      for (int i =0; i < documents.size(); i++) {
        KVStoreTuple<V> tuple = values.get(i);
        if(tuple.getObject() != null){
          // since the search index can be slightly behind the actual data, make sure to only include non-null tuples.
          entries.add(new DocEntry(documents.get(i), keys.get(i), tuple));
        }
      }

      return entries.iterator();
    }

    /**
     * Since we need to get the next value before we know it, we get it as part
     * of hasNext() and then memoize it on future invocations.
     */
    @Override
    public boolean hasNext() {
      try{
        if(state == State.DONE) {
          return false;
        }

        if(state == State.NEXT_PENDING){
          return true;
        }

        if(state == State.INIT) {
          List<Doc> documents = index.search(searchQuery, Math.min(pageSize, limit), sort, offset);
          if(documents.isEmpty()){
            state = State.DONE;
            return false;
          }
          docs = updateIterator(documents);
          endLastIterator = documents.get(documents.size() - 1);

        }

        /**
         * Needs to be a while because the documents returned from the index may
         * no longer exist in the kvstore (since it can be slightly behind). As
         * such, we'll keep looking for matched documents until we find one or
         * finish iterating.
         */
        while (!docs.hasNext()) {
          assert endLastIterator != null;
          List<Doc> documents = index.searchAfter(searchQuery, pageSize, sort, endLastIterator);
          if(documents.isEmpty()){
            state = State.DONE;
            return false;
          }
          docs = updateIterator(documents);
          endLastIterator = documents.get(documents.size() - 1);
        }

        pendingDoc = docs.next();
        state = State.NEXT_PENDING;
        return true;
      } catch(IOException ex) {
        throw Throwables.propagate(ex);
      }

  }

    @Override
    public Entry<KVStoreTuple<K>, KVStoreTuple<V>> next() {
      Preconditions.checkArgument(state == State.NEXT_PENDING);
      state = State.NEXT_USED;
      assert pendingDoc != null;

      returned++;
      if(returned >= limit){
        state = State.DONE;
      }
      DocEntry entry = pendingDoc;
      pendingDoc = null;
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  private class DocEntry implements Map.Entry<KVStoreTuple<K>, KVStoreTuple<V>> {
    private final Doc doc;
    private final KVStoreTuple<K> key;
    private KVStoreTuple<V> value;

    public DocEntry(Doc doc, KVStoreTuple<K> key, KVStoreTuple<V> value) {
      super();
      this.doc = doc;
      this.key = key;
      this.value = value;
    }

    @Override
    public KVStoreTuple<K> getKey() {
      return key;
    }

    @Override
    public KVStoreTuple<V> getValue() {
      return value;
    }

    @Override
    public KVStoreTuple<V> setValue(KVStoreTuple<V> value) {
      throw new UnsupportedOperationException();
    }

  }

}
