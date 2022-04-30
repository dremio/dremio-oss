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
import java.util.function.Supplier;

import com.dremio.common.tracing.TracingUtils;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;

import io.opentracing.Tracer;

/**
 * Traces calls to an underlying KV store.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
class TracingKVStore<K, V> implements KVStore<K, V> {

  public static final String METHOD_TAG = "method";
  public static final String TABLE_TAG = "table";
  public static final String OPERATION_NAME = "kvstore_request";
  public static final String CREATOR_TAG = "creator";

  private final Tracer tracer;
  private final KVStore<K, V> delegate;
  private final String creatorName;
  private final String tableName;

  TracingKVStore(String creatorName, Tracer tracer, KVStore<K,V> delegate) {
    this.tracer = tracer;
    this.creatorName = creatorName;
    this.delegate = delegate;
    this.tableName = delegate.getName();
  }

  public static <K, V> TracingKVStore<K, V> of(String name, Tracer tracer, KVStore<K,V> delegate) {
    return new TracingKVStore<>(name, tracer, delegate);
  }

  protected <R> R trace(String methodName, Supplier<R> method) {
    return TracingUtils.trace(method,
      tracer,
      OPERATION_NAME,
      METHOD_TAG, methodName,
      TABLE_TAG, tableName,
      CREATOR_TAG, creatorName
    );
  }

  @Override
  public Document<K, V> get(K key, GetOption... options) {
    return trace("get", () -> delegate.get(key, options));
  }

  @Override
  public Iterable<Document<K, V>> get(List<K> keys, GetOption... options) {
    return trace("getList", () -> delegate.get(keys, options));
  }

  @Override
  public boolean contains(K key, ContainsOption... options) {
    return trace("contains", () -> delegate.contains(key, options));
  }

  @Override
  public Document<K, V> put(K key, V value, PutOption... options) {
    return trace("put", () -> delegate.put(key, value, options));
  }

  @Override
  public Iterable<Document<K, V>> find(FindOption... options) {
    return trace("find", () -> delegate.find(options));
  }

  @Override
  public Iterable<Document<K, V>> find(FindByRange<K> find, FindOption... options) {
    return trace("findByRange", () -> delegate.find(find, options));
  }

  @Override
  public void delete(K key, DeleteOption... options) {
    trace("delete", () -> delegate.delete(key, options));
  }

  @Override
  public KVAdmin getAdmin() {
    return trace("getAdmin", delegate::getAdmin);
  }

  protected void trace(String methodName, Runnable method) {
    trace(methodName, () -> {
      method.run();
      return null;
    });
  }

  @Override
  public String getName() {
    return delegate.getName();
  }


  /**
   * Traces calls to an underlying indexed store.
   * @param <K> key type K.
   * @param <V> value type V.
   */
  public static class TracingIndexedStore<K,V> extends TracingKVStore<K,V> implements IndexedStore<K,V> {
    private final IndexedStore<K,V> indexedStore;

    TracingIndexedStore(String name, Tracer tracer, IndexedStore<K,V> delegate) {
      super(name, tracer, delegate);
      this.indexedStore = delegate;
    }

    public static <K, V> TracingIndexedStore<K, V> of(String name, Tracer tracer, IndexedStore<K,V> delegate) {
      return new TracingIndexedStore<>(name, tracer, delegate);
    }

    @Override
    public Iterable<Document<K, V>> find(FindByCondition find, FindOption ... options) {
      return trace("findByCondition", () -> indexedStore.find(find, options));
    }

    @Override
    public List<Integer> getCounts(SearchTypes.SearchQuery... conditions) {
      return trace("getCounts", () -> indexedStore.getCounts(conditions));
    }

    @Override
    public Integer version() {
      return indexedStore.version();
    }
  }
}
