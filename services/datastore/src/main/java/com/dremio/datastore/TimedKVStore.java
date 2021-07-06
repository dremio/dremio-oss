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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.dremio.context.TenantContext;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.telemetry.api.metrics.Histogram;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

/**
 * Times individual ops in the underlying kvstore.
 * @param <K>
 * @param <V>
 */
public class TimedKVStore<K, V> implements KVStore<K, V> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TimedKVStore.class);
  private static final int OP_LATENCY_THRESHOLD_IN_MS = 200;

  private enum Ops {
    get,
    getList,
    put,
    contains,
    delete,
    findByRange,
    findAll,
    applyForAllTenants,
    findByCondition,
    getCounts,
    findForAllTenants,
  }

  private final KVStore<K, V> delegate;
  private final Map<Ops, Histogram> metrics;

  public TimedKVStore(KVStore<K, V> delegate) {
    this.delegate = delegate;
    this.metrics = registerMetrics();
  }

  public static <K, V> TimedKVStore<K, V> of(KVStore<K,V> delegate) {
    return new TimedKVStore<>(delegate);
  }


  private Map<Ops, Histogram> registerMetrics() {
    final ImmutableMap.Builder<Ops, Histogram> builder = ImmutableMap.builder();
    for (Ops op : Ops.values()) {
      final Histogram hist = Metrics.newHistogram(Metrics.join(getMetricsPrefix(), op.name()), Metrics.ResetType.NEVER);
      builder.put(op, hist);
    }
    return builder.build();
  }

  protected String getMetricsPrefix() {
    return Metrics.join("kvstore", delegate.getName());
  }


  @Override
  public Document<K, V> get(K key, GetOption... options) {
    try(final OpTimer ctx = time(Ops.get)) {
      return delegate.get(key, options);
    }
  }

  @Override
  public Iterable<Document<K, V>> get(List<K> keys, GetOption... options) {
    try(final OpTimer ctx = time(Ops.getList)) {
      return delegate.get(keys, options);
    }
  }

  @Override
  public Document<K, V> put(K key, V value, PutOption... options) {
    try(final OpTimer ctx = time(Ops.put)) {
      return delegate.put(key, value, options);
    }
  }

  @Override
  public void delete(K key, DeleteOption... options) {
    try(final OpTimer ctx = time(Ops.delete)) {
      delegate.delete(key, options);
    }
  }

  @Override
  public boolean contains(K key, ContainsOption... options) {
    try(final OpTimer ctx = time(Ops.contains)) {
      return delegate.contains(key, options);
    }
  }

  @Override
  public Iterable<Document<K, V>> find(FindOption... options) {
    try(final OpTimer ctx = time(Ops.findAll)) {
      return delegate.find(options);
    }
  }

  @Override
  public Iterable<Document<K, V>> find(FindByRange<K> find, FindOption... options) {
    try(final OpTimer ctx = time(Ops.findByRange)) {
      return delegate.find(find, options);
    }
  }

  @Override
  public void applyForAllTenants(BiConsumer<K, V> consumer, ExecutorService executor, BiFunction<String, V, TenantContext> documentToTenantConverter, FindOption... options) {
    try(final OpTimer ctx = time(Ops.applyForAllTenants)) {
      delegate.applyForAllTenants(consumer, executor, documentToTenantConverter, options);
    }
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public KVAdmin getAdmin() {
    return delegate.getAdmin();
  }

  /**
   * tracks timer metric for the op & logs a warn message if the latency is above a threshold.
   */
  protected class OpTimer implements AutoCloseable {
    private final Stopwatch stopwatch;
    private final Ops op;
    public OpTimer(Ops op) {
      this.stopwatch = Stopwatch.createStarted();
      this.op = op;
    }

    @Override
    public void close() {
      final long elapsedMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      stopwatch.stop();
      if (elapsedMs > OP_LATENCY_THRESHOLD_IN_MS) {
        logger.warn("DHL kvstore: {} op: {} took(ms): {}", getName(), op.name(), elapsedMs);
      }

      //update histogram
      metrics.get(op).update(elapsedMs);
    }
  }

  protected OpTimer time(final Ops op) {
    return new OpTimer(op);
  }

  /**
   * Traces calls to an underlying indexed store.
   * @param <K> key type K.
   * @param <V> value type V.
   */
  public static class TimedIndexedStore<K,V> extends TimedKVStore<K,V> implements IndexedStore<K,V> {
    private final IndexedStore<K,V> indexedStore;

    TimedIndexedStore(IndexedStore<K,V> delegate) {
      super(delegate);
      this.indexedStore = delegate;
    }

    public static <K, V> TimedIndexedStore<K, V> of(IndexedStore<K,V> delegate) {
      return new TimedIndexedStore<>(delegate);
    }

    @Override
    public Iterable<Document<K, V>> find(FindByCondition find, FindOption ... options) {
      try(final OpTimer ctx = time(Ops.findByCondition)) {
        return indexedStore.find(find, options);
      }
    }

    @Override
    public List<Integer> getCounts(SearchTypes.SearchQuery... conditions) {
      try(final OpTimer ctx = time(Ops.getCounts)) {
        return indexedStore.getCounts(conditions);
      }
    }

    @Override
    public void applyForAllTenants(FindByCondition condition, BiConsumer<K, V> consumer, ExecutorService executor,
                                   BiFunction<String, V, TenantContext> tenantContextSupplier,
                                   FindOption... options) {
      try(final OpTimer ctx = time(Ops.applyForAllTenants)) {
        indexedStore.applyForAllTenants(condition, consumer, executor, tenantContextSupplier, options);
      }
    }
  }
}
