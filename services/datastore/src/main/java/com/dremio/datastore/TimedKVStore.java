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

import com.dremio.context.TenantContext;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.IncrementCounter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.telemetry.api.metrics.TimerUtils;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Times individual ops in the underlying kvstore.
 *
 * @param <K>
 * @param <V>
 */
public class TimedKVStore<K, V> implements KVStore<K, V> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TimedKVStore.class);

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
    reindex,
    bulkIncrement,
    bulkDelete
  }

  private final KVStore<K, V> delegate;

  public TimedKVStore(KVStore<K, V> delegate) {
    this.delegate = delegate;
  }

  /**
   * Creates a resource sample with tags for current operation and store name.
   *
   * @param op {@link Ops} the operation to create an autoclosable sample for
   * @return resourceSample {@link Timer.ResourceSample}
   */
  private Timer.ResourceSample timed(Ops op) {
    return TimerUtils.timedHistogram(
        "kvstore.operations",
        "Timed KV store operations",
        Duration.ofMillis(10),
        "name",
        delegate.getName(),
        "op",
        op.name());
  }

  /**
   * @param op {@link Ops} the operation being run
   * @param operation {@link Supplier} the function to time and run
   * @return
   */
  protected <V> V timedOperation(Ops op, Supplier<V> operation) {
    return TimerUtils.timedOperation(timed(op), operation);
  }

  protected void timedOperation(Ops op, Runnable operation) {
    TimerUtils.timedOperation(timed(op), operation);
  }

  public static <K, V> TimedKVStore<K, V> of(KVStore<K, V> delegate) {
    return new TimedKVStore<>(delegate);
  }

  @Override
  public Document<K, V> get(K key, GetOption... options) {
    return timedOperation(Ops.get, () -> delegate.get(key, options));
  }

  @Override
  public Iterable<Document<K, V>> get(List<K> keys, GetOption... options) {
    return timedOperation(Ops.getList, () -> delegate.get(keys, options));
  }

  @Override
  public Document<K, V> put(K key, V value, PutOption... options) {
    return timedOperation(Ops.put, () -> delegate.put(key, value, options));
  }

  @Override
  public void delete(K key, DeleteOption... options) {
    timedOperation(Ops.delete, () -> delegate.delete(key, options));
  }

  @Override
  public boolean contains(K key, ContainsOption... options) {
    return timedOperation(Ops.contains, () -> delegate.contains(key, options));
  }

  @Override
  public Iterable<Document<K, V>> find(FindOption... options) {
    return timedOperation(Ops.findAll, () -> delegate.find(options));
  }

  @Override
  public Iterable<Document<K, V>> find(FindByRange<K> find, FindOption... options) {
    return timedOperation(Ops.findByRange, () -> delegate.find(find, options));
  }

  @Override
  public void applyForAllTenants(
      BiConsumer<K, V> consumer,
      ExecutorService executor,
      BiFunction<String, V, TenantContext> documentToTenantConverter,
      FindOption... options) {
    timedOperation(
        Ops.applyForAllTenants,
        () -> delegate.applyForAllTenants(consumer, executor, documentToTenantConverter, options));
  }

  @Override
  public void bulkIncrement(
      Map<K, List<IncrementCounter>> keysToIncrement, IncrementOption option) {
    timedOperation(Ops.bulkIncrement, () -> delegate.bulkIncrement(keysToIncrement, option));
  }

  @Override
  public void bulkDelete(List<K> keysToDelete, DeleteOption... deleteOptions) {
    timedOperation(Ops.bulkDelete, () -> delegate.bulkDelete(keysToDelete, deleteOptions));
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
   * Traces calls to an underlying indexed store.
   *
   * @param <K> key type K.
   * @param <V> value type V.
   */
  public static class TimedIndexedStore<K, V> extends TimedKVStore<K, V>
      implements IndexedStore<K, V> {
    private final IndexedStore<K, V> indexedStore;

    TimedIndexedStore(IndexedStore<K, V> delegate) {
      super(delegate);
      this.indexedStore = delegate;
    }

    public static <K, V> TimedIndexedStore<K, V> of(IndexedStore<K, V> delegate) {
      return new TimedIndexedStore<>(delegate);
    }

    @Override
    public Iterable<Document<K, V>> find(FindByCondition find, FindOption... options) {
      return timedOperation(Ops.findByCondition, () -> indexedStore.find(find, options));
    }

    @Override
    public long reindex(FindByCondition findByCondition, FindOption... options) {
      return timedOperation(Ops.reindex, () -> indexedStore.reindex(findByCondition, options));
    }

    @Override
    public List<Integer> getCounts(SearchTypes.SearchQuery... conditions) {
      return timedOperation(Ops.getCounts, () -> indexedStore.getCounts(conditions));
    }

    @Override
    public void applyForAllTenants(
        FindByCondition condition,
        BiConsumer<K, V> consumer,
        ExecutorService executor,
        BiFunction<String, V, TenantContext> tenantContextSupplier,
        FindOption... options) {
      timedOperation(
          Ops.applyForAllTenants,
          () ->
              indexedStore.applyForAllTenants(
                  condition, consumer, executor, tenantContextSupplier, options));
    }

    @Override
    public Iterable<Document<K, V>> findOnAllTenants(
        FindByCondition condition, FindOption... options) {
      return timedOperation(
          Ops.findForAllTenants, () -> indexedStore.findOnAllTenants(condition, options));
    }

    @Override
    public Integer version() {
      return indexedStore.version();
    }
  }
}
