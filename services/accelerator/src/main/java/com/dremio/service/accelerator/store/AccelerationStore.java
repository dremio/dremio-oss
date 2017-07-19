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
package com.dremio.service.accelerator.store;

import java.util.Iterator;

import javax.annotation.Nullable;
import javax.inject.Provider;

import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.KVUtil;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.store.converter.AccelerationConverter;
import com.dremio.service.accelerator.store.extractor.AccelerationVersionExtractor;
import com.dremio.service.accelerator.store.serializer.AccelerationIdSerializer;
import com.dremio.service.accelerator.store.serializer.AccelerationSerializer;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * A store that holds {@link Acceleration}
 */
public class AccelerationStore {

  private static final String TABLE_NAME = "acceleration";

  private final Provider<KVStoreProvider> provider;
  private IndexedStore<AccelerationId, Acceleration> store;

  public AccelerationStore(final Provider<KVStoreProvider> provider) {
    this.provider = provider;
  }

  public void start() {
    this.store = provider.get().getStore(AccelerationStoreCreator.class);
  }

  public Iterable<Acceleration> find() {
    return KVUtil.values(store.find());
  }

  public Iterable<Acceleration> find(final IndexedStore.FindByCondition condition) {
    return KVUtil.values(store.find(condition));
  }

  public void save(final Acceleration acceleration) {
    store.put(acceleration.getId(), acceleration);
  }

  public Optional<Acceleration> get(final AccelerationId id) {
    return Optional.fromNullable(store.get(id));
  }

  public void remove(final AccelerationId id) {
    store.delete(id);
  }

  public Optional<Acceleration> getByIndex(final IndexKey key, final String value) {
    final SearchQuery query = SearchQueryUtils.newTermQuery(key, value);

    final IndexedStore.FindByCondition condition = new IndexedStore.FindByCondition()
        .setOffset(0)
        .setLimit(1)
        .setCondition(query);

    final Iterable<Acceleration> result = find(condition);
    final Iterator<Acceleration> it = result.iterator();
    if (it.hasNext()) {
      return Optional.fromNullable(it.next());
    }

    return Optional.absent();
  }

  public Iterable<Acceleration> getAllByIndex(final IndexKey key, final String value) {
    final SearchQuery query = SearchQueryUtils.newTermQuery(key, value);
    final IndexedStore.FindByCondition condition = new IndexedStore.FindByCondition()
        .setCondition(query);

    return find(condition);
  }

  public Optional<Layout> getLayoutById(final LayoutId id) {
    final Optional<Acceleration> acceleration = getByIndex(AccelerationIndexKeys.LAYOUT_ID, id.getId());
    if (!acceleration.isPresent()) {
      return Optional.absent();
    }

    return Iterables.tryFind(AccelerationUtils.getAllLayouts(acceleration.get()), new Predicate<Layout>() {
      @Override
      public boolean apply(@Nullable final Layout current) {
        return id.equals(current.getId());
      }
    });
  }


  /**
   * Acceleration Store creator.
   */
  public static final class AccelerationStoreCreator implements StoreCreationFunction<IndexedStore<AccelerationId, Acceleration>> {

    @Override
    public IndexedStore<AccelerationId, Acceleration> build(StoreBuildingFactory factory) {
      return factory.<AccelerationId, Acceleration>newStore()
          .name(TABLE_NAME)
          .keySerializer(AccelerationIdSerializer.class)
          .valueSerializer(AccelerationSerializer.class)
          .versionExtractor(AccelerationVersionExtractor.class)
          .buildIndexed(AccelerationConverter.class);
    }

  }

}
