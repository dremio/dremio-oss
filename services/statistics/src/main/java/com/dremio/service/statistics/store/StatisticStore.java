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
package com.dremio.service.statistics.store;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.statistics.Statistic;
import com.dremio.service.statistics.proto.StatisticId;
import com.dremio.service.statistics.proto.StatisticMessage;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

/**
 * store for external reflections
 */
public class StatisticStore {
  private static final String TABLE_NAME = "statistic_store";
  private final Supplier<LegacyKVStore<StatisticId, StatisticMessage>> store;
  private final Cache<StatisticId, Optional<Statistic>> cache;

  public StatisticStore(final Provider<LegacyKVStoreProvider> provider, long max, long timeout) {
    Preconditions.checkNotNull(provider, "kvStore provider required");
    this.store = Suppliers.memoize(new Supplier<LegacyKVStore<StatisticId, StatisticMessage>>() {
      @Override
      public LegacyKVStore<StatisticId, StatisticMessage> get() {
        return provider.get().getStore(StoreCreator.class);
      }
    });
    this.cache = CacheBuilder.newBuilder()
      .maximumWeight(max)
      .weigher((Weigher<StatisticId, Optional<Statistic>>) (key, val) -> 1)
      .softValues()
      .expireAfterAccess(timeout, TimeUnit.MINUTES)
      .build();
  }

  public void save(StatisticId id, Statistic statistic) {
    statistic.setCreatedAt(System.currentTimeMillis());
    cache.put(id, Optional.of(statistic));
    store.get().delete(id);
    store.get().put(id, statistic.getStatisticMessage());
  }

  public Statistic get(StatisticId statisticId) {
    Optional<Statistic> stat = cache.getIfPresent(statisticId);
    if (stat != null) {
      return stat.orElse(null);
    }

    StatisticMessage statisticMessage = store.get().get(statisticId);
    if (statisticMessage == null) {
      cache.put(statisticId, Optional.empty());
      return null;
    }
    Statistic statistic = new Statistic(statisticMessage);
    cache.put(statisticId, Optional.of(statistic));
    return statistic;
  }

  public Iterable<Map.Entry<StatisticId, StatisticMessage>> getAll() {
    return store.get().find();
  }

  public void delete(StatisticId statisticId) {
    cache.invalidate(statisticId);
    store.get().delete(statisticId);
  }

  private static final class StatisticStoreExtractor implements VersionExtractor<StatisticMessage> {
    @Override
    public Long getVersion(StatisticMessage value) {
      return value.getVersion();
    }

    @Override
    public void setVersion(StatisticMessage value, Long version) {
      value.setVersion(version);
    }

    @Override
    public String getTag(StatisticMessage value) {
      return value.getTag();
    }

    @Override
    public void setTag(StatisticMessage value, String tag) {
      value.setTag(tag);
    }
  }


  /**
   * {@link StatisticStore} creator
   */
  public static final class StoreCreator implements LegacyKVStoreCreationFunction<StatisticId, StatisticMessage> {
    @Override
    public LegacyKVStore<StatisticId, StatisticMessage> build(LegacyStoreBuildingFactory factory) {
      return factory.<StatisticId, StatisticMessage>newStore()
        .name(TABLE_NAME)
        .keyFormat(Format.ofProtostuff(StatisticId.class))
        .valueFormat(Format.ofProtostuff(StatisticMessage.class))
        .versionExtractor(StatisticStore.StatisticStoreExtractor.class)
        .build();
    }
  }


}
