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

import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.job.proto.JobId;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.Map;
import javax.inject.Provider;

/** store for StatisticEntry */
public class StatisticEntriesStore {
  private static final String STORE_NAME = "statistic_entries_store";
  private final Supplier<LegacyKVStore<String, JobId>> store;

  public StatisticEntriesStore(final Provider<LegacyKVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvStore provider required");
    this.store =
        Suppliers.memoize(
            new Supplier<LegacyKVStore<String, JobId>>() {
              @Override
              public LegacyKVStore<String, JobId> get() {
                return provider.get().getStore(StoreCreator.class);
              }
            });
  }

  public void save(String tablePath, JobId entry) {
    String path = tablePath.toLowerCase();
    store.get().delete(path);
    store.get().put(path, entry);
  }

  public JobId get(String tablePath) {
    String path = tablePath.toLowerCase();
    return store.get().get(path);
  }

  public Iterable<Map.Entry<String, JobId>> getAll() {
    return store.get().find();
  }

  public void delete(String tablePath) {
    store.get().delete(tablePath);
  }

  /** {@link StatisticEntriesStore} creator */
  public static final class StoreCreator implements LegacyKVStoreCreationFunction<String, JobId> {
    @Override
    public LegacyKVStore<String, JobId> build(LegacyStoreBuildingFactory factory) {
      return factory
          .<String, JobId>newStore()
          .name(STORE_NAME)
          .keyFormat(Format.ofString())
          .valueFormat(Format.ofProtostuff(JobId.class))
          .build();
    }
  }
}
