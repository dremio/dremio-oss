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
package com.dremio.service.reflection.store;

import com.dremio.datastore.KVUtil;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import javax.inject.Provider;

/** store the reflection entries */
public class ReflectionEntriesStore {
  private static final String TABLE_NAME = "reflection_entries";

  private final Supplier<LegacyKVStore<ReflectionId, ReflectionEntry>> store;

  public ReflectionEntriesStore(final Provider<LegacyKVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvstore provider cannot be null");
    this.store =
        Suppliers.memoize(
            new Supplier<LegacyKVStore<ReflectionId, ReflectionEntry>>() {
              @Override
              public LegacyKVStore<ReflectionId, ReflectionEntry> get() {
                return provider.get().getStore(StoreCreator.class);
              }
            });
  }

  public boolean contains(ReflectionId id) {
    return store.get().contains(id);
  }

  public ReflectionEntry get(ReflectionId id) {
    ReflectionEntry value = store.get().get(id);
    reflectionEntryGoalVersionUpdate(value);
    return value;
  }

  public void save(ReflectionEntry entry) {
    final long currentTime = System.currentTimeMillis();
    if (entry.getCreatedAt() == null) {
      entry.setCreatedAt(currentTime);
    }
    entry.setModifiedAt(currentTime);

    store.get().put(entry.getId(), entry);
  }

  public Iterable<ReflectionEntry> find() {
    return KVUtil.values(
        Iterables.transform(
            store.get().find(),
            (entry) -> {
              if (entry != null) {
                reflectionEntryGoalVersionUpdate(entry.getValue());
              }
              return entry;
            }));
  }

  public void delete(ReflectionId id) {
    store.get().delete(id);
  }

  private static final class ReflectionVersionExtractor
      implements VersionExtractor<ReflectionEntry> {
    @Override
    public String getTag(ReflectionEntry value) {
      return value.getTag();
    }

    @Override
    public void setTag(ReflectionEntry value, String tag) {
      value.setTag(tag);
    }
  }

  /** {@link ReflectionEntriesStore} creator */
  public static class StoreCreator
      implements LegacyKVStoreCreationFunction<ReflectionId, ReflectionEntry> {

    @Override
    public LegacyKVStore<ReflectionId, ReflectionEntry> build(LegacyStoreBuildingFactory factory) {
      return factory
          .<ReflectionId, ReflectionEntry>newStore()
          .name(TABLE_NAME)
          .keyFormat(Format.ofProtostuff(ReflectionId.class))
          .valueFormat(Format.ofProtostuff(ReflectionEntry.class))
          .versionExtractor(ReflectionVersionExtractor.class)
          .build();
    }
  }

  public void reflectionEntryGoalVersionUpdate(ReflectionEntry value) {
    if (value == null) {
      return;
    }
    if (Strings.isNullOrEmpty(value.getGoalVersion())) {
      String version =
          value.getLegacyGoalVersion() == null ? null : Long.toString(value.getLegacyGoalVersion());
      value.setGoalVersion(version);
    }
  }
}
