/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import javax.inject.Provider;

import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.KVUtil;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.VersionExtractor;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.store.Serializers.ReflectionEntrySerializer;
import com.dremio.service.reflection.store.Serializers.ReflectionIdSerializer;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * store the reflection entries
 */
public class ReflectionEntriesStore {
  private static final String TABLE_NAME = "reflection_entries";

  private final Supplier<KVStore<ReflectionId, ReflectionEntry>> store;

  public ReflectionEntriesStore(final Provider<KVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvstore provider cannot be null");
    this.store = Suppliers.memoize(new Supplier<KVStore<ReflectionId, ReflectionEntry>>() {
      @Override
      public KVStore<ReflectionId, ReflectionEntry> get() {
        return provider.get().getStore(StoreCreator.class);
      }
    });
  }

  public boolean contains(ReflectionId id) {
    return store.get().contains(id);
  }

  public ReflectionEntry get(ReflectionId id) {
    return store.get().get(id);
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
    return KVUtil.values(store.get().find());
  }

  public void delete(ReflectionId id) {
    store.get().delete(id);
  }

  private static final class ReflectionVersionExtractor implements VersionExtractor<ReflectionEntry> {
    @Override
    public Long getVersion(ReflectionEntry value) {
      return value.getVersion();
    }

    @Override
    public Long incrementVersion(ReflectionEntry value) {
      final Long current = value.getVersion();
      value.setVersion(Optional.fromNullable(current).or(-1L) + 1);
      return current;
    }

    @Override
    public void setVersion(ReflectionEntry value, Long version) {
      value.setVersion(version);
    }
  }

  /**
   * {@link ReflectionEntriesStore} creator
   */
  public static class StoreCreator implements StoreCreationFunction<KVStore<ReflectionId, ReflectionEntry>> {

    @Override
    public KVStore<ReflectionId, ReflectionEntry> build(StoreBuildingFactory factory) {
      return factory.<ReflectionId, ReflectionEntry>newStore()
        .name(TABLE_NAME)
        .keySerializer(ReflectionIdSerializer.class)
        .valueSerializer(ReflectionEntrySerializer.class)
        .versionExtractor(ReflectionVersionExtractor.class)
        .build();
    }
  }
}
