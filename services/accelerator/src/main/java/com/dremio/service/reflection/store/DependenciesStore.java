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

import java.util.List;
import java.util.Map;

import javax.inject.Provider;

import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.reflection.DependencyEntry;
import com.dremio.service.reflection.proto.ReflectionDependencies;
import com.dremio.service.reflection.proto.ReflectionDependencyEntry;
import com.dremio.service.reflection.proto.ReflectionId;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;

/**
 * stores the dependency graph
 */
public class DependenciesStore {
  private static final String TABLE_NAME = "dependencies";

  private final Supplier<LegacyKVStore<ReflectionId, ReflectionDependencies>> store;

  public DependenciesStore(final Provider<LegacyKVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvstore provider required");
    store  =Suppliers.memoize(new Supplier<LegacyKVStore<ReflectionId, ReflectionDependencies>>() {
      @Override
      public LegacyKVStore<ReflectionId, ReflectionDependencies> get() {
        return provider.get().getStore(StoreCreator.class);
      }
    });
  }

  public void save(ReflectionId id, Iterable<DependencyEntry> dependencies) {
    store.get().delete(id);
    List<ReflectionDependencyEntry> entries = FluentIterable.from(dependencies)
      .transform(new Function<DependencyEntry, ReflectionDependencyEntry>() {
        @Override
        public ReflectionDependencyEntry apply(DependencyEntry entry) {
          return entry.toProtobuf();
        }
      }).toList();
    store.get().put(id, new ReflectionDependencies()
      .setId(id)
      .setEntryList(entries));
  }

  public Iterable<Map.Entry<ReflectionId, ReflectionDependencies>> getAll() {
    return store.get().find();
  }

  public void delete(ReflectionId reflectionId) {
    store.get().delete(reflectionId);
  }

  private static final class DependenciesVersionExtractor implements VersionExtractor<ReflectionDependencies> {
    @Override public String getTag(ReflectionDependencies value) {
      return value.getTag();
    }

    @Override public void setTag(ReflectionDependencies value, String tag) {
      value.setTag(tag);
    }
  }

  /**
   * {@link DependenciesStore} creator
   */
  public static final class StoreCreator implements LegacyKVStoreCreationFunction<ReflectionId, ReflectionDependencies> {
    @Override
    public LegacyKVStore<ReflectionId, ReflectionDependencies> build(LegacyStoreBuildingFactory factory) {
      return factory.<ReflectionId, ReflectionDependencies>newStore()
        .name(TABLE_NAME)
        .keyFormat(Format.ofProtostuff(ReflectionId.class))
        .valueFormat(Format.ofProtostuff(ReflectionDependencies.class))
        .versionExtractor(DependenciesVersionExtractor.class)
        .build();
    }
  }
}
