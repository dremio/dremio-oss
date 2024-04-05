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

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.util.Map;
import javax.inject.Provider;

/** Reflection settings store */
public class ReflectionSettingsStore {
  private static final String TABLE_NAME = "reflection_settings";

  private final Supplier<LegacyKVStore<CatalogEntityKey, AccelerationSettings>> store;

  public ReflectionSettingsStore(final Provider<LegacyKVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvstore provider required");
    store =
        Suppliers.memoize(
            new Supplier<LegacyKVStore<CatalogEntityKey, AccelerationSettings>>() {
              @Override
              public LegacyKVStore<CatalogEntityKey, AccelerationSettings> get() {
                return provider.get().getStore(StoreCreator.class);
              }
            });
  }

  public AccelerationSettings get(CatalogEntityKey key) {
    return store.get().get(key);
  }

  public void save(CatalogEntityKey key, AccelerationSettings settings) {
    store.get().put(key, settings);
  }

  public void delete(CatalogEntityKey key) {
    store.get().delete(key);
  }

  private static final class AccelerationSettingsVersionExtractor
      implements VersionExtractor<AccelerationSettings> {
    @Override
    public String getTag(AccelerationSettings value) {
      return value.getTag();
    }

    @Override
    public void setTag(AccelerationSettings value, String tag) {
      value.setTag(tag);
    }
  }

  /** {@link ReflectionSettingsStore} creator */
  public static final class StoreCreator
      implements LegacyKVStoreCreationFunction<CatalogEntityKey, AccelerationSettings> {
    @Override
    public LegacyKVStore<CatalogEntityKey, AccelerationSettings> build(
        LegacyStoreBuildingFactory factory) {
      return factory
          .<CatalogEntityKey, AccelerationSettings>newStore()
          .name(TABLE_NAME)
          .keyFormat(
              Format.wrapped(
                  CatalogEntityKey.class,
                  CatalogEntityKey::toString,
                  CatalogEntityKey::new,
                  Format.ofString()))
          .valueFormat(Format.ofProtostuff(AccelerationSettings.class))
          .versionExtractor(AccelerationSettingsVersionExtractor.class)
          .build();
    }
  }

  public Iterable<Map.Entry<CatalogEntityKey, AccelerationSettings>> getAll() {
    return store.get().find();
  }
}
