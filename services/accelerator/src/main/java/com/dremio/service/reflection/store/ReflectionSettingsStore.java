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

import javax.inject.Provider;

import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.VersionExtractor;
import com.dremio.exec.catalog.CatalogSourceDataCreator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * Reflection settings store
 */
public class ReflectionSettingsStore {
  private static final String TABLE_NAME = "reflection_settings";

  private final Supplier<KVStore<NamespaceKey, AccelerationSettings>> store;

  public ReflectionSettingsStore(final Provider<KVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvstore provider required");
    store = Suppliers.memoize(new Supplier<KVStore<NamespaceKey, AccelerationSettings>>() {
      @Override
      public KVStore<NamespaceKey, AccelerationSettings> get() {
        return provider.get().getStore(StoreCreator.class);
      }
    });
  }

  public AccelerationSettings get(NamespaceKey key) {
    return store.get().get(key);
  }

  public void save(NamespaceKey key, AccelerationSettings settings) {
    store.get().put(key, settings);
  }

  public void delete(NamespaceKey key) {
    store.get().delete(key);
  }

  private static final class AccelerationSettingsVersionExtractor implements VersionExtractor<AccelerationSettings> {
    @Override
    public Long getVersion(AccelerationSettings value) {
      return value.getVersion();
    }

    @Override
    public void setVersion(AccelerationSettings value, Long version) {
      value.setVersion(version == null ? 0 : version);
    }

    @Override
    public String getTag(AccelerationSettings value) {
      return value.getTag();
    }

    @Override
    public void setTag(AccelerationSettings value, String tag) {
      value.setTag(tag);
    }
  }

  private static final class AccelerationSettingsSerializer extends SchemaSerializer<AccelerationSettings> {
    AccelerationSettingsSerializer() {
      super(AccelerationSettings.getSchema());
    }
  }

  /**
   * {@link ReflectionSettingsStore} creator
   */
  public static final class StoreCreator implements StoreCreationFunction<KVStore<NamespaceKey, AccelerationSettings>> {
    @Override
    public KVStore<NamespaceKey, AccelerationSettings> build(StoreBuildingFactory factory) {
      return factory.<NamespaceKey, AccelerationSettings>newStore()
        .name(TABLE_NAME)
        .keySerializer(CatalogSourceDataCreator.NamespaceKeySerializer.class)
        .valueSerializer(AccelerationSettingsSerializer.class)
        .versionExtractor(AccelerationSettingsVersionExtractor.class)
        .build();
    }
  }
}
