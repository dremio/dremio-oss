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
package com.dremio.services.configuration;

import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.StringSerializer;
import com.dremio.datastore.VersionExtractor;
import com.dremio.service.accelerator.store.serializer.SchemaSerializer;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.google.common.base.Preconditions;

/**
 * General store to store k/v pairs.
 */
public class ConfigurationStore {
  private static final String CONFIG_STORE = "configuration";
  private final KVStore<String, ConfigurationEntry> store;

  public ConfigurationStore(final KVStoreProvider storeProvider) {
    Preconditions.checkNotNull(storeProvider, "kvStore provider required");
    store = storeProvider.getStore(ConfigurationStoreCreator.class);
  }

  public void put(String key, ConfigurationEntry value) {
    store.put(key, value);
  }

  public ConfigurationEntry get(String key) {
    return store.get(key);
  }

  /**
   * Support storage creator.
   */
  public static final class ConfigurationStoreCreator implements StoreCreationFunction<KVStore<String, ConfigurationEntry>> {
    @Override
    public KVStore<String, ConfigurationEntry> build(StoreBuildingFactory factory) {
      return factory.<String, ConfigurationEntry>newStore()
        .name(CONFIG_STORE)
        .keySerializer(StringSerializer.class)
        .valueSerializer(ConfigurationEntrySerializer.class)
        .versionExtractor(ConfigurationEntryVersionExtractor.class)
        .build();
    }
  }

  /**
   * Serializer used for serializing cluster identity.
   */
  private static final class ConfigurationEntrySerializer extends SchemaSerializer<ConfigurationEntry> {
    ConfigurationEntrySerializer() {
      super(ConfigurationEntry.getSchema());
    }
  }

  /**
   * Version extractor
   */
  public static final class ConfigurationEntryVersionExtractor implements VersionExtractor<ConfigurationEntry> {
    @Override
    public Long getVersion(ConfigurationEntry value) {
      return value.getVersion();
    }

    @Override
    public Long incrementVersion(ConfigurationEntry value) {
      final Long currentVersion = value.getVersion();
      value.setVersion(currentVersion != null ? currentVersion + 1 : 0);
      return currentVersion;
    }

    @Override
    public void setVersion(ConfigurationEntry value, Long version) {
      value.setVersion(version);
    }
  }
}
