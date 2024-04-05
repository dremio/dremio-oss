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
package com.dremio.services.configuration;

import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.google.common.base.Preconditions;

/** General store to store k/v pairs. */
public class ConfigurationStore {
  private static final String CONFIG_STORE = "configuration";
  private final LegacyKVStore<String, ConfigurationEntry> store;

  public ConfigurationStore(final LegacyKVStoreProvider storeProvider) {
    Preconditions.checkNotNull(storeProvider, "kvStore provider required");
    store = storeProvider.getStore(ConfigurationStoreCreator.class);
  }

  public void put(String key, ConfigurationEntry value) {
    store.put(key, value);
  }

  public ConfigurationEntry get(String key) {
    return store.get(key);
  }

  public void delete(String key) {
    store.delete(key);
  }

  /** Support storage creator. */
  public static final class ConfigurationStoreCreator
      implements LegacyKVStoreCreationFunction<String, ConfigurationEntry> {
    @Override
    public LegacyKVStore<String, ConfigurationEntry> build(LegacyStoreBuildingFactory factory) {
      return factory
          .<String, ConfigurationEntry>newStore()
          .name(CONFIG_STORE)
          .keyFormat(Format.ofString())
          .valueFormat(Format.ofProtostuff(ConfigurationEntry.class))
          .versionExtractor(ConfigurationEntryVersionExtractor.class)
          .build();
    }
  }

  /** Version extractor */
  public static final class ConfigurationEntryVersionExtractor
      implements VersionExtractor<ConfigurationEntry> {
    @Override
    public String getTag(ConfigurationEntry value) {
      return value.getTag();
    }

    @Override
    public void setTag(ConfigurationEntry value, String tag) {
      value.setTag(tag);
    }
  }
}
