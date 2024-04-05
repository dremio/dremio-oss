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
package com.dremio.nessiemetadata.cache;

import static com.dremio.nessiemetadata.cache.NessieMetadataCacheOptions.BYPASS_DATAPLANE_CACHE;
import static com.dremio.nessiemetadata.cache.NessieMetadataCacheOptions.DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES;
import static com.dremio.nessiemetadata.cache.NessieMetadataCacheOptions.DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS;

import com.dremio.nessiemetadata.cacheLoader.DataplaneCacheLoader;
import com.dremio.nessiemetadata.storeprovider.NessieDataplaneCacheStoreProvider;
import com.dremio.options.OptionManager;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;

public class NessieDataplaneCaffeineCacheProvider implements NessieDataplaneCacheProvider {
  public NessieDataplaneCaffeineCacheProvider() {}

  @Override
  public <K extends String, V> NessieDataplaneCache<K, V> get(
      OptionManager optionManager,
      DataplaneCacheLoader<K, V> loader,
      NessieDataplaneCacheStoreProvider storeProvider) {
    return new NessieDataplaneCaffeineCache<>(
        Caffeine.newBuilder()
            .maximumSize(optionManager.getOption(DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS))
            .softValues()
            .expireAfterAccess(
                optionManager.getOption(
                    DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES),
                TimeUnit.MINUTES)
            .build(loader),
        optionManager.getOption(BYPASS_DATAPLANE_CACHE),
        loader);
  }
}
