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

import com.dremio.nessiemetadata.cacheLoader.DataplaneCacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieDataplaneCaffeineCache<K extends String, V>
    implements NessieDataplaneCache<K, V> {
  private final LoadingCache<K, V> loadingCache;
  private final boolean bypassCache;
  private final DataplaneCacheLoader<K, V> loader;
  private static final Logger logger = LoggerFactory.getLogger(NessieDataplaneCaffeineCache.class);

  public NessieDataplaneCaffeineCache(
      LoadingCache<K, V> loadingCache, boolean bypassCache, DataplaneCacheLoader<K, V> loader) {
    this.loadingCache = loadingCache;
    this.bypassCache = bypassCache;
    this.loader = loader;
  }

  @Override
  public @Nullable V get(@NonNull K var1) {
    if (bypassCache) {
      try {
        return loader.load(var1);
      } catch (Exception e) {
        logger.error("Failed to load dataplane cache", e);
        throw new LoadCacheException("Failed to load dataplane cache");
      }
    }
    return loadingCache.get(var1);
  }

  @Override
  public void close() throws Exception {}

  public static final class LoadCacheException extends RuntimeException {
    LoadCacheException(String message) {
      super(message);
    }
  }
}
