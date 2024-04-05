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
package com.dremio.nessiemetadata;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.nessiemetadata.cache.NessieDataplaneCaffeineCache;
import com.dremio.nessiemetadata.cacheLoader.DataplaneCacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.junit.jupiter.api.Test;

public class TestNessieDataplaneCaffeineCache {
  @Test
  public void testBypassCache() throws Exception {
    LoadingCache<String, String> loadingCache = mock(LoadingCache.class);
    DataplaneCacheLoader<String, String> loader = mock(DataplaneCacheLoader.class);
    NessieDataplaneCaffeineCache<String, String> cache =
        new NessieDataplaneCaffeineCache(loadingCache, true, loader);

    when(loader.load("key")).thenReturn("value");

    cache.get("key");
    verify(loadingCache, times(0)).get("key");
    verify(loader, times(1)).load("key");
  }

  @Test
  public void testCacheLoad() throws Exception {
    LoadingCache<String, String> loadingCache = mock(LoadingCache.class);
    DataplaneCacheLoader<String, String> loader = mock(DataplaneCacheLoader.class);
    NessieDataplaneCaffeineCache<String, String> cache =
        new NessieDataplaneCaffeineCache<>(loadingCache, false, loader);

    when(loadingCache.get("key")).thenReturn("value");

    cache.get("key");
    verify(loadingCache, times(1)).get("key");
    verify(loader, times(0)).load("key");
  }
}
