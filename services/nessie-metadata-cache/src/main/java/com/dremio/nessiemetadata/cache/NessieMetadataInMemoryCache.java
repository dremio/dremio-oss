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

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.telemetry.api.metrics.MetricsInstrumenter;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

public class NessieMetadataInMemoryCache implements NessieMetadataCache {
  private final LoadingCache<ImmutableTriple<ContentKey, ResolvedVersionContext, String>, Content>
      cache;
  private final NessieApiV2 nessieApi;
  private final boolean bypassCache;
  private static final MetricsInstrumenter metrics =
      new MetricsInstrumenter(NessieMetadataInMemoryCache.class);

  public NessieMetadataInMemoryCache(
      NessieApiV2 nessieApi, long maxSize, long ttlMinutes, boolean bypassCache) {
    this.cache =
        Caffeine.newBuilder()
            .maximumSize(maxSize)
            .softValues()
            .expireAfterWrite(ttlMinutes, TimeUnit.MINUTES)
            .build(new NessieContentCacheLoader());
    this.nessieApi = nessieApi;
    this.bypassCache = bypassCache;
  }

  @Override
  public void delete(String userId) {
    throw new UnsupportedOperationException("We do not support deleting keys from in memory cache");
  }

  @Override
  public Content get(ImmutableTriple<ContentKey, ResolvedVersionContext, String> key) {
    if (bypassCache) {
      return loadNessieContent(key).orElse(null);
    }
    return cache.get(key);
  }

  private final class NessieContentCacheLoader
      implements CacheLoader<ImmutableTriple<ContentKey, ResolvedVersionContext, String>, Content> {
    @Override
    public Content load(ImmutableTriple<ContentKey, ResolvedVersionContext, String> triple) {
      String userId =
          triple.right; // Unused because RequestContext is already set. However, required for the
      // cache key so that entries are unique per user.
      Preconditions.checkArgument(
          userId.equals(
              RequestContext.current()
                  .get(UserContext.CTX_KEY)
                  .getUserId())); // Defensive check only
      return loadNessieContent(triple).orElse(null);
    }
  }

  @WithSpan
  private Optional<Content> loadNessieContent(
      ImmutableTriple<ContentKey, ResolvedVersionContext, String> key) {
    return metrics.log(
        "loadNessieContent",
        () -> NessieContentLoader.loadNessieContent(nessieApi, key.left, key.middle));
  }
}
