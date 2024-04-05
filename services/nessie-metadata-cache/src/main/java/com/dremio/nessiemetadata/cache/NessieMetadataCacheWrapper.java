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
import com.dremio.nessiemetadata.storeprovider.NessieMetadataCacheStoreProvider;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

public class NessieMetadataCacheWrapper implements AutoCloseable {
  private NessieMetadataCache store;

  private final NessieMetadataCacheStoreProvider storeProvider;
  private @Nullable NessieApiV2 nessieApiV2;
  private final long maxSize;
  private final long ttlMinutes;
  private final boolean bypassCache;

  public NessieMetadataCacheWrapper(
      NessieMetadataCacheStoreProvider storeProvider,
      @Nullable NessieApiV2 nessieApiV2,
      long maxSize,
      long ttlMinutes,
      boolean bypassCache) {
    this.storeProvider = storeProvider;
    this.nessieApiV2 = nessieApiV2;
    this.maxSize = maxSize;
    this.ttlMinutes = ttlMinutes;
    this.bypassCache = bypassCache;
    start();
  }

  public void invalidate(String userId) {
    store.delete(userId);
  }

  public Optional<Content> get(ImmutableTriple<ContentKey, ResolvedVersionContext, String> key) {
    Content val = store.get(key);
    if (val == null) {
      return Optional.empty();
    }
    return Optional.of(val);
  }

  public void start() {
    this.store = storeProvider.getStore(nessieApiV2, maxSize, ttlMinutes, bypassCache);
  }

  @Override
  public void close() throws Exception {
    storeProvider.close();
  }
}
