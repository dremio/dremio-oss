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

import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.reflection.proto.RefreshRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * KVStore to store refresh requests on physical datasets
 */
public class RefreshRequestsStore {
  private static final String TABLE_NAME = "refresh_requests";

  private final Supplier<LegacyKVStore<String, RefreshRequest>> store;

  public RefreshRequestsStore(final Provider<LegacyKVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvStore provider required");
    store = Suppliers.memoize(new Supplier<LegacyKVStore<String, RefreshRequest>>() {
      @Override
      public LegacyKVStore<String, RefreshRequest> get() {
        return provider.get().getStore(StoreCreator.class);
      }
    });
  }

  public void save(String key, RefreshRequest request) {
    store.get().put(key, request);
  }

  public RefreshRequest get(String key) {
    return store.get().get(key);
  }

  private static final class VExtractor implements VersionExtractor<RefreshRequest> {
    @Override
    public String getTag(RefreshRequest value) {
      return value.getTag();
    }

    @Override
    public void setTag(RefreshRequest value, String tag) {
      value.setTag(tag);
    }
  }

  /**
   * {@link RefreshRequestsStore} creator
   */
  public static final class StoreCreator implements LegacyKVStoreCreationFunction<String, RefreshRequest> {
    @Override
    public LegacyKVStore<String, RefreshRequest> build(LegacyStoreBuildingFactory factory) {
      return factory.<String, RefreshRequest>newStore()
        .name(TABLE_NAME)
        .keyFormat(Format.ofString())
        .valueFormat(Format.ofProtostuff(RefreshRequest.class))
        .versionExtractor(VExtractor.class)
        .build();
    }
  }
}
