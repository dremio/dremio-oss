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
package com.dremio.service.reflection.store;

import javax.inject.Provider;

import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.StringSerializer;
import com.dremio.datastore.VersionExtractor;
import com.dremio.service.reflection.proto.RefreshRequest;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

/**
 * KVStore to store refresh requests on physical datasets
 */
public class RefreshRequestsStore {
  private static final String TABLE_NAME = "refresh_requests";

  private final Supplier<KVStore<String, RefreshRequest>> store;

  public RefreshRequestsStore(final Provider<KVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvStore provider required");
    store = Suppliers.memoize(new Supplier<KVStore<String, RefreshRequest>>() {
      @Override
      public KVStore<String, RefreshRequest> get() {
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
    public Long getVersion(RefreshRequest value) {
      return value.getVersion();
    }

    @Override
    public Long incrementVersion(RefreshRequest value) {
      final Long current = value.getVersion();
      value.setVersion(Optional.fromNullable(current).or(-1L) + 1);
      return current;
    }

    @Override
    public void setVersion(RefreshRequest value, Long version) {
      value.setVersion(version == null ? 0 : version);
    }
  }

  /**
   * {@link RefreshRequestsStore} creator
   */
  public static final class StoreCreator implements StoreCreationFunction<KVStore<String, RefreshRequest>> {
    @Override
    public KVStore<String, RefreshRequest> build(StoreBuildingFactory factory) {
      return factory.<String, RefreshRequest>newStore()
        .name(TABLE_NAME)
        .keySerializer(StringSerializer.class)
        .valueSerializer(Serializers.RefreshRequestSerializer.class)
        .versionExtractor(VExtractor.class)
        .build();
    }
  }
}
