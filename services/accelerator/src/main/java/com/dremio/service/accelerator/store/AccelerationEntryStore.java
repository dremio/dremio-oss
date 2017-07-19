/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.accelerator.store;

import javax.inject.Provider;

import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.store.converter.AccelerationEntryConverter;
import com.dremio.service.accelerator.store.extractor.AccelerationEntryVersionExtractor;
import com.dremio.service.accelerator.store.serializer.AccelerationEntrySerializer;
import com.dremio.service.accelerator.store.serializer.AccelerationIdSerializer;
import com.google.common.base.Optional;

/**
 * A store that holds {@link AccelerationEntry}
 */
public class AccelerationEntryStore {

  private static final String TABLE_NAME = "acceleration_holder";

  private final Provider<KVStoreProvider> provider;
  private IndexedStore<AccelerationId, AccelerationEntry> store;

  public AccelerationEntryStore(final Provider<KVStoreProvider> provider) {
    this.provider = provider;
  }

  public void start() {
    this.store = provider.get().getStore(AccelerationHolderStoreCreator.class);
  }

  public void save(final AccelerationEntry holder) {
    store.put(holder.getDescriptor().getId(), holder);
  }

  public Optional<AccelerationEntry> get(final AccelerationId id) {
    return Optional.fromNullable(store.get(id));
  }

  public void remove(final AccelerationId id) {
    store.delete(id);
  }

  /**
   * Store creator
   */
  public static class AccelerationHolderStoreCreator implements StoreCreationFunction<IndexedStore<AccelerationId, AccelerationEntry>> {
    @Override
    public IndexedStore<AccelerationId, AccelerationEntry> build(final StoreBuildingFactory factory) {
      return factory.<AccelerationId, AccelerationEntry>newStore()
          .name(TABLE_NAME)
          .keySerializer(AccelerationIdSerializer.class)
          .valueSerializer(AccelerationEntrySerializer.class)
          .versionExtractor(AccelerationEntryVersionExtractor.class)
          .buildIndexed(AccelerationEntryConverter.class);
    }
  }

}
