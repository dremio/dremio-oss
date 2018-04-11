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
package com.dremio.service.accelerator.store;

import javax.inject.Provider;

import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.KVUtil;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.store.converter.MaterializedLayoutConverter;
import com.dremio.service.accelerator.store.extractor.MaterializedLayoutVersionExtractor;
import com.dremio.service.accelerator.store.serializer.LayoutIdSerializer;
import com.dremio.service.accelerator.store.serializer.MaterializedLayoutSerializer;
import com.google.common.base.Optional;

/**
 * A store that holds {@link MaterializedLayout}
 */
public class MaterializationStore {

  private static final String TABLE_NAME = "acceleration_materialization";

  private final Provider<KVStoreProvider> provider;
  private IndexedStore<LayoutId, MaterializedLayout> store;

  public MaterializationStore(final Provider<KVStoreProvider> provider) {
    this.provider = provider;
  }

  public void start() {
    this.store = provider.get().getStore(MaterializationStoreCreator.class);
  }

  public Iterable<MaterializedLayout> find() {
    return KVUtil.values(store.find());
  }

  public Iterable<MaterializedLayout> find(final IndexedStore.FindByCondition condition) {
    return KVUtil.values(store.find(condition));
  }

  public void save(final MaterializedLayout layout) {
    store.put(layout.getLayoutId(), layout);
  }

  public Optional<MaterializedLayout> get(final LayoutId id) {
    return Optional.fromNullable(store.get(id));
  }

  public void remove(final LayoutId id) {
    store.delete(id);
  }

  /**
   * Acceleration Store creator.
   */
  public static final class MaterializationStoreCreator implements StoreCreationFunction<IndexedStore<LayoutId, MaterializedLayout>> {

    @Override
    public IndexedStore<LayoutId, MaterializedLayout> build(StoreBuildingFactory factory) {
      return factory.<LayoutId, MaterializedLayout>newStore()
          .name(TABLE_NAME)
          .keySerializer(LayoutIdSerializer.class)
          .valueSerializer(MaterializedLayoutSerializer.class)
          .versionExtractor(MaterializedLayoutVersionExtractor.class)
          .buildIndexed(MaterializedLayoutConverter.class);
    }

  }

}
