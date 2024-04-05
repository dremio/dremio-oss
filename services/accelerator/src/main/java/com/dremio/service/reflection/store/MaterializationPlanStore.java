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

import static com.dremio.datastore.SearchQueryUtils.newTermQuery;
import static com.dremio.service.reflection.store.ReflectionIndexKeys.MATERIALIZATION_PLAN_MATERIALIZATION_ID;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationPlan;
import com.dremio.service.reflection.proto.MaterializationPlanId;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import java.util.Map;
import javax.inject.Provider;

/**
 * Stores materialization plans used for matching. Plans are tied to a specific version of the
 * server.
 */
public class MaterializationPlanStore {

  public static MaterializationPlanId createMaterializationPlanId(
      MaterializationId materializationId) {
    return new MaterializationPlanId(
        String.format("%s-%s", materializationId.getId(), DremioVersionInfo.VERSION));
  }

  private static final String MATERIALIZATION_PLAN_TABLE_NAME = "materialization_plan_store";

  private final Supplier<LegacyIndexedStore<MaterializationPlanId, MaterializationPlan>>
      materializationPlanStore;

  public MaterializationPlanStore(final Provider<LegacyKVStoreProvider> provider) {
    Preconditions.checkNotNull(provider, "kvStore provider required");
    this.materializationPlanStore =
        Suppliers.memoize(
            new Supplier<LegacyIndexedStore<MaterializationPlanId, MaterializationPlan>>() {
              @Override
              public LegacyIndexedStore<MaterializationPlanId, MaterializationPlan> get() {
                return provider.get().getStore(MaterializationPlanStoreCreator.class);
              }
            });
  }

  public Iterable<Map.Entry<MaterializationPlanId, MaterializationPlan>> getAll() {
    return materializationPlanStore.get().find();
  }

  public Iterable<MaterializationPlan> getAllByMaterializationId(MaterializationId id) {
    final LegacyFindByCondition condition =
        new LegacyFindByCondition()
            .setCondition(newTermQuery(MATERIALIZATION_PLAN_MATERIALIZATION_ID, id.getId()));
    return Iterables.transform(materializationPlanStore.get().find(condition), x -> x.getValue());
  }

  /** Returns the materialization logical plan for the current server version */
  public MaterializationPlan getVersionedPlan(MaterializationId id) {
    return materializationPlanStore.get().get(createMaterializationPlanId(id));
  }

  public void save(MaterializationPlan m) {
    final long currentTime = System.currentTimeMillis();
    if (m.getCreatedAt() == null) {
      m.setCreatedAt(currentTime);
    }
    m.setModifiedAt(currentTime);

    materializationPlanStore.get().put(m.getId(), m);
  }

  public MaterializationPlan get(MaterializationPlanId materializationPlanId) {
    MaterializationPlan value = materializationPlanStore.get().get(materializationPlanId);
    return value;
  }

  public void delete(MaterializationPlanId id) {
    materializationPlanStore.get().delete(id);
  }

  private static final class MaterializationPlanVersionExtractor
      implements VersionExtractor<MaterializationPlan> {
    @Override
    public String getTag(MaterializationPlan value) {
      return value.getTag();
    }

    @Override
    public void setTag(MaterializationPlan value, String tag) {
      value.setTag(tag);
    }
  }

  private static final class MaterializationPlanConverter
      implements DocumentConverter<MaterializationPlanId, MaterializationPlan> {
    private Integer version = 0;

    @Override
    public Integer getVersion() {
      return version;
    }

    @Override
    public void convert(
        DocumentWriter writer, MaterializationPlanId key, MaterializationPlan record) {
      writer.write(MATERIALIZATION_PLAN_MATERIALIZATION_ID, record.getMaterializationId().getId());
    }
  }

  public static final class MaterializationPlanStoreCreator
      implements LegacyIndexedStoreCreationFunction<MaterializationPlanId, MaterializationPlan> {
    @Override
    public LegacyIndexedStore<MaterializationPlanId, MaterializationPlan> build(
        LegacyStoreBuildingFactory factory) {
      return factory
          .<MaterializationPlanId, MaterializationPlan>newStore()
          .name(MATERIALIZATION_PLAN_TABLE_NAME)
          .keyFormat(Format.ofProtostuff(MaterializationPlanId.class))
          .valueFormat(Format.ofProtostuff(MaterializationPlan.class))
          .versionExtractor(MaterializationPlanVersionExtractor.class)
          .buildIndexed(new MaterializationPlanConverter());
    }
  }
}
