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
package com.dremio.service.accelerator;

import static com.dremio.service.accelerator.AccelerationUtils.selfOrEmpty;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import com.dremio.common.utils.SqlUtils;
import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.sys.accel.AccelerationInfo;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.LayoutInfo;
import com.dremio.exec.store.sys.accel.MaterializationInfo;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

/**
 * Exposes the acceleration manager interface to the rest of the system (executor side)
 */
public class AccelerationListManagerImpl implements AccelerationListManager {

  private final AccelerationStore accelerationStore;
  private final MaterializationStore materializationStore;
  private Provider<SabotContext> contextProvider;
  private Provider<NamespaceService> namespaceService;

  public AccelerationListManagerImpl(Provider<KVStoreProvider> storeProvider,
      final Provider<SabotContext> contextProvider) {
    this.accelerationStore = new AccelerationStore(storeProvider);
    this.materializationStore = new MaterializationStore(storeProvider);
    this.contextProvider = contextProvider;
  }

  @Override
  public void start() throws Exception {
    accelerationStore.start();
    materializationStore.start();

    this.namespaceService = new Provider<NamespaceService>() {
      @Override
      public NamespaceService get() {
        return contextProvider.get().getNamespaceService(SYSTEM_USERNAME);
      }
    };
  }

  @Override
  public void close() throws Exception {

  }

  private Iterable<Acceleration> getAccelerations(final IndexedStore.FindByCondition condition) {
    return accelerationStore.find(condition);
  }

  private Iterable<Materialization> getMaterializations(final LayoutId layoutId) {
    return AccelerationUtils.getAllMaterializations(materializationStore.get(layoutId));
  }


  @Override
  public List<AccelerationInfo> getAccelerations() {
    return FluentIterable
      .from(getAccelerations(new IndexedStore.FindByCondition().setCondition(SearchQueryUtils.newMatchAllQuery())))
      .transform(new Function<Acceleration, AccelerationInfo>(){
        @Override
        public AccelerationInfo apply(Acceleration input) {
          DatasetConfig ds = namespaceService.get().findDatasetByUUID(input.getId().getId());
          return new AccelerationInfo(
            input.getId().getId(),
            SqlUtils.quotedCompound(ds.getFullPathList()),
            input.getState().name(),
            selfOrEmpty(input.getRawLayouts().getLayoutList()).size(),
            input.getRawLayouts().getEnabled(),
            selfOrEmpty(input.getAggregationLayouts().getLayoutList()).size(),
            input.getAggregationLayouts().getEnabled()
          );
        }}).toList();
  }

  @Override
  public List<LayoutInfo> getLayouts() {
    List<LayoutInfo> layouts = new ArrayList<>();
    for(Acceleration accel : getAccelerations(new IndexedStore.FindByCondition().setCondition(SearchQueryUtils.newMatchAllQuery()))) {
      for(Layout l : selfOrEmpty(accel.getAggregationLayouts().getLayoutList())){
        Optional<MaterializedLayout> materializedLayoutIf = materializationStore.get(l.getId());
        layouts.add(toInfo(accel.getId().getId(), l, materializedLayoutIf));
      }
      for(Layout l : selfOrEmpty(accel.getRawLayouts().getLayoutList())){
        Optional<MaterializedLayout> materializedLayoutIf = materializationStore.get(l.getId());
        layouts.add(toInfo(accel.getId().getId(), l, materializedLayoutIf));
      }
    }

    return layouts;
  }

  @Override
  public List<MaterializationInfo> getMaterializations() {
    List<MaterializationInfo> materializations = new ArrayList<>();
    for(Acceleration accel : getAccelerations(new IndexedStore.FindByCondition().setCondition(SearchQueryUtils.newMatchAllQuery()))){

      for(Layout l : selfOrEmpty(accel.getAggregationLayouts().getLayoutList())){
        for(Materialization m : getMaterializations(l.getId())){
          materializations.add(asInfo(accel, l, m));
        }
      }
      for(Layout l : selfOrEmpty(accel.getRawLayouts().getLayoutList())){
        for(Materialization m : getMaterializations(l.getId())){
          materializations.add(asInfo(accel, l, m));
        }
      }
    }

    return materializations;
  }

  private MaterializationInfo asInfo(Acceleration accel, Layout l, Materialization m){
    return new MaterializationInfo(
      accel.getId().getId(),
      l.getId().getId(),
      m.getId().getId(),
      m.getJob() != null && m.getJob().getJobEnd() != null ? new Timestamp(m.getJob().getJobEnd()) : null,
      m.getExpiration() != null ? new Timestamp(m.getExpiration()) : null,
      m.getJob() != null ? m.getJob().getOutputBytes() : null
    );
  }

  private LayoutInfo toInfo(String accelerationId, Layout l, Optional<MaterializedLayout> materializedLayoutIf){
    return new LayoutInfo(
      accelerationId,
      l.getId().getId(),
      l.getLayoutType().name(),
      toString(l.getDetails().getDisplayFieldList()),
      toDimensionString(l.getDetails().getDimensionFieldList()),
      toString(l.getDetails().getMeasureFieldList()),
      toString(l.getDetails().getPartitionFieldList()),
      toString(l.getDetails().getDistributionFieldList()),
      toString(l.getDetails().getSortFieldList()),
      materializedLayoutIf.isPresent() && materializedLayoutIf.get().getState() != null ? materializedLayoutIf.get().getState().name():null
    );
  }

  private String toDimensionString(final List<LayoutDimensionField> field){
    return FluentIterable.from(AccelerationUtils.selfOrEmpty(field))
      .transform(new Function<LayoutDimensionField, String>(){
        @Override
        public String apply(LayoutDimensionField input) {
          return String.format("%s with granularity %s", SqlUtils.quoteIdentifier(input.getName()), input.getGranularity());
        }})
      .join(Joiner.on(", "));
  }

  private String toString(List<LayoutField> field){
    if(field == null){
      return null;
    }
    return FluentIterable.from(field).transform(new Function<LayoutField, String>(){

      @Override
      public String apply(LayoutField input) {
        return SqlUtils.quoteIdentifier(input.getName());
      }}).join(Joiner.on(", "));
  }

}
