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

package com.dremio.exec.planner.sql.handlers;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.calcite.tools.RuleSet;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.PlannerCallback;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.MaterializationList;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.StoragePlugin;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;


public class SqlHandlerConfig {

  private final QueryContext context;
  private final SqlConverter converter;
  private final AttemptObserver observer;
  private final MaterializationList materializations;

  public SqlHandlerConfig(QueryContext context, SqlConverter converter, AttemptObserver observer,
      MaterializationList materializations) {
    super();
    this.context = context;
    this.converter = converter;
    this.observer = observer;
    this.materializations = materializations;
  }

  public QueryContext getContext() {
    return context;
  }

  public AttemptObserver getObserver() {
    return observer;
  }

  public Optional<MaterializationList> getMaterializations() {
    return Optional.fromNullable(materializations);
  }

  public RuleSet getRules(PlannerPhase phase) {
    return phase.getRules(context, getPlugins());
  }

  public SqlHandlerConfig cloneWithNewObserver(AttemptObserver replacementObserver){
    return new SqlHandlerConfig(this.context, this.converter, replacementObserver, this.materializations);
  }

  public PlannerCallback getPlannerCallback(PlannerPhase phase){
    List<PlannerCallback> callbacks = Lists.newArrayList();
    for(StoragePlugin<?> plugin: getPlugins()){
      if(plugin instanceof AbstractStoragePlugin){
        PlannerCallback callback = ((AbstractStoragePlugin<?>)plugin).getPlannerCallback(context, phase);
        if(callback != null){
          callbacks.add(callback);
        }
      }
    }
    return PlannerCallback.merge(callbacks);
  }

  @SuppressWarnings("rawtypes")
  private Collection<StoragePlugin> getPlugins(){
    Collection<StoragePlugin> plugins = Lists.newArrayList();
    for (Entry<String, StoragePlugin<?>> k : context.getStorage()) {
      plugins.add(k.getValue());
    }
    return plugins;
  }

  public SqlConverter getConverter() {
    return converter;
  }
}
