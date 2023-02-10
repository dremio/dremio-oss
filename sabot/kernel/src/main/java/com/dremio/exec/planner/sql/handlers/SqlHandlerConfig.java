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

package com.dremio.exec.planner.sql.handlers;

import java.util.Optional;

import org.apache.calcite.tools.RuleSet;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.sql.SqlConverter;


public class SqlHandlerConfig {

  private final QueryContext context;
  private final SqlConverter converter;
  private final AttemptObservers observer;
  private final MaterializationList materializations;

  public SqlHandlerConfig(QueryContext context, SqlConverter converter, AttemptObserver observer,
      MaterializationList materializations) {
    super();
    this.context = context;
    this.converter = converter;
    this.observer = AttemptObservers.of(observer);
    this.materializations = materializations;
  }

  public QueryContext getContext() {
    return context;
  }

  public AttemptObserver getObserver() {
    return observer;
  }

  public Optional<MaterializationList> getMaterializations() {
    return Optional.ofNullable(materializations);
  }

  public RuleSet getRules(PlannerPhase phase) {
    return PlannerPhase.mergedRuleSets(
        context.getInjectedRules(phase),
        phase.getRules(context, converter),
        context.getCatalogService().getStorageRules(context, phase));
  }

  public ScanResult getScanResult() {
    return context.getScanResult();
  }

  public SqlHandlerConfig cloneWithNewObserver(AttemptObserver replacementObserver){
    return new SqlHandlerConfig(this.context, this.converter, replacementObserver, this.materializations);
  }

  public SqlConverter getConverter() {
    return converter;
  }

  public void addObserver(AttemptObserver observer) {
    this.observer.add(observer);
  }
}
