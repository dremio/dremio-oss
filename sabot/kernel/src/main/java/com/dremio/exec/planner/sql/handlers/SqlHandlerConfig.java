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

import com.dremio.common.logical.PlanProperties.Generator.ResultMode;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.events.PlannerEventBus;
import com.dremio.exec.planner.events.PlannerEventBusImpl;
import com.dremio.exec.planner.normalizer.PlannerBaseComponentImpl;
import com.dremio.exec.planner.normalizer.PlannerBaseModule;
import com.dremio.exec.planner.normalizer.PlannerNormalizerComponent;
import com.dremio.exec.planner.normalizer.RelNormalizerTransformer;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.sql.SqlConverter;
import java.util.Optional;
import org.apache.calcite.tools.RuleSet;

public class SqlHandlerConfig {

  private final QueryContext context;
  private final SqlConverter converter;
  private final AttemptObservers observer;
  private final MaterializationList materializations;
  private final RelNormalizerTransformer relNormalizerTransformer;
  private final PlannerEventBus plannerEventBus;
  private ResultMode resultMode;

  public SqlHandlerConfig(
      QueryContext context,
      SqlConverter converter,
      AttemptObserver observer,
      MaterializationList materializations) {
    this(
        context,
        converter,
        toAttemptObservers(observer),
        materializations,
        new PlannerEventBusImpl(),
        ResultMode.EXEC);
  }

  private SqlHandlerConfig(
      QueryContext context,
      SqlConverter converter,
      AttemptObservers observer,
      MaterializationList materializations,
      PlannerEventBus plannerEventBus,
      ResultMode resultMode) {
    super();
    this.context = context;
    this.converter = converter;
    this.observer = observer;
    this.materializations = materializations;
    this.plannerEventBus = plannerEventBus;
    this.resultMode = resultMode;

    PlannerNormalizerComponent plannerNormalizerComponent =
        context.createPlannerNormalizerComponent(
            PlannerBaseComponentImpl.build(
                new PlannerBaseModule(),
                converter.getSettings(),
                converter.getFunctionImplementationRegistry(),
                converter.getFunctionContext(),
                converter.getOpTab(),
                converter,
                observer,
                plannerEventBus));
    this.relNormalizerTransformer = plannerNormalizerComponent.getRelNormalizerTransformer();
  }

  public QueryContext getContext() {
    return context;
  }

  public AttemptObservers getObserver() {
    return observer;
  }

  public Optional<MaterializationList> getMaterializations() {
    return Optional.ofNullable(materializations);
  }

  public RuleSet getRules(PlannerPhase phase) {
    return PlannerPhase.mergedRuleSets(
        context.getInjectedRules(phase),
        phase.getRules(context),
        context.getCatalogService().getStorageRules(context, phase));
  }

  public ScanResult getScanResult() {
    return context.getScanResult();
  }

  public SqlHandlerConfig cloneWithNewObserver(AttemptObserver replacementObserver) {
    AttemptObservers observer = toAttemptObservers(replacementObserver);
    return new SqlHandlerConfig(
        this.context,
        this.converter,
        observer,
        this.materializations,
        this.plannerEventBus,
        this.resultMode);
  }

  public SqlConverter getConverter() {
    return converter;
  }

  public void addObserver(AttemptObserver observer) {
    this.observer.add(observer);
  }

  public RelNormalizerTransformer getRelNormalizerTransformer() {
    return relNormalizerTransformer;
  }

  public void setResultMode(ResultMode resultMode) {
    this.resultMode = resultMode;
  }

  public ResultMode getResultMode() {
    return resultMode;
  }

  public PlannerEventBus getPlannerEventBus() {
    return plannerEventBus;
  }

  private static AttemptObservers toAttemptObservers(AttemptObserver observer) {
    return observer instanceof AttemptObservers
        ? (AttemptObservers) observer
        : AttemptObservers.of(observer);
  }
}
