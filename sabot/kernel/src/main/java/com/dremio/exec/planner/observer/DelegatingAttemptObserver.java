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
package com.dremio.exec.planner.observer;

import java.util.List;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.CachedAccelDetails;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeStats;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.reflection.hints.ReflectionExplanationsAndQueryDistance;
import com.dremio.resource.ResourceSchedulingDecisionInfo;

public class DelegatingAttemptObserver implements AttemptObserver {

  private final AttemptObserver observer;

  public DelegatingAttemptObserver(AttemptObserver observer) {
    this.observer = observer;
  }

  @Override
  public void beginState(AttemptEvent event) {
    observer.beginState(event);
  }

  @Override
  public void queryStarted(UserRequest query, String user) {
    observer.queryStarted(query, user);
  }

  @Override
  public void commandPoolWait(long waitInMillis) {
    observer.commandPoolWait(waitInMillis);
  }

  @Override
  public void planStart(String rawPlan) {
    observer.planStart(rawPlan);
  }

  @Override
  public void planValidated(RelDataType rowType, SqlNode node, long millisTaken) {
    observer.planValidated(rowType, node, millisTaken);
  }

  @Override
  public void planSerializable(RelNode serializable) {
    observer.planSerializable(serializable);
  }

  @Override
  public void planConvertedToRel(RelNode converted, long millisTaken) {
    observer.planConvertedToRel(converted, millisTaken);
  }

  @Override
  public void planConvertedScan(RelNode converted, long millisTaken) {
    observer.planConvertedScan(converted, millisTaken);
  }

  @Override
  public void planExpandView(RelRoot expanded, List<String> schemaPath, int nestingLevel, String sql) {
    observer.planExpandView(expanded, schemaPath, nestingLevel, sql);
  }

  @Override
  public void plansDistributionComplete(QueryWorkUnit unit) {
    observer.plansDistributionComplete(unit);
  }

  @Override
  public void planText(String text, long millisTaken) {
    observer.planText(text, millisTaken);
  }

  @Override
  public void finalPrel(Prel prel) {
    observer.finalPrel(prel);
  }

  @Override
  public void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after, long millisTaken) {
    observer.planRelTransform(phase, planner, before, after, millisTaken);
  }

  @Override
  public void planParallelStart() {
    observer.planParallelStart();
  }

  @Override
  public void planParallelized(PlanningSet planningSet) {
    observer.planParallelized(planningSet);
  }

  @Override
  public void planFindMaterializations(long millisTaken) {
    observer.planFindMaterializations(millisTaken);
  }

  @Override
  public void planNormalized(long millisTaken, List<RelNode> normalizedQueryPlans) {
    observer.planNormalized(millisTaken, normalizedQueryPlans);
  }

  @Override
  public void planSubstituted(DremioMaterialization materialization, List<RelNode> substitutions,
    RelNode target, long millisTaken, boolean defaultReflection) {
    observer.planSubstituted(materialization, substitutions, target, millisTaken,
      defaultReflection);
  }

  @Override
  public void substitutionFailures(Iterable<String> errors) {
    observer.substitutionFailures(errors);
  }

  @Override
  public void applyAccelDetails(final CachedAccelDetails accelDetails) {
    observer.applyAccelDetails(accelDetails);
  }

  @Override
  public void planAccelerated(final SubstitutionInfo info) {
    observer.planAccelerated(info);
  }

  @Override
  public void planCompleted(final ExecutionPlan plan) {
    observer.planCompleted(plan);
  }

  @Override
  public void execStarted(QueryProfile profile) {
    observer.execStarted(profile);
  }

  @Override
  public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
    observer.execDataArrived(outcomeListener, result);
  }

  @Override
  public void attemptCompletion(UserResult result) {
    observer.attemptCompletion(result);
  }

  @Override
  public void planJsonPlan(String text) {
    observer.planJsonPlan(text);
  }

  @Override
  public void executorsSelected(long millisTaken, int idealNumFragments, int idealNumNodes, int numExecutors, String detailsText) {
    observer.executorsSelected(millisTaken, idealNumFragments, idealNumNodes, numExecutors, detailsText);
  }

  @Override
  public void recordsProcessed(long recordCount) {
    observer.recordsProcessed(recordCount);
  }

  @Override
  public void planGenerationTime(long millisTaken) {
    observer.planGenerationTime(millisTaken);
  }

  @Override
  public void planAssignmentTime(long millisTaken) {
    observer.planAssignmentTime(millisTaken);
  }

  @Override
  public void fragmentsStarted(long millisTaken, FragmentRpcSizeStats stats) {
    observer.fragmentsStarted(millisTaken, stats);
  }

  @Override
  public void fragmentsActivated(long millisTaken) { observer.fragmentsActivated(millisTaken); }

  @Override
  public void activateFragmentFailed(Exception ex){ observer.activateFragmentFailed(ex); }

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
    observer.resourcesScheduled(resourceSchedulingDecisionInfo);
  }

  @Override
  public void recordExtraInfo(String name, byte[] bytes) {
    observer.recordExtraInfo(name, bytes);
  }

  @Override
  public void tablesCollected(Iterable<DremioTable> tables) {
    observer.tablesCollected(tables);
  }

  @Override
  public void updateReflectionsWithHints(ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
    observer.updateReflectionsWithHints(reflectionExplanationsAndQueryDistance);
  }
}
