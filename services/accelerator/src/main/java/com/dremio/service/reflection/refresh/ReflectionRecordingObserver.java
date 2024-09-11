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
package com.dremio.service.reflection.refresh;

import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.RelWithInfo;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.AccelerationProfile;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeStats;
import com.dremio.exec.proto.UserBitShared.PlannerPhaseRulesStats;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.reflection.hints.ReflectionExplanationsAndQueryDistance;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

/**
 * Collects ALL observer calls so that they can be played back for the persisted query profile.
 *
 * <p>We intentionally do not extend {@link
 * com.dremio.exec.planner.observer.AbstractAttemptObserver} so that future changes to {@link
 * AttemptObserver} require an update here.
 */
public class ReflectionRecordingObserver implements AttemptObserver {

  private interface ObserverCall {
    void doCall(AttemptObserver observer);
  }

  private final List<ObserverCall> calls = new ArrayList<>();

  @Override
  public void beginState(AttemptEvent event) {
    calls.add(observer -> observer.beginState(event));
  }

  @Override
  public void queryStarted(UserRequest query, String user) {
    calls.add(observer -> observer.queryStarted(query, user));
  }

  @Override
  public void commandPoolWait(long waitInMillis) {
    calls.add(observer -> observer.commandPoolWait(waitInMillis));
  }

  @Override
  public void planStart(String rawPlan) {
    calls.add(observer -> observer.planStart(rawPlan));
  }

  @Override
  public void resourcesPlanned(GroupResourceInformation resourceInformation, long millisTaken) {
    calls.add(observer -> observer.resourcesPlanned(resourceInformation, millisTaken));
  }

  @Override
  public void planValidated(
      RelDataType rowType,
      SqlNode node,
      long millisTaken,
      boolean isMaterializationCacheInitialized) {
    calls.add(
        observer ->
            observer.planValidated(rowType, node, millisTaken, isMaterializationCacheInitialized));
  }

  @Override
  public void planCacheUsed(int count) {
    calls.add(observer -> observer.planCacheUsed(count));
  }

  @Override
  public void restoreAccelerationProfileFromCachedPlan(AccelerationProfile accelerationProfile) {
    calls.add(observer -> observer.restoreAccelerationProfileFromCachedPlan(accelerationProfile));
  }

  @Override
  public void planSerializable(RelNode serializable) {
    calls.add(observer -> observer.planSerializable(serializable));
  }

  @Override
  public void planConvertedToRel(RelNode converted, long millisTaken) {
    calls.add(observer -> observer.planConvertedToRel(converted, millisTaken));
  }

  @Override
  public void recordExtraInfo(String name, byte[] bytes) {
    calls.add(observer -> observer.recordExtraInfo(name, bytes));
  }

  @Override
  public void planConvertedScan(RelNode converted, long millisTaken) {
    calls.add(observer -> observer.planConvertedScan(converted, millisTaken));
  }

  @Override
  public void planRefreshDecision(String text, long millisTaken) {
    calls.add(observer -> observer.planRefreshDecision(text, millisTaken));
  }

  @Override
  public void planExpandView(
      RelRoot expanded, List<String> schemaPath, int nestingLevel, String sql) {
    calls.add(observer -> observer.planExpandView(expanded, schemaPath, nestingLevel, sql));
  }

  @Override
  public void planRelTransform(
      PlannerPhase phase,
      RelOptPlanner planner,
      RelNode before,
      RelNode after,
      long millisTaken,
      List<PlannerPhaseRulesStats> rulesBreakdownStats) {
    calls.add(
        observer ->
            observer.planRelTransform(
                phase, planner, before, after, millisTaken, rulesBreakdownStats));
  }

  @Override
  public void tablesCollected(Iterable<DremioTable> tables) {
    calls.add(observer -> observer.tablesCollected(tables));
  }

  @Override
  public void planText(String text, long millisTaken) {
    calls.add(observer -> observer.planText(text, millisTaken));
  }

  @Override
  public void finalPrelPlanGenerated(Prel prel) {
    calls.add(observer -> observer.finalPrelPlanGenerated(prel));
  }

  @Override
  public void planParallelStart() {
    calls.add(observer -> observer.planParallelStart());
  }

  @Override
  public void planParallelized(PlanningSet planningSet) {
    calls.add(observer -> observer.planParallelized(planningSet));
  }

  @Override
  public void plansDistributionComplete(QueryWorkUnit unit) {
    calls.add(observer -> observer.plansDistributionComplete(unit));
  }

  @Override
  public void planFindMaterializations(long millisTaken) {
    calls.add(observer -> observer.planFindMaterializations(millisTaken));
  }

  @Override
  public void planNormalized(long millisTaken, List<RelWithInfo> normalizedQueryPlans) {
    calls.add(observer -> observer.planNormalized(millisTaken, normalizedQueryPlans));
  }

  @Override
  public void planSubstituted(long millisTaken) {
    calls.add(observer -> observer.planSubstituted(millisTaken));
  }

  @Override
  public void planSubstituted(
      DremioMaterialization materialization,
      List<RelWithInfo> substitutions,
      RelWithInfo target,
      long millisTaken,
      boolean defaultReflection) {
    calls.add(
        observer ->
            observer.planSubstituted(
                materialization, substitutions, target, millisTaken, defaultReflection));
  }

  @Override
  public void substitutionFailures(Iterable<String> errors) {
    calls.add(observer -> observer.substitutionFailures(errors));
  }

  @Override
  public void planAccelerated(SubstitutionInfo info) {
    calls.add(observer -> observer.planAccelerated(info));
  }

  @Override
  public void planCompleted(ExecutionPlan plan, BatchSchema batchSchema) {
    calls.add(observer -> observer.planCompleted(plan, batchSchema));
  }

  @Override
  public void execStarted(QueryProfile profile) {
    calls.add(observer -> observer.execStarted(profile));
  }

  @Override
  public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
    calls.add(observer -> observer.execDataArrived(outcomeListener, result));
  }

  @Override
  public void planJsonPlan(String text) {
    calls.add(observer -> observer.planJsonPlan(text));
  }

  @Override
  public void attemptCompletion(UserResult result) {
    calls.add(observer -> observer.attemptCompletion(result));
  }

  @Override
  public void executorsSelected(
      long millisTaken,
      int idealNumFragments,
      int idealNumNodes,
      int numExecutors,
      String detailsText) {
    calls.add(
        observer ->
            observer.executorsSelected(
                millisTaken, idealNumFragments, idealNumNodes, numExecutors, detailsText));
  }

  @Override
  public void recordsOutput(CoordinationProtos.NodeEndpoint endpoint, long recordCount) {
    calls.add(observer -> observer.recordsOutput(endpoint, recordCount));
  }

  @Override
  public void outputLimited() {
    calls.add(AttemptObserver::outputLimited);
  }

  @Override
  public void planGenerationTime(long millisTaken) {
    calls.add(observer -> observer.planGenerationTime(millisTaken));
  }

  @Override
  public void planAssignmentTime(long millisTaken) {
    calls.add(observer -> observer.planAssignmentTime(millisTaken));
  }

  @Override
  public void fragmentsStarted(long millisTaken, FragmentRpcSizeStats stats) {
    calls.add(observer -> observer.fragmentsStarted(millisTaken, stats));
  }

  @Override
  public void fragmentsActivated(long millisTaken) {
    calls.add(observer -> observer.fragmentsActivated(millisTaken));
  }

  @Override
  public void activateFragmentFailed(Exception ex) {
    calls.add(observer -> observer.activateFragmentFailed(ex));
  }

  @Override
  public void setNumJoinsInUserQuery(Integer joins) {
    calls.add(observer -> observer.setNumJoinsInUserQuery(joins));
  }

  @Override
  public void setNumJoinsInFinalPrel(Integer joins) {
    calls.add(observer -> observer.setNumJoinsInFinalPrel(joins));
  }

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
    calls.add(observer -> observer.resourcesScheduled(resourceSchedulingDecisionInfo));
  }

  @Override
  public void updateReflectionsWithHints(
      ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
    calls.add(
        observer -> observer.updateReflectionsWithHints(reflectionExplanationsAndQueryDistance));
  }

  @Override
  public void putProfileFailed() {
    calls.add(observer -> observer.putProfileFailed());
  }

  public void replay(AttemptObserver observer) {
    for (ObserverCall c : calls) {
      c.doCall(observer);
    }
  }
}
