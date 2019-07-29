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
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeStats;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.resource.ResourceSchedulingDecisionInfo;

public abstract class AbstractAttemptObserver implements AttemptObserver {

  public static final AbstractAttemptObserver NOOP = new AbstractAttemptObserver() {
  };

  @Override
  public void planText(String text, long millisTaken) {
  }

  @Override
  public void finalPrel(Prel prel) {
  }

  @Override
  public void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after, long millisTaken) {
  }

  @Override
  public void planParallelStart() {
  }

  @Override
  public void planParallelized(PlanningSet planningSet) {
  }

  @Override
  public void planFindMaterializations(long millisTaken) {
  }

  @Override
  public void recordExtraInfo(String name, byte[] bytes) {
  }

  @Override
  public void planNormalized(long millisTaken, List<RelNode> normalizedQueryPlans) {
  }

  @Override
  public void planAccelerated(final SubstitutionInfo info) {
  }

  @Override
  public void planConvertedToRel(RelNode converted, long millisTaken) {
  }

  @Override
  public void planConvertedScan(RelNode converted, long millisTaken) {
  }

  @Override
  public void planSubstituted(DremioMaterialization materialization,
                              List<RelNode> substitutions,
                              RelNode target,
                              long millisTaken) {
  }

  @Override
  public void substitutionFailures(Iterable<String> errors) {
  }

  @Override
  public void planCompleted(final ExecutionPlan plan) {
  }

  @Override
  public void planStart(String rawPlan) {
  }

  @Override
  public void planValidated(RelDataType rowType, SqlNode node, long millisTaken) {
  }

  @Override
  public void planSerializable(RelNode serializable) {
  }

  @Override
  public void planExpandView(RelRoot expanded, List<String> schemaPath, int nestingLevel, String sql) {
  }

  @Override
  public void plansDistributionComplete(QueryWorkUnit unit) {
  }

  @Override
  public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
  }

  @Override
  public void attemptCompletion(UserResult result) {
  }

  @Override
  public void queryStarted(UserRequest query, String user) {
  }

  @Override
  public void commandPoolWait(long waitInMillis) {
  }

  @Override
  public void execStarted(QueryProfile profile) {
  }

  @Override
  public void planJsonPlan(String text) {
  }

  @Override
  public void executorsSelected(long millisTaken, int idealNumFragments, int idealNumNodes, int numExecutors, String detailsText) {
  }

  @Override
  public void planGenerationTime(long millisTaken) {
  }

  @Override
  public void planAssignmentTime(long millisTaken) {
  }

  @Override
  public void fragmentsStarted(long millisTaken, FragmentRpcSizeStats stats) {
  }

  @Override
  public void fragmentsActivated(long millisTaken) {
  }

  @Override
  public void activateFragmentFailed(Exception ex) {

  }

  @Override
  public void tablesCollected(Iterable<DremioTable> tables) {
  }

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
  }
}
