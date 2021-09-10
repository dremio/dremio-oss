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
import java.util.concurrent.CountDownLatch;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.DeferredException;
import com.dremio.common.SerializedExecutor;
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

/**
 * Does query observations in order but not in the query execution thread. This
 * ensures two things:
 * - any blocking commands don't block the underlying thread
 * - any exceptions don't bleed into the caller.
 *
 * Additionally, the observer will report back all exceptions thrown in the callbacks to the delegate
 * {@link #attemptCompletion(UserResult)} callback
 */
public class OutOfBandAttemptObserver implements AttemptObserver {

  private final SerializedExecutor<Runnable> serializedExec;
  private final AttemptObserver innerObserver;
  private final DeferredException deferred = new DeferredException();

  OutOfBandAttemptObserver(AttemptObserver innerObserver, SerializedExecutor<Runnable> serializedExec) {
    this.serializedExec = serializedExec;
    this.innerObserver = innerObserver;
  }

  @Override
  public void beginState(final AttemptEvent event) {
    execute(() -> innerObserver.beginState(event));
  }

  @Override
  public void queryStarted(final UserRequest query, final String user) {
    execute(() -> innerObserver.queryStarted(query, user));
  }

  @Override
  public void commandPoolWait(long waitInMillis) {
    execute(() -> innerObserver.commandPoolWait(waitInMillis));
  }

  @Override
  public void planText(final String text, final long millisTaken) {
    execute(() -> innerObserver.planText(text, millisTaken));
  }

  @Override
  public void finalPrel(final Prel prel) {
    execute(() -> innerObserver.finalPrel(prel));
  }

  @Override
  public void recordExtraInfo(final String name, final byte[] bytes) {
    execute(() -> innerObserver.recordExtraInfo(name, bytes));
  }

  @Override
  public void planRelTransform(final PlannerPhase phase, final RelOptPlanner planner, final RelNode before, final RelNode after, final long millisTaken) {
    execute(() -> innerObserver.planRelTransform(phase, planner, before, after, millisTaken));
  }

  @Override
  public void planParallelStart() {
    execute(innerObserver::planParallelStart);
  }

  @Override
  public void planParallelized(final PlanningSet planningSet) {
    execute(() -> innerObserver.planParallelized(planningSet));
  }

  @Override
  public void planFindMaterializations(final long millisTaken) {
    execute(() -> innerObserver.planFindMaterializations(millisTaken));
  }

  @Override
  public void planNormalized(final long millisTaken, final List<RelNode> normalizedQueryPlans) {
    execute(() -> innerObserver.planNormalized(millisTaken, normalizedQueryPlans));
  }

  @Override
  public void planSubstituted(final DremioMaterialization materialization,
                              final List<RelNode> substitutions,
                              final RelNode target, final long millisTaken, boolean defaultReflection) {
    execute(() -> innerObserver.planSubstituted(materialization, substitutions, target, millisTaken, defaultReflection));
  }

  @Override
  public void substitutionFailures(Iterable<String> errors) {
    execute(() -> innerObserver.substitutionFailures(errors));
  }

  @Override
  public void planAccelerated(final SubstitutionInfo info) {
    execute(() -> innerObserver.planAccelerated(info));
  }

  @Override
  public void applyAccelDetails(final CachedAccelDetails accelDetails) {
    execute(() -> innerObserver.applyAccelDetails(accelDetails));
  }

  @Override
  public void planCompleted(final ExecutionPlan plan) {
    execute(() -> innerObserver.planCompleted(plan));
  }

  @Override
  public void execStarted(final QueryProfile profile) {
    execute(() -> innerObserver.execStarted(profile));
  }

  @Override
  public void execDataArrived(final RpcOutcomeListener<Ack> outcomeListener, final QueryWritableBatch result) {
    execute(() -> innerObserver.execDataArrived(outcomeListener, result));
  }

  @Override
  public void planJsonPlan(final String text) {
    execute(() -> innerObserver.planJsonPlan(text));
  }

  @Override
  public void planStart(final String rawPlan) {
    execute(() -> innerObserver.planStart(rawPlan));
  }

  @Override
  public void planValidated(final RelDataType rowType, final SqlNode node, final long millisTaken) {
    execute(() -> innerObserver.planValidated(rowType, node, millisTaken));
  }

  @Override
  public void planCacheUsed(int count) {
    execute(() -> innerObserver.planCacheUsed(count));
  }

  @Override
  public void planSerializable(final RelNode serializable) {
    execute(() -> innerObserver.planSerializable(serializable));
  }

  @Override
  public void planConvertedToRel(final RelNode converted, final long millisTaken) {
    execute(() -> innerObserver.planConvertedToRel(converted, millisTaken));
  }

  @Override
  public void planConvertedScan(final RelNode converted, final long millisTaken) {
    execute(() -> innerObserver.planConvertedScan(converted, millisTaken));
  }

  @Override
  public void planExpandView(final RelRoot expanded, final List<String> schemaPath, final int nestingLevel, final String sql) {
    execute(() -> innerObserver.planExpandView(expanded, schemaPath, nestingLevel, sql));
  }

  @Override
  public void plansDistributionComplete(final QueryWorkUnit unit) {
    execute(() -> innerObserver.plansDistributionComplete(unit));
  }

  @Override
  public void attemptCompletion(final UserResult result) {
    // make sure we have correct ordering (this should come after all previous observations).
    final CountDownLatch cd = new CountDownLatch(1);
    serializedExec.execute(() -> {
      try {
        UserResult finalResult = result;
        if (deferred.hasException()) {
          finalResult = finalResult.withException(deferred.getAndClear());
        }
        innerObserver.attemptCompletion(finalResult);
      } finally {
        cd.countDown();
      }
    });
    try{
      cd.await();
    } catch(InterruptedException ex){
      Thread.currentThread().interrupt();
    }

  }

  @Override
  public void executorsSelected(long millisTaken, int idealNumFragments, int idealNumNodes, int numExecutors, String detailsText) {
    execute(() -> innerObserver.executorsSelected(millisTaken, idealNumFragments, idealNumNodes, numExecutors, detailsText));
  }

  @Override
  public void recordsProcessed(long recordCount) {
    execute(() -> innerObserver.recordsProcessed(recordCount));
  }

  @Override
  public void planGenerationTime(final long millisTaken) {
    execute(() -> innerObserver.planGenerationTime(millisTaken));
  }

  @Override
  public void planAssignmentTime(final long millisTaken) {
    execute(() -> innerObserver.planAssignmentTime(millisTaken));
  }

  @Override
  public void fragmentsStarted(final long millisTaken, FragmentRpcSizeStats stats) {
    execute(() -> innerObserver.fragmentsStarted(millisTaken, stats));
  }

  @Override
  public void fragmentsActivated(final long millisTaken) {
    execute(() -> innerObserver.fragmentsActivated(millisTaken));
  }

  @Override
  public void activateFragmentFailed(Exception ex) {
    execute(() -> innerObserver.activateFragmentFailed(ex));
  }

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
    execute(() -> innerObserver.resourcesScheduled(resourceSchedulingDecisionInfo));
  }

  @Override
  public void updateReflectionsWithHints(ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
    execute(() -> innerObserver.updateReflectionsWithHints(reflectionExplanationsAndQueryDistance));
  }

  @Override
  public void tablesCollected(Iterable<DremioTable> tables) {
    execute(() -> innerObserver.tablesCollected(tables));
  }

  /**
   * Wraps the runnable so that any exception thrown will eventually cause the attempt
   * to fail when handling the {@link #attemptCompletion(UserResult)} callback
   */
  private void execute(Runnable runnable) {
    serializedExec.execute(() -> {
      try {
        runnable.run();
      } catch (Throwable ex) {
        deferred.addThrowable(ex);
      }
    });
  }
}
