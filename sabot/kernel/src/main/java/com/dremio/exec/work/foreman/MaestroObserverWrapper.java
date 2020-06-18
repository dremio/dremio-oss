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
package com.dremio.exec.work.foreman;

import com.dremio.exec.maestro.MaestroObserver;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.resource.ResourceSchedulingDecisionInfo;

public class MaestroObserverWrapper implements MaestroObserver {
  private final AttemptObserver observer;

  public MaestroObserverWrapper(final AttemptObserver observer) {
    this.observer = observer;
  }

  @Override
  public void beginState(AttemptEvent event) {
    observer.beginState(event);
  }

  @Override
  public void commandPoolWait(long waitInMillis) {
    observer.commandPoolWait(waitInMillis);
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
  public void plansDistributionComplete(QueryWorkUnit unit) {
      observer.plansDistributionComplete(unit);
  }

  @Override
  public void planCompleted(ExecutionPlan plan) {
      observer.planCompleted(plan);
  }

  @Override
  public void execStarted(UserBitShared.QueryProfile profile) {
      observer.execStarted(profile);
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
  public void fragmentsStarted(long millisTaken, UserBitShared.FragmentRpcSizeStats stats) {
      observer.fragmentsStarted(millisTaken, stats);
  }

  @Override
  public void fragmentsActivated(long millisTaken) {
      observer.fragmentsActivated(millisTaken);
  }

  @Override
  public void activateFragmentFailed(Exception ex) {
      observer.activateFragmentFailed(ex);
  }

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
      observer.resourcesScheduled(resourceSchedulingDecisionInfo);
  }

  @Override
  public void recordExtraInfo(String name, byte[] bytes) {
      observer.recordExtraInfo(name, bytes);
  }
}
