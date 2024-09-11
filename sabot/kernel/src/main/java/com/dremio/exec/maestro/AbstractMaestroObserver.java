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
package com.dremio.exec.maestro;

import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeStats;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.resource.ResourceSchedulingDecisionInfo;

public abstract class AbstractMaestroObserver implements MaestroObserver {

  public static final AbstractMaestroObserver NOOP = new AbstractMaestroObserver() {};

  @Override
  public void beginState(AttemptEvent event) {}

  @Override
  public void planParallelStart() {}

  @Override
  public void planParallelized(PlanningSet planningSet) {}

  @Override
  public void recordExtraInfo(String name, byte[] bytes) {}

  @Override
  public void planCompleted(final ExecutionPlan plan, final BatchSchema batchSchema) {}

  @Override
  public void plansDistributionComplete(QueryWorkUnit unit) {}

  @Override
  public void commandPoolWait(long waitInMillis) {}

  @Override
  public void execStarted(QueryProfile profile) {}

  @Override
  public void executorsSelected(
      long millisTaken,
      int idealNumFragments,
      int idealNumNodes,
      int numExecutors,
      String detailsText) {}

  @Override
  public void recordsOutput(CoordinationProtos.NodeEndpoint endpoint, long recordCount) {}

  @Override
  public void outputLimited() {}

  @Override
  public void planGenerationTime(long millisTaken) {}

  @Override
  public void planAssignmentTime(long millisTaken) {}

  @Override
  public void fragmentsStarted(long millisTaken, FragmentRpcSizeStats stats) {}

  @Override
  public void fragmentsActivated(long millisTaken) {}

  @Override
  public void activateFragmentFailed(Exception ex) {}

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {}

  @Override
  public void putProfileFailed() {}
}
