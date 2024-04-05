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
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import java.util.LinkedList;
import java.util.List;

/** Maintains a list of observers & delivers notification to each of them */
public class MaestroObservers implements MaestroObserver {
  private List<MaestroObserver> chain = new LinkedList<>();

  // create object using 'of'
  private MaestroObservers() {}

  @Override
  public void beginState(AttemptEvent event) {
    for (final MaestroObserver observer : chain) {
      observer.beginState(event);
    }
  }

  @Override
  public void commandPoolWait(long waitInMillis) {
    for (final MaestroObserver observer : chain) {
      observer.commandPoolWait(waitInMillis);
    }
  }

  @Override
  public void recordExtraInfo(String name, byte[] bytes) {
    for (final MaestroObserver observer : chain) {
      observer.recordExtraInfo(name, bytes);
    }
  }

  @Override
  public void planCompleted(final ExecutionPlan plan, final BatchSchema batchSchema) {
    for (final MaestroObserver observer : chain) {
      observer.planCompleted(plan, batchSchema);
    }
  }

  @Override
  public void execStarted(UserBitShared.QueryProfile profile) {
    for (final MaestroObserver observer : chain) {
      observer.execStarted(profile);
    }
  }

  @Override
  public void executorsSelected(
      long millisTaken,
      int idealNumFragments,
      int idealNumNodes,
      int numExecutors,
      String detailsText) {
    for (final MaestroObserver observer : chain) {
      observer.executorsSelected(
          millisTaken, idealNumFragments, idealNumNodes, numExecutors, detailsText);
    }
  }

  @Override
  public void planParallelStart() {
    for (final MaestroObserver observer : chain) {
      observer.planParallelStart();
    }
  }

  @Override
  public void planParallelized(PlanningSet planningSet) {
    for (final MaestroObserver observer : chain) {
      observer.planParallelized(planningSet);
    }
  }

  @Override
  public void planAssignmentTime(long millisTaken) {
    for (final MaestroObserver observer : chain) {
      observer.planAssignmentTime(millisTaken);
    }
  }

  @Override
  public void planGenerationTime(long millisTaken) {
    for (final MaestroObserver observer : chain) {
      observer.planGenerationTime(millisTaken);
    }
  }

  @Override
  public void plansDistributionComplete(QueryWorkUnit unit) {
    for (final MaestroObserver observer : chain) {
      observer.plansDistributionComplete(unit);
    }
  }

  @Override
  public void recordsProcessed(long recordCount) {
    for (final MaestroObserver observer : chain) {
      observer.recordsProcessed(recordCount);
    }
  }

  @Override
  public void recordsOutput(long recordCount) {
    for (final MaestroObserver observer : chain) {
      observer.recordsOutput(recordCount);
    }
  }

  @Override
  public void fragmentsStarted(long millisTaken, UserBitShared.FragmentRpcSizeStats stats) {
    for (final MaestroObserver observer : chain) {
      observer.fragmentsStarted(millisTaken, stats);
    }
  }

  @Override
  public void fragmentsActivated(long millisTaken) {
    for (final MaestroObserver observer : chain) {
      observer.fragmentsActivated(millisTaken);
    }
  }

  @Override
  public void activateFragmentFailed(Exception ex) {
    for (final MaestroObserver observer : chain) {
      observer.activateFragmentFailed(ex);
    }
  }

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
    for (final MaestroObserver observer : chain) {
      observer.resourcesScheduled(resourceSchedulingDecisionInfo);
    }
  }

  /**
   * Add to the collection of observers.
   *
   * @param observer attempt observer
   */
  public void add(final MaestroObserver observer) {
    chain.add(observer);
  }

  /**
   * Create a collection of observers.
   *
   * @param observers attempt observers
   * @return attempt observers
   */
  public static MaestroObservers of(final MaestroObserver... observers) {
    final MaestroObservers chain = new MaestroObservers();
    for (final MaestroObserver observer : observers) {
      chain.add(observer);
    }
    return chain;
  }
}
