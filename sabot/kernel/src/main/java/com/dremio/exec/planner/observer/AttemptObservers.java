/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.DremioRelOptMaterialization;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.resource.ResourceSchedulingDecisionInfo;

/**
 * Collection of observers.
 */
public class AttemptObservers implements AttemptObserver {

  private final List<AttemptObserver> observers = new LinkedList<>();

  // use #of()
  private AttemptObservers() {
  }

  @Override
  public void queryStarted(UserRequest query, String user) {
    for (final AttemptObserver observer : observers) {
      observer.queryStarted(query, user);
    }
  }

  @Override
  public void planStart(String rawPlan) {
    for (final AttemptObserver observer : observers) {
      observer.planStart(rawPlan);
    }
  }

  @Override
  public void planValidated(RelDataType rowType, SqlNode node, long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.planValidated(rowType, node, millisTaken);
    }
  }

  @Override
  public void planSerializable(RelNode serializable) {
    for (final AttemptObserver observer : observers) {
      observer.planSerializable(serializable);
    }
  }

  @Override
  public void planConvertedToRel(RelNode converted, long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.planConvertedToRel(converted, millisTaken);
    }
  }

  @Override
  public void planConvertedScan(RelNode converted, long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.planConvertedScan(converted, millisTaken);
    }
  }

  @Override
  public void planExpandView(RelRoot expanded, List<String> schemaPath, int nestingLevel, String sql) {
    for (final AttemptObserver observer : observers) {
      observer.planExpandView(expanded, schemaPath, nestingLevel, sql);
    }
  }

  @Override
  public void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after, long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.planRelTransform(phase, planner, before, after, millisTaken);
    }
  }

  @Override
  public void planText(String text, long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.planText(text, millisTaken);
    }
  }

  @Override
  public void finalPrel(Prel prel) {
    for (final AttemptObserver observer : observers) {
      observer.finalPrel(prel);
    }
  }

  @Override
  public void planParallelStart() {
    for (final AttemptObserver observer : observers) {
      observer.planParallelStart();
    }
  }

  @Override
  public void planParallelized(PlanningSet planningSet) {
    for (final AttemptObserver observer : observers) {
      observer.planParallelized(planningSet);
    }
  }

  @Override
  public void plansDistributionComplete(QueryWorkUnit unit) {
    for (final AttemptObserver observer : observers) {
      observer.plansDistributionComplete(unit);
    }
  }

  @Override
  public void planFindMaterializations(long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.planFindMaterializations(millisTaken);
    }
  }

  @Override
  public void planNormalized(long millisTaken, List<RelNode> normalizedQueryPlans) {
    for (final AttemptObserver observer : observers) {
      observer.planNormalized(millisTaken, normalizedQueryPlans);
    }
  }

  @Override
  public void planSubstituted(DremioRelOptMaterialization materialization, List<RelNode> substitutions,
                              RelNode target, long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.planSubstituted(materialization, substitutions, target, millisTaken);
    }
  }

  @Override
  public void substitutionFailures(Iterable<String> errors) {
    for (final AttemptObserver observer : observers) {
      observer.substitutionFailures(errors);
    }
  }

  @Override
  public void planAccelerated(SubstitutionInfo info) {
    for (final AttemptObserver observer : observers) {
      observer.planAccelerated(info);
    }
  }

  @Override
  public void planCompleted(ExecutionPlan plan) {
    for (final AttemptObserver observer : observers) {
      observer.planCompleted(plan);
    }
  }

  @Override
  public void execStarted(QueryProfile profile) {
    for (final AttemptObserver observer : observers) {
      observer.execStarted(profile);
    }
  }

  @Override
  public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
    for (final AttemptObserver observer : observers) {
      observer.execDataArrived(outcomeListener, result);
    }
  }

  @Override
  public void planJsonPlan(String text) {
    for (final AttemptObserver observer : observers) {
      observer.planJsonPlan(text);
    }
  }

  @Override
  public void attemptCompletion(UserResult result) {
    for (final AttemptObserver observer : observers) {
      observer.attemptCompletion(result);
    }
  }

  @Override
  public void planGenerationTime(long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.planGenerationTime(millisTaken);
    }
  }

  @Override
  public void planAssignmentTime(long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.planAssignmentTime(millisTaken);
    }
  }

  @Override
  public void intermediateFragmentScheduling(long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.intermediateFragmentScheduling(millisTaken);
    }
  }

  @Override
  public void leafFragmentScheduling(long millisTaken) {
    for (final AttemptObserver observer : observers) {
      observer.leafFragmentScheduling(millisTaken);
    }
  }

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
    for (final AttemptObserver observer : observers) {
      observer.resourcesScheduled(resourceSchedulingDecisionInfo);
    }
  }

  @Override
  public void recordExtraInfo(String name, byte[] bytes) {
    for (final AttemptObserver observer : observers) {
      observer.recordExtraInfo(name, bytes);
    }
  }

  @Override
  public void tablesCollected(Iterable<DremioTable> tables) {
    observers.forEach(o -> o.tablesCollected(tables));
  }

  /**
   * Add to the collection of observers.
   *
   * @param observer attempt observer
   */
  public void add(final AttemptObserver observer) {
    observers.add(observer);
  }

  /**
   * Create a collection of observers.
   *
   * @param observers attempt observers
   * @return attempt observers
   */
  public static AttemptObservers of(final AttemptObserver... observers) {
    final AttemptObservers chain = new AttemptObservers();
    for (final AttemptObserver observer : observers) {
      chain.add(observer);
    }

    return chain;
  }


}
