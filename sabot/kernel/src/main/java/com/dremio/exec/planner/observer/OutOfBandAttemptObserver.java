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
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.resource.ResourceSchedulingDecisionInfo;

/**
 * Does query observations in order but not in the query execution thread. This
 * ensures two things:
 * - any blocking commands don't block the underlying thread
 * - any exceptions don't bleed into the caller.
 */
public class OutOfBandAttemptObserver implements AttemptObserver {

  private final SerializedExecutor serializedExec;
  private final AttemptObserver innerObserver;
  private final DeferredException deferred = new DeferredException();

  public OutOfBandAttemptObserver(AttemptObserver innerObserver, SerializedExecutor serializedExec) {
    super();
    this.serializedExec = serializedExec;
    this.innerObserver = innerObserver;
  }

  @Override
  public void queryStarted(final UserRequest query, final String user) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.queryStarted(query, user);
      }});
  }

  @Override
  public void planText(final String text, final long millisTaken) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planText(text, millisTaken);
      }
    });
  }

  @Override
  public void finalPrel(final Prel prel) {
    serializedExec.execute(new DeferredRunnable() {
        @Override
        public void doRun() {
          innerObserver.finalPrel(prel);
        }
      });
  }

  @Override
  public void recordExtraInfo(final String name, final byte[] bytes) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.recordExtraInfo(name, bytes);
      }
    });
  }

  @Override
  public void planRelTransform(final PlannerPhase phase, final RelOptPlanner planner, final RelNode before, final RelNode after, final long millisTaken) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planRelTransform(phase, planner, before, after, millisTaken);
      }
    });
  }

  @Override
  public void planParallelStart() {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planParallelStart();
      }
    });
  }

  @Override
  public void planParallelized(final PlanningSet planningSet) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planParallelized(planningSet);
      }
    });
  }

  @Override
  public void planFindMaterializations(final long millisTaken) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planFindMaterializations(millisTaken);
      }
    });
  }

  @Override
  public void planNormalized(final long millisTaken, final List<RelNode> normalizedQueryPlans) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planNormalized(millisTaken, normalizedQueryPlans);
      }
    });
  }

  @Override
  public void planSubstituted(final DremioMaterialization materialization,
                              final List<RelNode> substitutions,
                              final RelNode target, final long millisTaken) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planSubstituted(materialization, substitutions, target, millisTaken);
      }
    });
  }

  @Override
  public void substitutionFailures(Iterable<String> errors) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      void doRun() {
        innerObserver.substitutionFailures(errors);
      }
    });
  }

  @Override
  public void planAccelerated(final SubstitutionInfo info) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planAccelerated(info);
      }
    });
  }

  @Override
  public void planCompleted(final ExecutionPlan plan) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planCompleted(plan);
      }
    });  }

  @Override
  public void execStarted(final QueryProfile profile) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.execStarted(profile);
      }
    });
  }

  @Override
  public void execDataArrived(final RpcOutcomeListener<Ack> outcomeListener, final QueryWritableBatch result) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.execDataArrived(outcomeListener, result);
      }
    });
  }

  @Override
  public void planJsonPlan(final String text) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      public void doRun() {
        innerObserver.planJsonPlan(text);
      }
    });
  }

  @Override
  public void planStart(final String rawPlan) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.planStart(rawPlan);
      }});
  }

  @Override
  public void planValidated(final RelDataType rowType, final SqlNode node, final long millisTaken) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.planValidated(rowType, node, millisTaken);
      }});
  }

  @Override
  public void planSerializable(final RelNode serializable) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.planSerializable(serializable);
      }});
  }

  @Override
  public void planConvertedToRel(final RelNode converted, final long millisTaken) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.planConvertedToRel(converted, millisTaken);
      }});
  }

  @Override
  public void planConvertedScan(final RelNode converted, final long millisTaken) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.planConvertedScan(converted, millisTaken);
      }});
  }

  @Override
  public void planExpandView(final RelRoot expanded, final List<String> schemaPath, final int nestingLevel, final String sql) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.planExpandView(expanded, schemaPath, nestingLevel, sql);
      }});
  }

  @Override
  public void plansDistributionComplete(final QueryWorkUnit unit) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.plansDistributionComplete(unit);
      }});
  }

  private abstract class DeferredRunnable implements Runnable {
    public void run(){
      try {
        doRun();
      } catch (Throwable ex) {
        deferred.addThrowable(ex);
      }
    }

    abstract void doRun();
  }


  @Override
  public void attemptCompletion(final UserResult result) {
    // make sure we have correct ordering (this should come after all previous observations).
    final CountDownLatch cd = new CountDownLatch(1);
    serializedExec.execute(new Runnable() {
      @Override
      public void run() {
        try {
          UserResult finalResult = result;
          if(deferred.hasException()){
            finalResult = finalResult.withException(deferred.getAndClear());
          }
          innerObserver.attemptCompletion(finalResult);
        } finally {
          cd.countDown();
        }
      }
    });
    try{
      cd.await();
    } catch(InterruptedException ex){
      Thread.currentThread().interrupt();
    }

  }

  @Override
  public void planGenerationTime(final long millisTaken) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.planGenerationTime(millisTaken);
      }});
  }

  @Override
  public void planAssignmentTime(final long millisTaken) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.planAssignmentTime(millisTaken);
      }});
  }

  @Override
  public void intermediateFragmentScheduling(final long millisTaken) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.intermediateFragmentScheduling(millisTaken);
      }});
  }

  @Override
  public void leafFragmentScheduling(final long millisTaken) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.leafFragmentScheduling(millisTaken);
      }});
  }

  @Override
  public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
    serializedExec.execute(new DeferredRunnable(){
      @Override
      public void doRun() {
        innerObserver.resourcesScheduled(resourceSchedulingDecisionInfo);
      }});
  }

  @Override
  public void tablesCollected(Iterable<DremioTable> tables) {
    serializedExec.execute(new DeferredRunnable() {
      @Override
      void doRun() {
        innerObserver.tablesCollected(tables);
      }
    });
  }
}
