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
package com.dremio.exec.planner.sql.handlers.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.sql.DremioRelOptMaterialization;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;

/**
 * Take a sql node, plan it and then return an async response.
 */
public class HandlerToPreparePlan implements CommandRunner<CreatePreparedStatementResp> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HandlerToPreparePlan.class);

  private static final AtomicLong PREPARE_ID = new AtomicLong(0);

  private final QueryContext context;
  private final SqlNode sqlNode;
  private final SqlToPlanHandler handler;
  private final Cache<Long, PreparedPlan> planCache;
  private final String sql;
  private final AttemptObserver observer;
  private final SqlHandlerConfig config;

  private PhysicalPlan plan;
  private ServerPreparedStatementState state;


  public HandlerToPreparePlan(
      QueryContext context,
      SqlNode sqlNode,
      SqlToPlanHandler handler,
      Cache<Long, PreparedPlan> planCache,
      String sql,
      AttemptObserver observer,
      SqlHandlerConfig config) {
    this.context = context;
    this.sqlNode = sqlNode;
    this.handler = handler;
    this.planCache = planCache;
    this.sql = sql;
    this.observer = observer;
    this.config = config;
  }

  @Override
  public double plan() throws Exception {
    try{
      final RecordingObserver recording = new RecordingObserver();
      final AttemptObservers observers = AttemptObservers.of(observer, recording);
      observers.planStart(sql);
      plan = handler.getPlan(config.cloneWithNewObserver(observers), sql, sqlNode);
      PreparedPlan prepared = new PreparedPlan(context.getQueryId(), context.getQueryUserName(), sql, plan, recording);
      final Long handle = PREPARE_ID.getAndIncrement();
      state = ServerPreparedStatementState.newBuilder()
        .setHandle(handle)
        .setSqlQuery(sql)
        .setPrepareId(context.getQueryId())
        .build();
      planCache.put(handle, prepared);

      // record a partial plan so that we can grab metadata and use it (for example during view creation of via sql).
      observers.planCompleted(new ExecutionPlan(plan, ImmutableList.<PlanFragment>of()));
      return 1;
    }catch(Exception ex){
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }


  @Override
  public CreatePreparedStatementResp execute() throws Exception {
    return PreparedStatementProvider.build(plan.getRoot().getSchema(context.getFunctionRegistry()), state,
      context.getQueryId(), context.getSession().getCatalogName());
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SYNC_RESPONSE;
  }

  @Override
  public String getDescription() {
    return "prepare; query";
  }

  private interface ObserverCall {
    void doCall(AttemptObserver observer);
  }

  /**
   * Collects planning calls to be carried to execution. So not all methods are overridden.
   */
  public static class RecordingObserver extends AbstractAttemptObserver {

    private final List<ObserverCall> calls = new ArrayList<>();

    @Override
    public void planStart(final String rawPlan) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planStart(rawPlan);
        }});
    }

    @Override
    public void planValidated(final RelDataType rowType, final SqlNode node, final long millisTaken) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planValidated(rowType, node, millisTaken);
        }});
    }

    @Override
    public void planSerializable(final RelNode plan) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planSerializable(plan);
        }});
    }

    @Override
    public void planConvertedToRel(final RelNode converted, final long millisTaken) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planConvertedToRel(converted, millisTaken);
        }});
    }

    @Override
    public void planConvertedScan(final RelNode converted, final long millisTaken) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planConvertedScan(converted, millisTaken);
        }});
    }

    @Override
    public void planExpandView(final RelRoot expanded, final List<String> schemaPath, final int nestingLevel, final String sql) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planExpandView(expanded, schemaPath, nestingLevel, sql);
        }});
    }

    @Override
    public void planSubstituted(final DremioRelOptMaterialization materialization,
                                final List<RelNode> substitutions,
                                final RelNode target, final long millisTaken) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planSubstituted(materialization, substitutions, target, millisTaken);
        }});
    }

    @Override
    public void planText(final String text, final long millisTaken) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planText(text, millisTaken);
        }});
    }

    @Override
    public void planRelTransform(final PlannerPhase phase, final RelOptPlanner planner, final RelNode before, final RelNode after, final long millisTaken) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planRelTransform(phase, planner, before, after, millisTaken);
        }});
    }

    @Override
    public void planFindMaterializations(final long millisTaken) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planFindMaterializations(millisTaken);
        }});
    }


    @Override
    public void planNormalized(final long millisTaken, final List<RelNode> normalizedQueryPlans) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planNormalized(millisTaken, normalizedQueryPlans);
        }});
    }

    @Override
    public void planAccelerated(final SubstitutionInfo info) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planAccelerated(info);
        }});
    }

    @Override
    public void planJsonPlan(final String text) {
      calls.add(new ObserverCall(){
        @Override
        public void doCall(AttemptObserver observer) {
          observer.planJsonPlan(text);
        }});
    }

    public void replay(AttemptObserver observer) {
      for(ObserverCall c : calls){
        c.doCall(observer);
      }
    }

  }

}
