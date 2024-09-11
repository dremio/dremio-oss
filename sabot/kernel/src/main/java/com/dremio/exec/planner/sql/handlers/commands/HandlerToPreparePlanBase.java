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
package com.dremio.exec.planner.sql.handlers.commands;

import static com.dremio.exec.planner.physical.PlannerSettings.ENABLE_DYNAMIC_PARAM_PREPARE;
import static com.dremio.exec.planner.sql.handlers.SqlHandlerUtil.containsParameters;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.RelWithInfo;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.fragment.PlanFragmentsIndex;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserBitShared.AccelerationProfile;
import com.dremio.exec.proto.UserBitShared.PlannerPhaseRulesStats;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.reflection.hints.ReflectionExplanationsAndQueryDistance;
import com.dremio.resource.GroupResourceInformation;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

/**
 * Take a sql node, plan it and then return an async response.
 *
 * @param <T> The response class to provide.
 */
public abstract class HandlerToPreparePlanBase<T> implements CommandRunner<T> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(HandlerToPreparePlanBase.class);

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
  private RelDataType rowtype;
  private RelDataType preparedRowType;
  private final boolean isParameterEnabled;

  public HandlerToPreparePlanBase(
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
    this.isParameterEnabled =
        context.getOptions().getOption(ENABLE_DYNAMIC_PARAM_PREPARE) && containsParameters(sqlNode);
  }

  @Override
  public double plan() throws Exception {
    try {

      if (isParameterEnabled) {
        SqlValidatorAndToRelContext sqlValidatorAndToRelContext =
            config
                .getConverter()
                .getUserQuerySqlValidatorAndToRelContextBuilderFactory()
                .builder()
                .build();
        SqlNode validatedNode = sqlValidatorAndToRelContext.validate(sqlNode);
        rowtype = sqlValidatorAndToRelContext.getValidator().getValidatedNodeType(validatedNode);
        preparedRowType =
            sqlValidatorAndToRelContext.getValidator().getParameterRowType(validatedNode);

        final long handle = PREPARE_ID.getAndIncrement();
        state =
            ServerPreparedStatementState.newBuilder()
                .setHandle(handle)
                .setSqlQuery(sql)
                .setPrepareId(context.getQueryId())
                .build();
      } else {
        final RecordingObserver recording = getRecordingObserver();
        final AttemptObservers observers = AttemptObservers.of(observer, recording);
        observers.planStart(sql);
        plan = handler.getPlan(config.cloneWithNewObserver(observers), sql, sqlNode);
        PreparedPlan prepared =
            new PreparedPlan(
                context.getQueryId(),
                context.getQueryUserName(),
                context.getQueryRequiresGroupsInfo(),
                sql,
                plan,
                recording);

        final long handle = PREPARE_ID.getAndIncrement();
        state =
            ServerPreparedStatementState.newBuilder()
                .setHandle(handle)
                .setSqlQuery(sql)
                .setPrepareId(context.getQueryId())
                .build();
        planCache.put(handle, prepared);

        // record a partial plan so that we can grab metadata and use it (for example during view
        // creation of via sql).
        observers.planCompleted(
            new ExecutionPlan(
                context.getQueryId(), plan, ImmutableList.of(), new PlanFragmentsIndex.Builder()),
            null);
      }

      return 1;
    } catch (Exception ex) {
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  protected RecordingObserver getRecordingObserver() {
    return new RecordingObserver();
  }

  @Override
  public void close() throws Exception {
    // no-op
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SYNC_RESPONSE;
  }

  @Override
  public String getDescription() {
    return "prepare; query";
  }

  public interface ObserverCall {
    void doCall(AttemptObserver observer);
  }

  /** Collects planning calls to be carried to execution. So not all methods are overridden. */
  public static class RecordingObserver extends AbstractAttemptObserver {

    protected final List<ObserverCall> calls = new ArrayList<>();

    @Override
    public void planStart(final String rawPlan) {
      calls.add(observer -> observer.planStart(rawPlan));
    }

    @Override
    public void resourcesPlanned(GroupResourceInformation resourceInformation, long millisTaken) {
      calls.add(observer -> observer.resourcesPlanned(resourceInformation, millisTaken));
    }

    @Override
    public void planValidated(
        final RelDataType rowType,
        final SqlNode node,
        final long millisTaken,
        boolean isMaterializationCacheInitialized) {
      calls.add(
          observer ->
              observer.planValidated(
                  rowType, node, millisTaken, isMaterializationCacheInitialized));
    }

    @Override
    public void planSerializable(final RelNode plan) {
      calls.add(observer -> observer.planSerializable(plan));
    }

    @Override
    public void planConvertedToRel(final RelNode converted, final long millisTaken) {
      calls.add(observer -> observer.planConvertedToRel(converted, millisTaken));
    }

    @Override
    public void planConvertedScan(final RelNode converted, final long millisTaken) {
      calls.add(observer -> observer.planConvertedScan(converted, millisTaken));
    }

    @Override
    public void planExpandView(
        final RelRoot expanded,
        final List<String> schemaPath,
        final int nestingLevel,
        final String sql) {
      calls.add(observer -> observer.planExpandView(expanded, schemaPath, nestingLevel, sql));
    }

    @Override
    public void planSubstituted(
        final DremioMaterialization materialization,
        final List<RelWithInfo> substitutions,
        final RelWithInfo target,
        final long millisTaken,
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
    public void planText(final String text, final long millisTaken) {
      calls.add(observer -> observer.planText(text, millisTaken));
    }

    @Override
    public void planRelTransform(
        final PlannerPhase phase,
        final RelOptPlanner planner,
        final RelNode before,
        final RelNode after,
        final long millisTaken,
        List<PlannerPhaseRulesStats> rulesBreakdownStats) {
      calls.add(
          observer ->
              observer.planRelTransform(
                  phase, planner, before, after, millisTaken, rulesBreakdownStats));
    }

    @Override
    public void planFindMaterializations(final long millisTaken) {
      calls.add(observer -> observer.planFindMaterializations(millisTaken));
    }

    @Override
    public void planNormalized(
        final long millisTaken, final List<RelWithInfo> normalizedQueryPlans) {
      calls.add(observer -> observer.planNormalized(millisTaken, normalizedQueryPlans));
    }

    @Override
    public void planSubstituted(final long millisTaken) {
      calls.add(observer -> observer.planSubstituted(millisTaken));
    }

    @Override
    public void planAccelerated(final SubstitutionInfo info) {
      calls.add(observer -> observer.planAccelerated(info));
    }

    @Override
    public void planJsonPlan(final String text) {
      calls.add(observer -> observer.planJsonPlan(text));
    }

    @Override
    public void planRefreshDecision(String text, long millisTaken) {
      calls.add(observer -> observer.planRefreshDecision(text, millisTaken));
    }

    @Override
    public void restoreAccelerationProfileFromCachedPlan(AccelerationProfile accelerationProfile) {
      calls.add(observer -> observer.restoreAccelerationProfileFromCachedPlan(accelerationProfile));
    }

    @Override
    public void planCacheUsed(int count) {
      calls.add(observer -> observer.planCacheUsed(count));
    }

    @Override
    public void updateReflectionsWithHints(
        ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
      calls.add(
          observer -> observer.updateReflectionsWithHints(reflectionExplanationsAndQueryDistance));
    }

    public void replay(AttemptObserver observer) {
      for (ObserverCall c : calls) {
        c.doCall(observer);
      }
    }
  }

  protected QueryContext getContext() {
    return context;
  }

  protected PhysicalPlan getPlan() {
    return plan;
  }

  protected ServerPreparedStatementState getState() {
    return state;
  }

  protected RelDataType getRowType() {
    return rowtype;
  }

  protected RelDataType getPreparedRowType() {
    return preparedRowType;
  }

  protected boolean isParameterEnabled() {
    return isParameterEnabled;
  }
}
