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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.ops.OperatorMetricRegistry;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.PlanCaptureAttemptObserver;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.service.jobtelemetry.GetQueryProfileRequest;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.jobtelemetry.PutPlanningProfileRequest;
import com.dremio.service.jobtelemetry.PutTailProfileRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

/**
 * Tracker for the profile of a query attempt.
 */
class AttemptProfileTracker {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(AttemptProfileTracker.class);
  private static final ObjectWriter JSON_PRETTY_SERIALIZER = new ObjectMapper().writerWithDefaultPrettyPrinter();

  private final UserBitShared.QueryId queryId;
  private final QueryContext queryContext;
  private final String queryDescription;
  private final Supplier<UserBitShared.QueryResult.QueryState> queryStateSupplier;
  private final PlanCaptureAttemptObserver capturer;
  private final AttemptObserver mergedObserver;
  private final JobTelemetryClient jobTelemetryClient;
  private final UserBitShared.QueryProfile.Builder planningProfileBuilder =
    UserBitShared.QueryProfile.newBuilder();

  // the following mutable variables capture ongoing query status
  private long commandPoolWait;
  private long startPlanningTime;
  private long endPlanningTime;  // NB: tracks the end of both planning and resource scheduling. Name match the profile object, which was kept intact for legacy reasons
  private long startTime;
  private long endTime;
  private List<AttemptEvent> stateList = new ArrayList<>();

  private volatile UserBitShared.QueryProfile planningProfile;
  private volatile UserBitShared.QueryId prepareId;
  private volatile String cancelReason;
  private volatile UserException userException;
  private volatile ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo;

  AttemptProfileTracker(UserBitShared.QueryId queryId,
                        QueryContext queryContext,
                        String queryDescription,
                        Supplier<UserBitShared.QueryResult.QueryState> queryStateSupplier,
                        AttemptObserver observer,
                        JobTelemetryClient jobTelemetryClient) {
    this.queryId = queryId;
    this.queryContext = queryContext;
    this.queryDescription = queryDescription;
    this.queryStateSupplier = queryStateSupplier;
    this.jobTelemetryClient = jobTelemetryClient;

    // add additional observers.
    final OptionManager optionManager = queryContext.getOptions();
    capturer = new PlanCaptureAttemptObserver(
      optionManager.getOption(PlannerSettings.VERBOSE_PROFILE),
      optionManager.getOption(PlannerSettings.INCLUDE_DATASET_PROFILE),
      queryContext.getFunctionRegistry(),
      queryContext.getAccelerationManager().newPopulator(), RelSerializerFactory
      .getProfileFactory(queryContext.getConfig(), queryContext.getScanResult()));

    mergedObserver = AttemptObservers.of(observer, capturer, new TimeMarker());
  }

  AttemptObserver getObserver() {
    return mergedObserver;
  }

  void setPrepareId(UserBitShared.QueryId prepareId) {
    this.prepareId = prepareId;
  }

  void setCancelReason(String cancelReason) {
    this.cancelReason = cancelReason;
  }

  String getCancelReason() {
    return cancelReason;
  }

  // Send planning profile to JTS.
  ListenableFuture<Empty> sendPlanningProfile() {
    return jobTelemetryClient.getFutureStub()
      .putQueryPlanningProfile(
        PutPlanningProfileRequest.newBuilder()
          .setQueryId(queryId)
          .setProfile(getPlanningProfile())
          .build()
      );
  }

  // Send tail profile to JTS (used after query terminates).
  void sendTailProfile(UserException userException) {
    this.userException = userException;

    jobTelemetryClient.getBlockingStub()
      .putQueryTailProfile(
        PutTailProfileRequest.newBuilder()
          .setQueryId(queryId)
          .setProfile(getPlanningProfile())
          .build()
      );
  }

  // Get the latest planning profile.
  synchronized UserBitShared.QueryProfile getPlanningProfile() {
    UserBitShared.QueryProfile.Builder builder = UserBitShared.QueryProfile.newBuilder();

    if (planningProfile == null) {
      addPlanningDetails(builder);
      if (builder.getPlanningEnd() > 0) {
        // if planning has completed, this part will remain unchanged. save it to
        // optimise future requests.
        planningProfile = builder.build();
      }
    } else {
      builder.mergeFrom(planningProfile);
    }

    addLatestState(builder);
    return builder.build();
  }

  // fetch full profile (including executor profiles) from JTS.
  UserBitShared.QueryProfile getFullProfile() {
    return jobTelemetryClient.getBlockingStub()
      .getQueryProfile(
        GetQueryProfileRequest.newBuilder()
          .setQueryId(queryId)
          .build()
      ).getProfile();
  }

  // add planner related details to the profile builder.
  private void addPlanningDetails(UserBitShared.QueryProfile.Builder builder) {
    builder.setQuery(queryDescription);
    builder.setUser(queryContext.getQueryUserName());
    builder.setId(queryId);
    builder.setForeman(queryContext.getCurrentEndpoint());
    builder.setDremioVersion(DremioVersionInfo.getVersion());
    builder.setCommandPoolWaitMillis(commandPoolWait);
    builder.setPlanningStart(startPlanningTime);
    if (endPlanningTime > 0) {
      builder.setPlanningEnd(endPlanningTime);
    }
    if(stateList != null) {
      builder.clearStateList().addAllStateList(stateList);
    }

    try {
      builder.setNonDefaultOptionsJSON(JSON_PRETTY_SERIALIZER.writeValueAsString(queryContext.getNonDefaultOptions()));
    } catch (Exception e) {
      logger.warn("Failed to serialize the non-default option list to JSON", e);
      builder.setNonDefaultOptionsJSON("Failed to serialize the non-default " +
        "option list to JSON: " + e.getMessage());
    }
    builder.setOperatorTypeMetricsMap(OperatorMetricRegistry.getCoreOperatorTypeMetricsMap());

    if (capturer != null) {
      builder.setTotalFragments(capturer.getNumFragments());
      builder.setAccelerationProfile(capturer.getAccelerationProfile());
      builder.addAllDatasetProfile(capturer.getDatasets());

      final String planText = capturer.getText();
      if (planText != null) {
        builder.setPlan(planText);
      }

      final String planJson = capturer.getJson();
      if (planJson != null) {
        builder.setJsonPlan(planJson);
      }

      final String fullSchema = capturer.getFullSchema();
      if (fullSchema != null) {
        builder.setFullSchema(fullSchema);
      }

      final List<UserBitShared.PlanPhaseProfile> planPhases = capturer.getPlanPhases();
      if (planPhases != null) {
        builder.addAllPlanPhases(planPhases);
      }

      byte[] serializedPlan = capturer.getSerializedPlan();
      if (serializedPlan != null) {
        builder.setSerializedPlan(ByteString.copyFrom(serializedPlan));
      }
    }

    // get stats from schema tree provider
    builder.addAllPlanPhases(queryContext.getCatalog().getMetadataStatsCollector().getPlanPhaseProfiles());

    if (prepareId != null) {
      builder.setPrepareId(prepareId);
    }
    if (queryContext.getSession().getClientInfos() != null) {
      builder.setClientInfo(queryContext.getSession().getClientInfos());
    }

    if (resourceSchedulingDecisionInfo != null) {
      builder.setResourceSchedulingProfile(getResourceSchedulingProfile());
    }
  }

  private UserBitShared.ResourceSchedulingProfile getResourceSchedulingProfile() {
    UserBitShared.ResourceSchedulingProfile.Builder resourceBuilder = UserBitShared.ResourceSchedulingProfile
      .newBuilder();
    if (resourceSchedulingDecisionInfo.getQueueId() != null) {
      resourceBuilder.setQueueId(resourceSchedulingDecisionInfo.getQueueId());
    }
    if (resourceSchedulingDecisionInfo.getQueueName() != null) {
      resourceBuilder.setQueueName(resourceSchedulingDecisionInfo.getQueueName());
    }
    if (resourceSchedulingDecisionInfo.getRuleContent() != null) {
      resourceBuilder.setRuleContent(resourceSchedulingDecisionInfo.getRuleContent());
    }
    if (resourceSchedulingDecisionInfo.getRuleId() != null) {
      resourceBuilder.setRuleId(resourceSchedulingDecisionInfo.getRuleId());
    }
    if (resourceSchedulingDecisionInfo.getRuleName() != null) {
      resourceBuilder.setRuleName(resourceSchedulingDecisionInfo.getRuleName());
    }
    if (resourceSchedulingDecisionInfo.getRuleAction() != null) {
      resourceBuilder.setRuleAction(resourceSchedulingDecisionInfo.getRuleAction());
    }
    final ResourceSchedulingProperties schedulingProperties = resourceSchedulingDecisionInfo
      .getResourceSchedulingProperties();
    if (schedulingProperties != null) {
      UserBitShared.ResourceSchedulingProperties.Builder resourcePropsBuilder =
        UserBitShared.ResourceSchedulingProperties.newBuilder();
      if (schedulingProperties.getClientType() != null) {
        resourcePropsBuilder.setClientType(schedulingProperties.getClientType());
      }
      if (schedulingProperties.getQueryType() != null) {
        resourcePropsBuilder.setQueryType(schedulingProperties.getQueryType());
      }
      if (schedulingProperties.getQueryCost() != null) {
        resourcePropsBuilder.setQueryCost(schedulingProperties.getQueryCost());
      }
      if (schedulingProperties.getRoutingTag() != null) {
        resourcePropsBuilder.setTag(schedulingProperties.getRoutingTag());
      }
      resourceBuilder.setSchedulingProperties(resourcePropsBuilder);
    }
    resourceBuilder.setResourceSchedulingStart(resourceSchedulingDecisionInfo.getSchedulingStartTimeMs());
    resourceBuilder.setResourceSchedulingEnd(resourceSchedulingDecisionInfo.getSchedulingEndTimeMs());
    return resourceBuilder.build();
  }

  // add latest state to the profile builder.
  private void addLatestState(UserBitShared.QueryProfile.Builder builder) {
    builder.setState(queryStateSupplier.get());
    if (stateList != null) {
      builder.clearStateList().addAllStateList(stateList);
    }

    // update times.
    builder.setStart(startTime);
    builder.setEnd(endTime);

    if (cancelReason != null) {
      builder.setCancelReason(cancelReason);
    }
    UserResult.addError(userException, builder);
  }

  private class TimeMarker extends AbstractAttemptObserver {

    @Override
    public void queryStarted(UserRequest query, String user) {
      markStartTime();
    }

    @Override
    public void commandPoolWait(long waitInMillis) {
      addCommandPoolWaitTime(waitInMillis);
    }

    @Override
    public void planStart(String rawPlan) {
      markStartPlanningTime();
    }

    @Override
    public void planCompleted(ExecutionPlan plan) {
      markEndPlanningTime();
    }

    @Override
    public void resourcesScheduled(ResourceSchedulingDecisionInfo info) {
      resourceSchedulingDecisionInfo = info;
    }

    @Override
    public void beginState(AttemptEvent event) {
      stateList.add(event);
    }
  }

  private void markStartPlanningTime() {
    startPlanningTime = System.currentTimeMillis();
  }

  private void markEndPlanningTime() {
    endPlanningTime = System.currentTimeMillis();
  }

  private void markStartTime() {
    startTime = System.currentTimeMillis();
  }

  private void addCommandPoolWaitTime(long waitInMillis) {
    commandPoolWait += waitInMillis;
  }

  void markEndTime() {
    endTime = System.currentTimeMillis();
  }

  long getTime() {
    return Math.max(0, endTime - startTime);
  }
}
