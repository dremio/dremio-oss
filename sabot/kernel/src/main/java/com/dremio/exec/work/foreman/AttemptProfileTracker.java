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

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.common.utils.PathUtils;
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
import com.dremio.exec.record.BatchSchema;
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
import io.grpc.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;

/** Tracker for the profile of a query attempt. */
public class AttemptProfileTracker {
  private static final Logger logger =
      org.slf4j.LoggerFactory.getLogger(AttemptProfileTracker.class);
  private static final ObjectWriter JSON_PRETTY_SERIALIZER =
      new ObjectMapper().writerWithDefaultPrettyPrinter();

  private final UserBitShared.QueryId queryId;
  private final QueryContext queryContext;
  private final String queryDescription;
  private final Supplier<UserBitShared.QueryResult.QueryState> queryStateSupplier;
  private final PlanCaptureAttemptObserver capturer;
  private final AttemptObservers mergedObserver;
  private final JobTelemetryClient jobTelemetryClient;
  private final UserBitShared.QueryProfile.Builder planningProfileBuilder =
      UserBitShared.QueryProfile.newBuilder();

  // the following mutable variables capture ongoing query status
  private long commandPoolWait;
  private long startPlanningTime;
  private long
      endPlanningTime; // NB: tracks the end of both planning and resource scheduling. Name match
  // the profile object, which was kept intact for legacy reasons
  private long startTime;
  private long endTime;
  private long cancelStartTime;
  private long screenOperatorCompletionTime;
  private long screenCompletionRpcReceivedAt;
  private long lastNodeCompletionRpcReceivedAt;
  private long lastNodeCompletionRpcStartedAt;
  private List<AttemptEvent> stateList = new ArrayList<>();

  private volatile UserBitShared.QueryProfile planningProfile;
  private volatile UserBitShared.QueryId prepareId;
  private volatile String cancelReason;
  private volatile UserException userException;
  private volatile ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo;
  private final Context grpcContext;

  protected AttemptProfileTracker(
      UserBitShared.QueryId queryId,
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
    capturer = newCapturerObserver();

    mergedObserver = AttemptObservers.of(observer, capturer, new TimeMarker());
    // separate out grpc context for jts
    this.grpcContext = Context.current().fork();
    cancelStartTime = -1;
  }

  protected AttemptObservers getObserver() {
    return mergedObserver;
  }

  protected QueryContext getQueryContext() {
    return queryContext;
  }

  protected PlanCaptureAttemptObserver newCapturerObserver() {
    final OptionManager optionManager = queryContext.getOptions();
    return new PlanCaptureAttemptObserver(
        optionManager.getOption(PlannerSettings.VERBOSE_PROFILE),
        optionManager.getOption(PlannerSettings.INCLUDE_DATASET_PROFILE),
        queryContext.getFunctionRegistry(),
        queryContext.getAccelerationManager().newPopulator(),
        RelSerializerFactory.getProfileFactory(
            queryContext.getConfig(), queryContext.getScanResult()));
  }

  void setPrepareId(UserBitShared.QueryId prepareId) {
    this.prepareId = prepareId;
  }

  void setCancelReason(String cancelReason) {
    this.cancelReason = cancelReason;
    this.cancelStartTime = System.currentTimeMillis();
  }

  String getCancelReason() {
    return cancelReason;
  }

  // Send planning profile to JTS.
  ListenableFuture<Empty> sendPlanningProfile() {
    try {
      return grpcContext.call(
          () -> {
            return jobTelemetryClient
                .getRetryer()
                .call(
                    () ->
                        jobTelemetryClient
                            .getFutureStub()
                            .putQueryPlanningProfile(
                                PutPlanningProfileRequest.newBuilder()
                                    .setQueryId(queryId)
                                    .setProfile(getPlanningProfile())
                                    .build()));
          });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Send tail profile to JTS (used after query terminates).
  UserBitShared.QueryProfile sendTailProfile(UserException userException) throws Exception {
    this.userException = userException;
    // DX-28440 : when query is cancelled from flight service
    // sometimes the original context might not be valid.
    // fork a new context always.
    UserBitShared.QueryProfile profile = getPlanningProfile();
    grpcContext.run(
        () -> {
          jobTelemetryClient
              .getExponentiaRetryer()
              .call(
                  () ->
                      jobTelemetryClient
                          .getBlockingStub()
                          .putQueryTailProfile(
                              PutTailProfileRequest.newBuilder()
                                  .setQueryId(queryId)
                                  .setProfile(profile)
                                  .build()));
        });
    return profile;
  }

  private UserBitShared.QueryProfile getPlanningProfileFunction(boolean ignoreExceptions)
      throws UserException, com.dremio.common.exceptions.UserCancellationException {
    UserBitShared.QueryProfile.Builder builder = UserBitShared.QueryProfile.newBuilder();

    try {
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
    } catch (com.dremio.common.exceptions.UserCancellationException userCancellationException) {
      if (ignoreExceptions) {
        logger.debug("Ignoring Exceptions", userCancellationException);
      } else {
        throw userCancellationException;
      }
    } finally {
      addLatestState(builder);
      return builder.build();
    }
  }

  // Get the latest planning profile.
  synchronized UserBitShared.QueryProfile getPlanningProfile()
      throws UserException, com.dremio.common.exceptions.UserCancellationException {
    return getPlanningProfileFunction(false);
  }

  // Get the latest planning profile ignoring Exceptions.
  synchronized UserBitShared.QueryProfile getPlanningProfileNoException() {
    return getPlanningProfileFunction(true);
  }

  // fetch full profile (including executor profiles) from JTS.
  UserBitShared.QueryProfile getFullProfile() {
    try {
      return grpcContext.call(
          () ->
              jobTelemetryClient
                  .getExponentiaRetryer()
                  .call(
                      () ->
                          jobTelemetryClient
                              .getBlockingStub()
                              .getQueryProfile(
                                  GetQueryProfileRequest.newBuilder().setQueryId(queryId).build())
                              .getProfile()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Adds planner related details to the profile builder. This method is continuously called until
   * planning is done.
   *
   * @param builder
   * @throws UserException
   * @throws com.dremio.common.exceptions.UserCancellationException
   */
  private void addPlanningDetails(UserBitShared.QueryProfile.Builder builder)
      throws UserException, com.dremio.common.exceptions.UserCancellationException {
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
    if (stateList != null) {
      builder.clearStateList().addAllStateList(stateList);
    }

    try {
      builder.setNonDefaultOptionsJSON(
          JSON_PRETTY_SERIALIZER.writeValueAsString(queryContext.getNonDefaultOptions()));
    } catch (Exception e) {
      logger.warn("Failed to serialize the non-default option list to JSON", e);
      builder.setNonDefaultOptionsJSON(
          "Failed to serialize the non-default " + "option list to JSON: " + e.getMessage());
    }
    builder.setOperatorTypeMetricsMap(OperatorMetricRegistry.getCoreOperatorTypeMetricsMap());

    if (capturer != null) {
      builder.setTotalFragments(capturer.getNumFragments());
      builder.addAllDatasetProfile(capturer.getDatasets());
      builder.setNumPlanCacheUsed(capturer.getNumPlanCacheUses());
      if (capturer.getNumJoinsInUserQuery() != null) {
        builder.setNumJoinsInUserQuery(capturer.getNumJoinsInUserQuery());
      }
      if (capturer.getNumJoinsInFinalPrel() != null) {
        builder.setNumJoinsInFinalPrel(capturer.getNumJoinsInFinalPrel());
      }

      final String planText = capturer.getText();
      if (planText != null) {
        builder.setPlan(planText);
      }

      final String planJson = capturer.getJson();
      if (planJson != null) {
        builder.setJsonPlan(planJson);
      }

      // Scrape Final Physical Plan into a usable API
      final Map<String, UserBitShared.RelNodeInfo> finalPrelInfo = capturer.getFinalPrelInfo();
      if (queryContext.getOptions().getOption(PlannerSettings.PRETTY_PLAN_SCRAPING)) {
        builder.putAllRelInfoMap(finalPrelInfo);
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
      // This method might throw exception, hence moved it to the bottom
      builder.setAccelerationProfile(capturer.buildAccelerationProfile(false));
    }

    // Adds final planning stats for catalog access
    if (endPlanningTime > 0) {
      // Adds stats for individual table access (does not include versioned sources)
      addCatalogStats(builder);
    }
    // Populates source version mapping and marks the ones relevant to the current datasets in the
    // query
    if (endPlanningTime > 0) {
      List<UserBitShared.SourceVersionSetting> sourceVersionSettingList = new ArrayList<>();
      Set<String> currentDatasetSources =
          StreamSupport.stream(capturer.getDatasets().spliterator(), false)
              .map(x -> PathUtils.parseFullPath(x.getDatasetPath()).get(0).toLowerCase())
              .collect(Collectors.toSet());
      queryContext.getSession().getSourceVersionMapping().entrySet().stream()
          .forEach(
              x ->
                  populateGlobalVersionContextMapping(
                      x, sourceVersionSettingList, currentDatasetSources));
      UserBitShared.ContextInfo contextInfo =
          UserBitShared.ContextInfo.newBuilder()
              .setSchemaPathContext(queryContext.getQueryContextInfo().getDefaultSchemaName())
              .addAllSourceVersionSetting(sourceVersionSettingList)
              .build();
      builder.setContextInfo(contextInfo);
    }

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

  protected void addCatalogStats(UserBitShared.QueryProfile.Builder builder) {
    builder.addAllPlanPhases(
        queryContext.getCatalog().getMetadataStatsCollector().getPlanPhaseProfiles());
    builder.addAllPlanPhases(
        queryContext.getCatalog().getCatalogAccessStats().toPlanPhaseProfiles());
  }

  private void populateGlobalVersionContextMapping(
      Map.Entry<String, VersionContext> svMapEntryFromContext,
      List<UserBitShared.SourceVersionSetting> svSettingList,
      Set<String> currentDatasetSources) {
    UserBitShared.SourceVersionSetting svSetting =
        UserBitShared.SourceVersionSetting.getDefaultInstance();
    svSettingList.add(
        svSetting.toBuilder()
            .setSource(svMapEntryFromContext.getKey())
            .setVersionContext(svMapEntryFromContext.getValue().toString())
            .setUsage(
                currentDatasetSources.contains(svMapEntryFromContext.getKey())
                    ? UserBitShared.SourceVersionSetting.Usage.USED_BY_QUERY
                    : UserBitShared.SourceVersionSetting.Usage.NOT_USED_BY_QUERY)
            .build());
  }

  private UserBitShared.ResourceSchedulingProfile getResourceSchedulingProfile() {
    UserBitShared.ResourceSchedulingProfile.Builder resourceBuilder =
        UserBitShared.ResourceSchedulingProfile.newBuilder();
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
    final ResourceSchedulingProperties schedulingProperties =
        resourceSchedulingDecisionInfo.getResourceSchedulingProperties();
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
      if (schedulingProperties.getQueryLabel() != null) {
        resourcePropsBuilder.setQueryLabel(schedulingProperties.getQueryLabel());
      }
      resourceBuilder.setSchedulingProperties(resourcePropsBuilder);
    }
    resourceBuilder.setResourceSchedulingStart(
        resourceSchedulingDecisionInfo.getSchedulingStartTimeMs());
    resourceBuilder.setResourceSchedulingEnd(
        resourceSchedulingDecisionInfo.getSchedulingEndTimeMs());
    if (resourceSchedulingDecisionInfo.getEngineName() != null) {
      resourceBuilder.setEngineName(resourceSchedulingDecisionInfo.getEngineName());
    }
    return resourceBuilder.build();
  }

  // add latest state to the profile builder.
  void addLatestState(UserBitShared.QueryProfile.Builder builder) {
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
    if (cancelStartTime != -1) {
      builder.setCancelStartTime(cancelStartTime);
    }
    UserResult.addError(userException, builder);
  }

  private class TimeMarker extends AbstractAttemptObserver {

    @Override
    public void queryStarted(UserRequest query, String user) {
      super.queryStarted(query, user);
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
    public void planCompleted(ExecutionPlan plan, BatchSchema batchSchema) {
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

  void markStartTime(long startTimeMs) {
    startTime = startTimeMs;
  }

  private void addCommandPoolWaitTime(long waitInMillis) {
    commandPoolWait += waitInMillis;
  }

  void markEndTime(long endTimeMs) {
    endTime = endTimeMs;
  }

  long getTime() {
    return Math.max(0, endTime - startTime);
  }

  public long getEndTime() {
    return endTime;
  }

  public void markExecutionTime(
      long screenOperatorCompletionTime,
      long screenCompletionRpcReceivedAt,
      long lastNodeCompletionRpcReceivedAt,
      long lastNodeCompletionRpcStartedAt) {
    this.screenOperatorCompletionTime = screenOperatorCompletionTime;
    this.screenCompletionRpcReceivedAt = screenCompletionRpcReceivedAt;
    this.lastNodeCompletionRpcReceivedAt = lastNodeCompletionRpcReceivedAt;
    this.lastNodeCompletionRpcStartedAt = lastNodeCompletionRpcStartedAt;
  }

  public long getScreenOperatorCompletionTime() {
    return screenOperatorCompletionTime;
  }

  public long getScreenCompletionRpcReceivedAt() {
    return screenCompletionRpcReceivedAt;
  }

  public long getLastNodeCompletionRpcReceivedAt() {
    return lastNodeCompletionRpcReceivedAt;
  }

  public long getLastNodeCompletionRpcStartedAt() {
    return lastNodeCompletionRpcStartedAt;
  }
}
