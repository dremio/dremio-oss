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
package com.dremio.service.jobs;

import static com.dremio.common.utils.Protos.listNotNull;
import static com.dremio.service.job.proto.JobState.EXECUTION_PLANNING;
import static com.dremio.service.job.proto.JobState.METADATA_RETRIEVAL;
import static com.dremio.service.job.proto.JobState.PENDING;
import static com.dremio.service.job.proto.JobState.PLANNING;
import static com.dremio.service.job.proto.JobState.RUNNING;
import static com.dremio.service.job.proto.JobState.STARTING;
import static com.dremio.service.job.proto.QueryType.UI_INITIAL_PREVIEW;
import static com.dremio.service.jobs.JobIndexKeys.ALL_DATASETS;
import static com.dremio.service.jobs.JobIndexKeys.DATASET;
import static com.dremio.service.jobs.JobIndexKeys.DATASET_VERSION;
import static com.dremio.service.jobs.JobIndexKeys.DURATION;
import static com.dremio.service.jobs.JobIndexKeys.END_TIME;
import static com.dremio.service.jobs.JobIndexKeys.JOBID;
import static com.dremio.service.jobs.JobIndexKeys.JOB_STATE;
import static com.dremio.service.jobs.JobIndexKeys.PARENT_DATASET;
import static com.dremio.service.jobs.JobIndexKeys.QUERY_TYPE;
import static com.dremio.service.jobs.JobIndexKeys.QUEUE_NAME;
import static com.dremio.service.jobs.JobIndexKeys.SPACE;
import static com.dremio.service.jobs.JobIndexKeys.SQL;
import static com.dremio.service.jobs.JobIndexKeys.START_TIME;
import static com.dremio.service.jobs.JobIndexKeys.USER;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.DeferredException;
import com.dremio.common.concurrent.CloseableExecutorService;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.concurrent.ContextMigratingExecutorService.ContextMigratingCloseableExecutorService;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logging.StructuredLogger;
import com.dremio.common.perf.Timer;
import com.dremio.common.perf.Timer.TimedBlock;
import com.dremio.common.util.Closeable;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.common.util.concurrent.DremioFutures;
import com.dremio.common.utils.PathUtils;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.datastore.DatastoreException;
import com.dremio.datastore.IndexedSearchQueryConverterUtil;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.api.LegacyIndexedStoreCreationFunction;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.CachedAccelDetails;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.RootSchemaFinder;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AbstractQueryObserver;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.planner.observer.QueryObserverFactory;
import com.dremio.exec.planner.physical.PlannerSettings.StoreQueryResultsPolicy;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.SearchProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.RpcEndpointInfos;
import com.dremio.exec.proto.UserBitShared.WorkloadClass;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementReq;
import com.dremio.exec.proto.UserProtos.QueryPriority;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.proto.UserProtos.SubmissionSource;
import com.dremio.exec.proto.beans.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.JobResultInfoProvider;
import com.dremio.exec.server.SimpleJobRunner;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.server.options.SessionOptionManagerFactory;
import com.dremio.exec.server.options.SessionOptionManagerFactoryImpl;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.JobResultsStoreConfig;
import com.dremio.exec.store.easy.arrow.ArrowFileFormat;
import com.dremio.exec.store.easy.arrow.ArrowFileMetadata;
import com.dremio.exec.store.easy.arrow.ArrowFileReader;
import com.dremio.exec.store.sys.accel.AccelerationDetailsPopulator;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.protector.ForemenTool;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.rpc.CoordTunnel;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalExecutionConfig;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.exec.work.user.LocalUserUtil;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.reflection.hints.ReflectionExplanationsAndQueryDistance;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserRpcUtils;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Service;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.commandpool.ReleasableCommandPool;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.job.ActiveJobSummary;
import com.dremio.service.job.ActiveJobsRequest;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.CancelReflectionJobRequest;
import com.dremio.service.job.JobCounts;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobStats;
import com.dremio.service.job.JobStatsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsWithParentDatasetRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.ReflectionJobDetailsRequest;
import com.dremio.service.job.ReflectionJobEventsRequest;
import com.dremio.service.job.ReflectionJobProfileRequest;
import com.dremio.service.job.ReflectionJobSummaryRequest;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.SearchReflectionJobsRequest;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.StoreJobResultRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.UniqueUserStats;
import com.dremio.service.job.UniqueUserStatsRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.log.LoggedQuery;
import com.dremio.service.job.proto.Acceleration;
import com.dremio.service.job.proto.ExecutionNode;
import com.dremio.service.job.proto.ExtraInfo;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobCancellationInfo;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.JobSubmission;
import com.dremio.service.job.proto.JoinAnalysis;
import com.dremio.service.job.proto.JoinInfo;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.dremio.service.job.proto.SessionId;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.jobtelemetry.DeleteProfileRequest;
import com.dremio.service.jobtelemetry.GetQueryProfileRequest;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.jobtelemetry.JobTelemetryServiceGrpc;
import com.dremio.service.jobtelemetry.PutTailProfileRequest;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.ScheduleUtils;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.usersessions.UserSessionService;
import com.dremio.telemetry.utils.TracerFacade;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.Empty;
import com.google.protobuf.util.Timestamps;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.trace.Span;
import io.protostuff.ByteString;

/**
 * Submit and monitor jobs from DAC.
 */
public class LocalJobsService implements Service, JobResultInfoProvider, SimpleJobRunner {
  private static final Logger logger = LoggerFactory.getLogger(LocalJobsService.class);
  public static final String QUERY_LOGGER = "query.logger";

  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(LocalJobsService.class);

  private static final int DISABLE_CLEANUP_VALUE = -1;

  private static final int DELAY_BEFORE_STARTING_CLEANUP_IN_MINUTES = 5;

  private static final long ONE_DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1);

  private static final int MAX_NUMBER_JOBS_TO_FETCH = 10;

  public static final String JOBS_NAME = "jobs";

  private static final String LOCAL_TASK_LEADER_NAME = "localjobsclean";

  private static final String LOCAL_ONE_TIME_TASK_LEADER_NAME = "localjobsabandon";

  private static final int SEARCH_JOBS_PAGE_SIZE = 100;

  private static final long LOCAL_ABANDONED_JOBS_TASK_SCHEDULE_MILLIS = 1800000;

  @VisibleForTesting
  public static final String INJECTOR_ATTEMPT_COMPLETION_ERROR = "attempt-completion-error";

  @VisibleForTesting
  public static final String INJECTOR_ATTEMPT_COMPLETION_KV_ERROR = "attempt-completion-kv-error";

  private final Provider<LocalQueryExecutor> queryExecutor;
  private final Provider<LegacyKVStoreProvider> kvStoreProvider;
  private final Provider<JobResultsStoreConfig> jobResultsStoreConfig;
  private final Provider<JobResultsStore> jobResultsStoreProvider;
  private final ConcurrentHashMap<JobId, QueryListener> runningJobs;
  private final BufferAllocator allocator;
  private final Provider<ForemenTool> foremenTool;
  private final Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider;
  private final Provider<Collection<CoordinationProtos.NodeEndpoint>> jobServiceInstances;
  private final Provider<NamespaceService> namespaceServiceProvider;
  private final Provider<OptionManager> optionManagerProvider;
  private final Provider<AccelerationManager> accelerationManagerProvider;
  private final Provider<CoordTunnelCreator> coordTunnelCreator;
  private final Provider<SchedulerService> schedulerService;
  private final Provider<CommandPool> commandPoolService;
  private final Provider<JobTelemetryClient> jobTelemetryClientProvider;
  private final Provider<UserSessionService> userSessionService;
  private final Provider<OptionValidatorListing> optionValidatorProvider;
  private final boolean isMaster;
  private final LocalAbandonedJobsHandler localAbandonedJobsHandler;
  private final StructuredLogger<Job> jobResultLogger;
  private final ContextMigratingCloseableExecutorService executorService;
  private final List<ExternalCleaner> extraExternalCleaners;
  private final CloseableExecutorService queryLoggerExecutorService;

  private NodeEndpoint identity;
  private LegacyIndexedStore<JobId, JobResult> store;
  private NamespaceService namespaceService;
  private String storageName;
  private JobResultsStore jobResultsStore;
  private Cancellable jobResultsCleanupTask;
  private Cancellable jobDependenciesCleanupTask;
  private Cancellable abandonLocalJobsTask;
  private QueryObserverFactory queryObserverFactory;
  private JobTelemetryServiceGrpc.JobTelemetryServiceBlockingStub jobTelemetryServiceStub;
  private SessionOptionManagerFactory sessionOptionManagerFactory;

  private final RemoteJobServiceForwarder forwarder;

  private static final List<SearchFieldSorting> DEFAULT_SORTER = ImmutableList.of(
      JobIndexKeys.START_TIME.toSortField(SearchTypes.SortOrder.DESCENDING),
      JobIndexKeys.END_TIME.toSortField(SearchTypes.SortOrder.DESCENDING),
      JobIndexKeys.JOBID.toSortField(SearchTypes.SortOrder.DESCENDING));

  /**
   * A utility method to create and compose a StructuredLogger with Job
   * @return
   */
  public static StructuredLogger<Job> createJobResultLogger() {
    return StructuredLogger.get(LoggedQuery.class, QUERY_LOGGER)
      .compose(new JobResultToLogEntryConverter());
  }

  public LocalJobsService(
      final Provider<LegacyKVStoreProvider> kvStoreProvider,
      final BufferAllocator allocator,
      final Provider<JobResultsStoreConfig> jobResultsStoreConfig,
      final Provider<JobResultsStore> jobResultsStoreProvider,
      final Provider<LocalQueryExecutor> queryExecutor,
      final Provider<CoordTunnelCreator> coordTunnelCreator,
      final Provider<ForemenTool> foremenTool,
      final Provider<CoordinationProtos.NodeEndpoint> nodeEndpointProvider,
      final Provider<Collection<CoordinationProtos.NodeEndpoint>> jobServiceInstances,
      final Provider<NamespaceService> namespaceServiceProvider,
      final Provider<OptionManager> optionManagerProvider,
      final Provider<AccelerationManager> accelerationManagerProvider,
      final Provider<SchedulerService> schedulerService,
      final Provider<CommandPool> commandPoolService,
      final Provider<JobTelemetryClient> jobTelemetryClientProvider,
      final StructuredLogger<Job> jobResultLogger,
      final boolean isMaster,
      final Provider<ConduitProvider> conduitProvider,
      final Provider<UserSessionService> userSessionService,
      final Provider<OptionValidatorListing> optionValidatorProvider,
      final List<ExternalCleaner> extraExternalCleaners
  ) {
    this.kvStoreProvider = kvStoreProvider;
    this.allocator = allocator;
    this.queryExecutor = checkNotNull(queryExecutor);
    this.jobResultsStoreConfig = checkNotNull(jobResultsStoreConfig);
    this.jobResultsStoreProvider = jobResultsStoreProvider;
    this.jobResultLogger = jobResultLogger;
    this.runningJobs = new ConcurrentHashMap<>();
    this.foremenTool = foremenTool;
    this.nodeEndpointProvider = nodeEndpointProvider;
    this.jobServiceInstances = jobServiceInstances;
    this.namespaceServiceProvider = namespaceServiceProvider;
    this.optionManagerProvider = optionManagerProvider;
    this.accelerationManagerProvider = accelerationManagerProvider;
    this.coordTunnelCreator = coordTunnelCreator;
    this.schedulerService = schedulerService;
    this.commandPoolService = commandPoolService;
    this.jobTelemetryClientProvider = jobTelemetryClientProvider;
    this.isMaster = isMaster;
    this.forwarder = new RemoteJobServiceForwarder(conduitProvider);
    this.localAbandonedJobsHandler = new LocalAbandonedJobsHandler();
    this.userSessionService = userSessionService;
    this.optionValidatorProvider = optionValidatorProvider;
    this.executorService = new ContextMigratingCloseableExecutorService<>(
      new CloseableThreadPool("job-event-collating-observer"), TracerFacade.INSTANCE);
    this.extraExternalCleaners = extraExternalCleaners;
    this.queryLoggerExecutorService = new CloseableThreadPool("async-query-logger");
  }

  public QueryObserverFactory getQueryObserverFactory() {
    return queryObserverFactory;
  }

  @Override
  public void start() throws IOException, InterruptedException {
    logger.info("Starting JobsService");

    this.identity = JobsServiceUtil.toStuff(nodeEndpointProvider.get());
    this.store = kvStoreProvider.get().getStore(JobsStoreCreator.class);
    this.namespaceService = namespaceServiceProvider.get();

    final JobResultsStoreConfig resultsStoreConfig = jobResultsStoreConfig.get();
    this.storageName = resultsStoreConfig.getStorageName();
    this.jobResultsStore = jobResultsStoreProvider.get();
    this.jobTelemetryServiceStub = jobTelemetryClientProvider.get().getBlockingStub();
    this.sessionOptionManagerFactory = new SessionOptionManagerFactoryImpl(optionValidatorProvider.get());

    // if Dremio process died, clean up
    CountDownLatch wasRun = new CountDownLatch(1);
    final Cancellable task = schedulerService.get()
      .schedule(ScheduleUtils.scheduleToRunOnceNow(LOCAL_ONE_TIME_TASK_LEADER_NAME), () -> {
          try {
            setAbandonedJobsToFailedState(store,
            jobServiceInstances.get());
          } finally {
            wasRun.countDown();
          }
        });
    if (!task.isDone()) {
      // wait only if it is task leader
      wasRun.await();
    }

    // register to listen to query lifecycle
    this.queryObserverFactory = new JobsObserverFactory();

    final OptionManager optionManager = optionManagerProvider.get();

    // job results
    final long maxJobResultsAgeInDays = optionManager.getOption(ExecConstants.RESULTS_MAX_AGE_IN_DAYS);

    // job profiles
    final long jobProfilesAgeOffsetInMillis = optionManager.getOption(ExecConstants.DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS);
    final long maxJobProfilesAgeInDays = optionManager.getOption(ExecConstants.JOB_MAX_AGE_IN_DAYS);
    final long maxJobProfilesAgeInMillis = (maxJobProfilesAgeInDays * ONE_DAY_IN_MILLIS) + jobProfilesAgeOffsetInMillis;

    if (isMaster) {
      final long releaseLeadership = optionManager.getOption(ExecConstants.JOBS_RELEASE_LEADERSHIP_MS);
      final Schedule schedule;

      // Schedule job dependencies cleanup to run every day unless the max age is less than a day (used for testing)
      if (maxJobProfilesAgeInDays != DISABLE_CLEANUP_VALUE) {
        if (maxJobProfilesAgeInMillis < ONE_DAY_IN_MILLIS) {
          schedule = Schedule.Builder.everyMillis(maxJobProfilesAgeInMillis)
              .startingAt(Instant.now()
                  .plus(DELAY_BEFORE_STARTING_CLEANUP_IN_MINUTES, ChronoUnit.MINUTES))
            .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
            .releaseOwnershipAfter(releaseLeadership, TimeUnit.MILLISECONDS)
              .build();
        } else {
          final long jobCleanupStartHour = optionManager.getOption(ExecConstants.JOB_CLEANUP_START_HOUR);
          final LocalTime startTime = LocalTime.of((int) jobCleanupStartHour, 0);

          // schedule every day at the user configured hour (defaults to 1 am)
          schedule = Schedule.Builder.everyDays(1, startTime)
            .withTimeZone(ZoneId.systemDefault())
            .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
            .releaseOwnershipAfter(releaseLeadership, TimeUnit.MILLISECONDS)
            .build();
        }
        jobDependenciesCleanupTask = schedulerService.get().schedule(schedule, new JobDependenciesCleanupTask());
      }

      // Schedule job results cleanup
      if (maxJobResultsAgeInDays != DISABLE_CLEANUP_VALUE) {
        final long jobResultsCleanupStartHour = optionManager.getOption(ExecConstants.JOB_RESULTS_CLEANUP_START_HOUR);
        final LocalTime startTime = LocalTime.of((int) jobResultsCleanupStartHour, 0);

        // schedule every day at the user configured hour (defaults to midnight)
        final Schedule resultSchedule = Schedule.Builder.everyDays(1, startTime)
          .withTimeZone(ZoneId.systemDefault())
          .build();

        jobResultsCleanupTask = schedulerService.get().schedule(resultSchedule, new JobResultsCleanupTask());
      }

      // Schedule a recurring abandoned jobs cleanup
      final Schedule abandonedJobsSchedule = Schedule.Builder.everyMinutes(5)
        .withTimeZone(ZoneId.systemDefault())
        .build();
      abandonLocalJobsTask = schedulerService.get()
        .schedule(abandonedJobsSchedule, new AbandonLocalJobsTask());
    }

    // schedule the task every 30 minutes to set abandoned jobs state to FAILED.
    localAbandonedJobsHandler.schedule(LOCAL_ABANDONED_JOBS_TASK_SCHEDULE_MILLIS);

    MapFilterToJobState.init();
    logger.info("JobsService is up");
  }

  @VisibleForTesting
  static void setAbandonedJobsToFailedState(LegacyIndexedStore<JobId, JobResult> jobStore,
                                            Collection<CoordinationProtos.NodeEndpoint> coordinators) {
    final Set<Entry<JobId, JobResult>> apparentlyAbandoned =
      StreamSupport.stream(jobStore.find(new LegacyFindByCondition()
      .setCondition(JobsServiceUtil.getApparentlyAbandonedQuery())).spliterator(), false)
      .collect(Collectors.toSet());
    final Set<CoordinationProtos.NodeEndpoint> coordEndpoints = new HashSet<>(coordinators);

    for (final Entry<JobId, JobResult> entry : apparentlyAbandoned) {
      final JobResult jobResult = entry.getValue();
      final List<JobAttempt> attempts = jobResult.getAttemptsList();
      final int numAttempts = attempts.size();
      if (numAttempts > 0) {
        final JobAttempt lastAttempt = attempts.get(numAttempts - 1);
        // .. check again; the index may not be updated, but the store maybe
        // set to failed only if issuing coordinator is no longer present
        boolean shouldAbandon = JobsServiceUtil.isNonFinalState(lastAttempt.getState())
          && !coordEndpoints.contains(JobsServiceUtil.toPB(lastAttempt.getEndpoint()));

        if (shouldAbandon) {
          logger.debug("Failing abandoned job {}", lastAttempt.getInfo().getJobId().getId());
          final long finishTimestamp = System.currentTimeMillis();
          List<com.dremio.exec.proto.beans.AttemptEvent> attemptEventList = lastAttempt.getStateListList();
          if (attemptEventList == null) {
            attemptEventList = new ArrayList<>();
          }
          attemptEventList.add(JobsServiceUtil.createAttemptEvent(AttemptEvent.State.FAILED, finishTimestamp));
          final JobAttempt newLastAttempt = lastAttempt.setState(JobState.FAILED)
              .setStateListList(attemptEventList)
              .setInfo(lastAttempt.getInfo()
                  .setFinishTime(finishTimestamp)
                  .setFailureInfo("Query failed as Dremio was restarted. Details and profile information " +
                      "for this job may be missing."));
          attempts.remove(numAttempts - 1);
          attempts.add(newLastAttempt);
          jobResult.setCompleted(true); // mark the job as completed
          jobStore.put(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping JobsService");
    if (jobResultsCleanupTask != null) {
      jobResultsCleanupTask.cancel(false);
      jobResultsCleanupTask = null;
    }

    if (jobDependenciesCleanupTask != null) {
      jobDependenciesCleanupTask.cancel(false);
      jobDependenciesCleanupTask = null;
    }

    if (abandonLocalJobsTask != null) {
      abandonLocalJobsTask.cancel(false);
      abandonLocalJobsTask = null;
    }

    AutoCloseables.close(localAbandonedJobsHandler, jobResultsStore, allocator, executorService, queryLoggerExecutorService);
    logger.info("Stopped JobsService");
  }

  void registerListener(JobId jobId, StreamObserver<JobEvent> observer) {
    Job job;
    try {
      final GetJobRequest request = GetJobRequest.newBuilder()
          .setJobId(jobId)
          .setUserName(SYSTEM_USERNAME)
          .build();
      job = getJob(request);

      registerListenerWithJob(job, observer);

    } catch (JobNotFoundException e) {
      throw UserException.validationError()
          .message("Status requested for unknown job %s.", jobId.getId())
          .build(logger);
    }
  }

  void registerListenerWithJob(Job job, StreamObserver<JobEvent> observer)
    throws JobNotFoundException {

    final QueryListener queryListener = runningJobs.get(job.getJobId());
    if (queryListener != null) {
      queryListener.listeners.register(observer, JobsServiceUtil.toJobSummary(job));
    } else {
      //check if its for remote co-ordinator
      if (mustForwardRequest(job)) {
        final NodeEndpoint source = job.getJobAttempt().getEndpoint();
        try {
          forwarder.subscribeToJobEvents(JobsProtoUtil.toBuf(source), JobsProtoUtil.toBuf(job.getJobId()), observer);
        } catch (ExecutionException executionException) {
          JobsRpcUtils.handleException(observer, UserException.systemError(executionException).buildSilently());
        } finally {
          return;
        }
      } // forward the request to remote server

      if (!job.isCompleted()) {
        final GetJobRequest request = GetJobRequest.newBuilder()
          .setJobId(job.getJobId())
          .setUserName(SYSTEM_USERNAME)
          .build();
        job = getJob(request); // not completed implies running, so try again once more

        if (!job.isCompleted()) {
          // there is still a race condition with runningJobs in #execCompletion, so inform the client to retry
          JobsRpcUtils.handleException(observer,
            UserException.systemError(
              new IllegalStateException("Job is in an inconsistent state. Wait, and try again later."))
              .buildSilently());
          return;
        }
      }

      observer.onNext(JobEvent.newBuilder()
        .setFinalJobSummary(JobsServiceUtil.toJobSummary(job))
        .build());
      observer.onCompleted();
    }
  }

  private void startJob(
    ExternalId externalId,
    SubmitJobRequest jobRequest,
    JobEventCollatingObserver eventObserver,
    SessionObserver sessionObserver,
    PlanTransformationListener planTransformationListener,
    String sessionId) {
    // (1) create job details
    final JobId jobId = JobsServiceUtil.getExternalIdAsJobId(externalId);
    final String inSpace = !jobRequest.hasVersionedDataset() &&
        !jobRequest.getVersionedDataset().getPathList().isEmpty() &&
            namespaceService.exists(new NamespaceKey(jobRequest.getVersionedDataset().getPath(0)),
                NameSpaceContainer.Type.SPACE)
            ? jobRequest.getVersionedDataset().getPath(0) : null;

    final JobInfo jobInfo = JobsServiceUtil.createJobInfo(jobRequest, jobId, inSpace);
    final JobAttempt jobAttempt = new JobAttempt()
        .setInfo(jobInfo)
        .setEndpoint(identity)
        .setState(PENDING)
        .setDetails(new JobDetails());
    final Job job = new Job(jobId, jobAttempt, new SessionId().setId(sessionId));

    // (2) deduce execution configuration
    final QueryType queryType = JobsProtoUtil.toStuff(jobRequest.getQueryType());
    final boolean enableLeafLimits = QueryTypeUtils.requiresLeafLimits(queryType);
    final LocalExecutionConfig config =
      LocalExecutionConfig.newBuilder()
        .setEnableLeafLimits(enableLeafLimits)
        .setEnableOutputLimits(QueryTypeUtils.isQueryFromUI(queryType))
        // for UI queries, we should allow reattempts even if data has been returned from query
        .setFailIfNonEmptySent(!QueryTypeUtils.isQueryFromUI(queryType))
        .setUsername(jobRequest.getUsername())
        .setSqlContext(jobRequest.getSqlQuery().getContextList())
        .setInternalSingleThreaded(queryType == UI_INITIAL_PREVIEW || jobRequest.getRunInSameThread())
        .setQueryResultsStorePath(storageName)
        .setAllowPartitionPruning(queryType != QueryType.ACCELERATOR_EXPLAIN)
        .setExposeInternalSources(QueryTypeUtils.isInternal(queryType))
        .setSubstitutionSettings(JobsProtoUtil.toPojo(jobRequest.getMaterializationSettings().getSubstitutionSettings()))
        .setSourceVersionMapping(JobsProtoUtil.toSourceVersionMapping(jobRequest.getSqlQuery().getSourceVersionMappingMap()))
        .setEngineName(jobRequest.getSqlQuery().getEngineName())
        .setSessionId(sessionId) // Should be the same as jobRequest.getSqlQuery().getSessionId() after the first statement
        .setStoreQueryResultsPolicy(jobRequest.getStreamResultsMode() ? StoreQueryResultsPolicy.NO : StoreQueryResultsPolicy.PATH_AND_ATTEMPT_ID)
        .build();

    // (3) register listener
    final QueryListener jobObserver = new QueryListener(job, eventObserver, sessionObserver,
      planTransformationListener, jobRequest.getStreamResultsMode());
    storeJob(job);
    runningJobs.put(jobId, jobObserver);

    final boolean isPrepare = queryType.equals(QueryType.PREPARE_INTERNAL);
    final WorkloadClass workloadClass = QueryTypeUtils.getWorkloadClassFor(queryType);
    final UserBitShared.WorkloadType workloadType = QueryTypeUtils.getWorkloadType(queryType);

    final Object queryRequest;
    if (isPrepare) {
      queryRequest = CreatePreparedStatementReq.newBuilder()
          .setSqlQuery(jobRequest.getSqlQuery().getSql())
          .build();
    } else {
      queryRequest = RunQuery.newBuilder()
          .setType(UserBitShared.QueryType.SQL)
          .setSource(SubmissionSource.LOCAL)
          .setPlan(jobRequest.getSqlQuery().getSql())
          .setPriority(QueryPriority.newBuilder()
              .setWorkloadClass(workloadClass)
              .setWorkloadType(workloadType))
          .build();
    }

    // (4) submit the job
    try {
      UserSession session = null;
      if (optionManagerProvider.get().getOption(UserSession.ENABLE_SESSION_IDS)) {
        session = userSessionService.get().getSession(sessionId).getSession();
        SessionOptionManager sessionOptionManager = sessionOptionManagerFactory.getOrCreate(sessionId);
        session.setSessionOptionManager(sessionOptionManager, optionManagerProvider.get());
      }
      sessionObserver.onSessionModified(session);
      queryExecutor.get()
        .submitLocalQuery(externalId, jobObserver, queryRequest, isPrepare, config, jobRequest.getRunInSameThread(), session);
    } catch (Exception ex) {
      // Failed to submit the job
      jobAttempt.setState(JobState.FAILED);
      jobAttempt.setInfo(jobAttempt.getInfo()
        .setFinishTime(System.currentTimeMillis())
        .setFailureInfo("Failed to submit the job"));
      // Update the job in KVStore
      storeJob(job);
      // Add profile for this job
      final AttemptId attemptId = new AttemptId(JobsServiceUtil.getJobIdAsExternalId(jobId), 0);
      UserException userException = UserException.systemError(ex).build(logger);
      final QueryProfile.Builder profileBuilder = QueryProfile.newBuilder()
        .setQuery(jobRequest.getSqlQuery().getSql())
        .setUser(jobRequest.getUsername())
        .setId(attemptId.toQueryId())
        .setState(QueryState.FAILED)
        .setStart(jobAttempt.getInfo().getStartTime())
        .setEnd(jobAttempt.getInfo().getFinishTime())
        .setCommandPoolWaitMillis(0)
        .setError(userException.getMessage())
        .setVerboseError(userException.getVerboseMessage(false))
        .setErrorId(userException.getErrorId())
        .setDremioVersion(DremioVersionInfo.getVersion());

      try {
        jobTelemetryServiceStub
          .putQueryTailProfile(
            PutTailProfileRequest.newBuilder()
              .setQueryId(attemptId.toQueryId())
              .setProfile(profileBuilder.build())
              .build()
          );
      } catch (Exception telemetryEx) {
        ex.addSuppressed(telemetryEx);
      }

      // Remove the job from running jobs
      runningJobs.remove(jobId);
      throw ex;
    }
  }

  /**
   * Validates JobRequest
   */
  @VisibleForTesting
  static SubmitJobRequest validateJobRequest(SubmitJobRequest submitJobRequest) {

    final SubmitJobRequest.Builder submitJobRequestBuilder = SubmitJobRequest.newBuilder();
    Preconditions.checkArgument(submitJobRequest.hasSqlQuery(),"sql query not provided");

    submitJobRequestBuilder.setSqlQuery(submitJobRequest.getSqlQuery());
    submitJobRequestBuilder.setQueryType(submitJobRequest.getQueryType());
    submitJobRequestBuilder.setRunInSameThread(submitJobRequest.getRunInSameThread());
    submitJobRequestBuilder.setStreamResultsMode(submitJobRequest.getStreamResultsMode());

    if (submitJobRequest.hasDownloadSettings()) {
      Preconditions.checkArgument(submitJobRequest.getQueryType() == com.dremio.service.job.QueryType.UI_EXPORT,
        "download jobs must be of UI_EXPORT type");
      Preconditions.checkArgument(!submitJobRequest.getDownloadSettings().getDownloadId().isEmpty(), "download id not provided");
      Preconditions.checkArgument(!submitJobRequest.getDownloadSettings().getFilename().isEmpty(), "file name not provided");
      submitJobRequestBuilder.setDownloadSettings(submitJobRequest.getDownloadSettings());
    } else if (submitJobRequest.hasMaterializationSettings()) {
      Preconditions.checkArgument(submitJobRequest.getMaterializationSettings().hasMaterializationSummary(), "materialization summary not provided");
      Preconditions.checkArgument(submitJobRequest.getMaterializationSettings().hasSubstitutionSettings(), "substitution settings not provided");
      submitJobRequestBuilder.setMaterializationSettings(submitJobRequest.getMaterializationSettings());
    }

    String username = submitJobRequest.getUsername();
    final String queryUsername = submitJobRequest.getSqlQuery().getUsername();
    if (username.isEmpty() && queryUsername.isEmpty()) {
      throw new IllegalArgumentException("Username not provided");
    } else if (username.isEmpty()) {
      username = queryUsername;
    }
    submitJobRequestBuilder.setUsername(username);

    List<String> datasetPathComponents = ImmutableList.of("UNKNOWN");
    String datasetVersion = "UNKNOWN";
    if (submitJobRequest.hasVersionedDataset()) {
      if (!submitJobRequest.getVersionedDataset().getPathList().isEmpty()) {
        datasetPathComponents = submitJobRequest.getVersionedDataset().getPathList();
      }
      if (!Strings.isNullOrEmpty(submitJobRequest.getVersionedDataset().getVersion())) {
        datasetVersion = submitJobRequest.getVersionedDataset().getVersion();
      }
    }
    return submitJobRequestBuilder.setVersionedDataset(VersionedDatasetPath.newBuilder()
        .addAllPath(datasetPathComponents)
        .setVersion(datasetVersion)
        .build())
      .build();

  }

  @VisibleForTesting
  public JobSubmission submitJob(
      SubmitJobRequest submitJobRequest,
      StreamObserver<JobEvent> eventObserver,
      PlanTransformationListener planTransformationListener
  ) {
    final SubmitJobRequest jobRequest = validateJobRequest(submitJobRequest);
    final ExternalId externalId = ExternalIdHelper.generateExternalId();
    final JobId jobId = JobsServiceUtil.getExternalIdAsJobId(externalId);
    final JobSubmission jobSubmission = new JobSubmission().setJobId(jobId);
    checkNotNull(eventObserver, "an event observer must be provided");
    final JobEventCollatingObserver collatingObserver = new JobEventCollatingObserver(jobId, eventObserver, executorService, submitJobRequest.getStreamResultsMode());
    final String sessionId;
    final SessionObserver sessionObserver;

    if (optionManagerProvider.get().getOption(UserSession.ENABLE_SESSION_IDS)) {
      sessionId = getSessionId(eventObserver, jobRequest);
      sessionObserver = new SessionUpdateObserver(userSessionService.get(), sessionId);
      jobSubmission.setSessionId(new SessionId().setId(sessionId));
    } else {
      sessionId = null;
      sessionObserver = SessionObserver.NO_OP;
    }

    commandPoolService.get().submit(CommandPool.Priority.HIGH,
      ExternalIdHelper.toString(externalId) + ":job-submission",
      (waitInMillis) -> {
        if (!queryExecutor.get().canAcceptWork()) {
          throw UserException.resourceError()
            .message(UserException.QUERY_REJECTED_MSG)
            .buildSilently();
        }

        Span.current().setAttribute("jobId", jobId.getId());
        startJob(externalId, jobRequest, collatingObserver, sessionObserver, planTransformationListener, sessionId);
        logger.debug("Submitted new job. Id: {} Type: {} Sql: {}", jobId.getId(), jobRequest.getQueryType(),
          jobRequest.getSqlQuery());
        if (waitInMillis > CommandPool.WARN_DELAY_MS) {
          logger.warn("Job submission {} waited too long in the command pool: wait was {}ms", jobId.getId(), waitInMillis);
        }
        return null;
      }, jobRequest.getRunInSameThread())
      // once job submission is done, make sure we call either jobSubmitted() or submissionFailed() depending on
      // the outcome
      .whenComplete((o, e) -> {
        if (e == null) {
          collatingObserver.onSubmitted(JobEvent.newBuilder()
              .setJobSubmitted(Empty.newBuilder().build())
              .build());
        } else {
          collatingObserver.onError(
              UserException.systemError(e)
                  .message("Failed to submit job %s", jobId.getId())
                  .buildSilently());
        }
      });

    // begin sending events to the client, and start with jobId + sessionId
    collatingObserver.start(JobEvent.newBuilder()
        .setJobSubmission(JobsProtoUtil.toBuf(jobSubmission))
        .build());
    return jobSubmission;
  }

  private String getSessionId(StreamObserver<JobEvent> eventObserver, SubmitJobRequest jobRequest) {
    String sessionId = jobRequest.getSqlQuery().getSessionId();
    if (StringUtil.isEmpty(sessionId)) {
      // Create a new session
      final QueryType queryType = JobsProtoUtil.toStuff(jobRequest.getQueryType());
      UserSession session = UserSession.Builder.newBuilder()
        .withSessionOptionManager(new SessionOptionManagerImpl(
            optionManagerProvider.get().getOptionValidatorListing()),
          optionManagerProvider.get())
        .setSupportComplexTypes(true)
        .withCredentials(UserBitShared.UserCredentials
          .newBuilder()
          .setUserName(jobRequest.getUsername())
          .build())
        .withInitialQuoting(Quoting.DOUBLE_QUOTE) // Default quoting is double quotes.
        .exposeInternalSources(QueryTypeUtils.isInternal(queryType))
        .withDefaultSchema(jobRequest.getSqlQuery().getContextList())
        .withSubstitutionSettings(JobsProtoUtil.toPojo(jobRequest.getMaterializationSettings().getSubstitutionSettings()))
        .withClientInfos(UserRpcUtils.getRpcEndpointInfos("Dremio Java local client"))
        .withEngineName(jobRequest.getSqlQuery().getEngineName())
        .build();

      session.setLastQueryId(UserBitShared.QueryId.getDefaultInstance());
      sessionId = userSessionService.get().putSession(session).getId();
    } else {
      // Check if the session is still active
      if (userSessionService.get().getSession(sessionId) == null) {
        // Session expired/not found.
        JobsRpcUtils.handleException(eventObserver,
          UserException.systemError(new SessionNotFoundException(sessionId))
            .buildSilently());
      }
    }
    return sessionId;
  }

  void submitJob(SubmitJobRequest jobRequest, StreamObserver<JobEvent> eventObserver) {
    submitJob(jobRequest, eventObserver, PlanTransformationListener.NO_OP);
  }

  JobSubmissionHelper getJobSubmissionHelper(SubmitJobRequest jobRequest, StreamObserver<JobEvent> eventObserver, PlanTransformationListener planTransformationListener) {
    CommandPool commandPool = commandPoolService.get();

    if (commandPool instanceof ReleasableCommandPool) {
      ReleasableCommandPool releasableCommandPool = (ReleasableCommandPool) commandPool;
      // Protecting this code from callers who do not hold the command pool slot
      // check if the caller holds the command pool slot before releasing it
      if (releasableCommandPool.amHoldingSlot()) {
        SubmitJobRequest newJobRequest = SubmitJobRequest.newBuilder(jobRequest)
          .setRunInSameThread(false)
          .build();

        logger.debug("The SQL query {} will be submitted to the releasable command pool", jobRequest.getSqlQuery().getSql());
        return new SubmitJobToReleasableCommandPool(newJobRequest, eventObserver, planTransformationListener, releasableCommandPool);
      }
    }

    logger.info("The SQL query {} will be submitted on the same thread", jobRequest.getSqlQuery().getSql());
    return new JobSubmissionHelper(jobRequest, eventObserver, planTransformationListener);
  }

  @Override
  public void runQueryAsJob(String query, String userName, String queryType) throws Exception {
    final SubmitJobRequest jobRequest = SubmitJobRequest.newBuilder()
      .setQueryType(com.dremio.service.job.QueryType.valueOf(queryType))
      .setSqlQuery(SqlQuery.newBuilder().setSql(query))
      .setUsername(userName)
      .setRunInSameThread(true)
      .build();

    final CompletionListener completionListener = new CompletionListener(false);
    final JobStatusListenerAdapter streamObserver = new JobStatusListenerAdapter(completionListener);
    final JobSubmissionHelper jobSubmissionHelper = getJobSubmissionHelper(jobRequest, streamObserver, PlanTransformationListener.NO_OP);

    // release the slot in the releasable command pool; submit the job and wait before re-acquiring
    try (Closeable closeable = jobSubmissionHelper.releaseAndReacquireCommandPool()) {
      // submit the job to the command pool
      JobSubmission jobSubmission = jobSubmissionHelper.submitJobToCommandPool();
      JobId submittedJobId = jobSubmission.getJobId();

      logger.info("New job submitted. Job Id: {} - Type: {} - Query: {}", submittedJobId, jobRequest.getQueryType(), jobRequest.getSqlQuery().getSql());
      // Renames the current thread to indicate the JobId of the new triggered job
      final String originalThreadName = Thread.currentThread().getName();
      Thread.currentThread().setName(originalThreadName + ":" + submittedJobId);

      try {
        completionListener.await();
        if (completionListener.getException() != null) {
          logger.info("Submitted job (JobID {}) has failed", submittedJobId);
          throw new IllegalStateException(completionListener.getException());
        }
        if (!completionListener.isCompleted()) {
          logger.info("Submitted job (JobID {}) was cancelled", submittedJobId);
          throw new IllegalStateException(String.format("Submitted job (JobID %s) was cancelled. %s", submittedJobId, completionListener.getCancelledReason()));
        } else {
          logger.info("Submitted job (JobID {}) has completed successfully", submittedJobId);
        }
      } catch (Exception e) {
        logger.info("Submitted job (JobID {}) has failed", submittedJobId);
        throw e;
      } finally {
        // Reverts the thread renaming once the submitted job is completed (passed or failed).
        Thread.currentThread().setName(originalThreadName);
      }
    }
  }

  Job getJob(GetJobRequest request) throws JobNotFoundException {
    JobId jobId = request.getJobId();
    if (!request.isFromStore()) {
      QueryListener listener = runningJobs.get(jobId);
      if (listener != null) {
        return listener.getJob();
      }
    }

    return getJobFromStore(jobId);
  }

  Job getJobFromStore(final JobId jobId) throws JobNotFoundException {
    final JobResult jobResult = store.get(jobId);
    if (jobResult == null) {
      throw new JobNotFoundException(jobId);
    }

    SessionId sessionId = jobResult.getSessionId();
    Job job = new Job(jobId, jobResult, jobResultsStore, sessionId);
    populateJobDetailsFromFullProfile(job);
    return job;
  }

  // The job page in the UI shows a lot of stats collected from the full profile.
  // These are populated in the job info on the first such access from the UI.
  private void populateJobDetailsFromFullProfile(Job job) {
    try {
      JobAttempt attempt = job.getJobAttempt();
      if (!QueryTypeUtils.isAccelerationType(attempt.getInfo().getQueryType())
        && ifJobAttemptHasRunningState(attempt)
        && (attempt.getStats() == null || attempt.getStats().getOutputBytes() == null)) {
        logger.debug("Populating job details from full profile {}", job.getJobId());
        QueryProfile profile = getProfileFromJob(job, job.getAttempts().size() - 1);

        if (profile != null) {
          updateJobDetails(job, profile);

          // store only if job is in terminal state
          if (JobsServiceUtil.finalJobStates.contains(attempt.getState())) {
            logger.debug("Updating job details to store in populateJobDetailsFromFullProfile");
            storeJob(job);
          }
        }
      }
    } catch (Exception e) {
      //StatusRuntimeException from profile or NPE from Job attempt fields is possible
      logger.warn("Exception while populating job details from full profile: ", e);
    }
  }

  private boolean ifJobAttemptHasRunningState(JobAttempt attempt) {
    for (com.dremio.exec.proto.beans.AttemptEvent event : attempt.getStateListList()) {
        if (event.getState() == com.dremio.exec.proto.beans.AttemptEvent.State.RUNNING) {
          return true;
        }
    }
    return false;
  }

  JobSummary getJobSummary(JobSummaryRequest jobSummaryRequest)
    throws JobNotFoundException, ExecutionException {
    final GetJobRequest getJobRequest = GetJobRequest.newBuilder()
      .setJobId(JobsProtoUtil.toStuff(jobSummaryRequest.getJobId()))
      .setFromStore(jobSummaryRequest.getFromStore())
      .setUserName(jobSummaryRequest.getUserName())
      .build();

    Job job = null;
    JobSummary summary = null;
    if (getJobRequest.isFromStore()) {
      job = getJobFromStore(getJobRequest.getJobId());
      summary = JobsServiceUtil.toJobSummary(job);
    } else {
      job = getJob(getJobRequest);
      if (mustForwardRequest(job)) {
        logger.debug("Forwarding JobSummary request for jobId {} to target {}",
          getJobRequest.getJobId().getId(), job.getJobAttempt().getEndpoint());
        summary = forwarder.getJobSummary(JobsProtoUtil.toBuf(job.getJobAttempt().getEndpoint()), jobSummaryRequest);
      } else {
        summary = JobsServiceUtil.toJobSummary(job);
      }
    }

    return summary;
  }

  com.dremio.service.job.JobDetails getJobDetails(JobDetailsRequest jobDetailsRequest)
    throws JobNotFoundException, ExecutionException {
    final GetJobRequest getJobRequest = GetJobRequest.newBuilder()
      .setJobId(JobsProtoUtil.toStuff(jobDetailsRequest.getJobId()))
      .setUserName(jobDetailsRequest.getUserName())
      .setFromStore(jobDetailsRequest.getFromStore())
      .build();

    Job job = null;
    com.dremio.service.job.JobDetails details = null;

    if (getJobRequest.isFromStore()) {
      job = getJobFromStore(getJobRequest.getJobId());
      details =  JobsServiceUtil.toJobDetails(job, jobDetailsRequest.getProvideResultInfo());
    } else {
      job = getJob(getJobRequest);
      if (mustForwardRequest(job)) {
        logger.debug("Forwarding JobDetails request for jobId {} to target {}",
          getJobRequest.getJobId().getId(), job.getJobAttempt().getEndpoint());
        details = this.forwarder.getJobDetails(JobsProtoUtil.toBuf(job.getJobAttempt().getEndpoint()), jobDetailsRequest);
      } else {
        details = JobsServiceUtil.toJobDetails(job, jobDetailsRequest.getProvideResultInfo());
      }
    }

    return details;
  }

  JobCounts getJobCounts(JobCountsRequest request) {
    final SearchQuery[] conditions = request.getDatasetsList()
        .stream()
        .map(LocalJobsService::getFilter)
        .toArray(SearchQuery[]::new);

    JobCounts.Builder jobCounts = JobCounts.newBuilder();
    jobCounts.addAllCount(store.getCounts(conditions));
    return jobCounts.build();
  }

  private static SearchQuery getFilter(VersionedDatasetPath datasetPath) {
    final NamespaceKey namespaceKey = new NamespaceKey(datasetPath.getPathList());
    Preconditions.checkNotNull(namespaceKey);

    final ImmutableList.Builder<SearchQuery> builder = ImmutableList.<SearchQuery>builder()
        .add(SearchQueryUtils.newTermQuery(JobIndexKeys.ALL_DATASETS, namespaceKey.toString()))
        .add(JobIndexKeys.UI_EXTERNAL_JOBS_FILTER);

    if (!Strings.isNullOrEmpty(datasetPath.getVersion())) {
      final DatasetVersion datasetVersion =  new DatasetVersion(datasetPath.getVersion());
      builder.add(SearchQueryUtils.newTermQuery(JobIndexKeys.DATASET_VERSION, datasetVersion.getVersion()));
    }

    return SearchQueryUtils.and(builder.build());
  }

  @VisibleForTesting
  public JobDataFragment getJobData(JobId jobId, int offset, int limit)
    throws JobNotFoundException {
    GetJobRequest request = GetJobRequest.newBuilder()
      .setJobId(jobId)
      .setUserName(SYSTEM_USERNAME)
      .build();
    return getJob(request).getData().range(offset, limit);
  }

  @VisibleForTesting
  JobResultsStore getJobResultsStore() {
    return jobResultsStore;
  }

  private static final ImmutableList<SimpleEntry<JobStats.Type, SearchQuery>> JOBS_STATS_TYPE_TO_SEARCH_QUERY_MAPPING =
      ImmutableList.of(
          new SimpleEntry<>(JobStats.Type.UI, JobIndexKeys.UI_JOBS_FILTER),
          new SimpleEntry<>(JobStats.Type.EXTERNAL, JobIndexKeys.EXTERNAL_JOBS_FILTER),
          new SimpleEntry<>(JobStats.Type.ACCELERATION, JobIndexKeys.ACCELERATION_JOBS_FILTER),
          new SimpleEntry<>(JobStats.Type.DOWNLOAD, JobIndexKeys.DOWNLOAD_JOBS_FILTER),
          new SimpleEntry<>(JobStats.Type.INTERNAL, JobIndexKeys.INTERNAL_JOBS_FILTER),
          new SimpleEntry<>(JobStats.Type.DAILY_JOBS, JobIndexKeys.DAILY_JOBS_FILTER),
          new SimpleEntry<>(JobStats.Type.USER_JOBS, JobIndexKeys.USER_JOBS_FILTER)
      );

  JobStats getJobStats(JobStatsRequest request) {
    final long startDate = Timestamps.toMillis(request.getStartDate());
    final long endDate = Timestamps.toMillis(request.getEndDate());
    final List<JobStats.Type> typeList = request.getJobStatsTypeList();

    final JobStats.Builder jobStats = JobStats.newBuilder();
    for (JobStats.Type jobStatsType : typeList) {
      jobStats.addCounts(JobStats.JobCountWithType.newBuilder()
        .setType(JOBS_STATS_TYPE_TO_SEARCH_QUERY_MAPPING.get(jobStatsType.getNumber()).getKey())
        .setCount(store.getCounts(SearchQueryUtils.and(
          SearchQueryUtils.newRangeLong(JobIndexKeys.START_TIME.getIndexFieldName(), startDate, endDate, true, true),
          JOBS_STATS_TYPE_TO_SEARCH_QUERY_MAPPING.get(jobStatsType.getNumber()).getValue())).get(0)));
    }
    return jobStats.build();
  }

  private static final String FILTER = "(st=gt=%d;st=lt=%d)";

  UniqueUserStats getUniqueUserStats(UniqueUserStatsRequest request) {
    final long startDate = Timestamps.toMillis(request.getStartDate());
    final long endDate = Timestamps.toMillis(request.getEndDate());

    final String filter = String.format(FILTER, startDate, endDate);
    final SearchJobsRequest searchJobsRequest = SearchJobsRequest.newBuilder()
      .setFilterString(filter)
      .build();

    LegacyFindByCondition condition = createCondition(searchJobsRequest);

    Stopwatch sw = Stopwatch.createStarted();

    Iterable<Entry<JobId, JobResult>> iterable = store.find(condition);
    long uniqueUsers = StreamSupport.stream(iterable.spliterator(), true)
      .parallel()
      .map(x -> x.getValue().getAttemptsList().get(0).getInfo().getUser())
      .distinct()
      .count();

    final long timeConsumed = sw.elapsed(java.util.concurrent.TimeUnit.SECONDS);
    if (timeConsumed > 59) {
      logger.info("UniqueUserStats time taken in sec: " + timeConsumed);
    } else {
      logger.debug("UniqueUserStats time taken in sec: " + timeConsumed);
    }

    return UniqueUserStats.newBuilder().setUniqueUsers((int)uniqueUsers).build();
  }

  private static SearchQuery getDatasetFilter(String datasetPath, String version, String userName) {
    final ImmutableList.Builder<SearchTypes.SearchQuery> builder =
      ImmutableList.<SearchTypes.SearchQuery>builder()
        .add(SearchQueryUtils.newTermQuery(JobIndexKeys.ALL_DATASETS, datasetPath))
        .add(JobIndexKeys.UI_EXTERNAL_JOBS_FILTER);

    if (!Strings.isNullOrEmpty(version)) {
      DatasetVersion datasetVersion = new DatasetVersion(version);
      builder.add(SearchQueryUtils.newTermQuery(DATASET_VERSION, datasetVersion.getVersion()));
    }

    // TODO(DX-17909): this must be provided for authorization purposes
    if (!Strings.isNullOrEmpty(userName)) {
      builder.add(SearchQueryUtils.newTermQuery(USER, userName));
    }

    return SearchQueryUtils.and(builder.build());
  }

  private static List<SearchFieldSorting> buildSorter(final String sortKey, final SearchTypes.SortOrder order) {
    if (!Strings.isNullOrEmpty(sortKey)) {
      final IndexKey key = JobIndexKeys.MAPPING.getKey(sortKey);
      if(key == null || !key.isSorted()){
        throw UserException.functionError()
          .message("Unable to sort by field {}.", sortKey)
          .buildSilently();
      }
      return ImmutableList.of(key.toSortField(order));
    }

    return DEFAULT_SORTER;
  }

  private static class MapFilterToJobState {
    private static String setupFilter = "";
    private static String runningFilter = "";

    private static void init() {
      Map<String, List<JobState>> filterToJobStateMap = new HashMap<>();
      filterToJobStateMap.put("SETUP", Arrays.asList(PENDING, METADATA_RETRIEVAL, PLANNING));
      filterToJobStateMap.put("RUNNING", Arrays.asList(EXECUTION_PLANNING, STARTING, RUNNING));
      filterToJobStateMap = Collections.unmodifiableMap(filterToJobStateMap);

      filterToJobStateMap.get("SETUP").forEach((v) -> setupFilter += "jst==" + v.name() + ",");
      setupFilter = setupFilter.substring(0, setupFilter.length()-1);

      filterToJobStateMap.get("RUNNING").forEach((v) -> runningFilter += "jst==" + v.name() + ",");
      runningFilter = runningFilter.substring(0, runningFilter.length()-1);
    }

    private static String map(String filterString) {
      filterString = filterString.replace("jst==\"SETUP\"", setupFilter);
      return filterString.replace("jst==\"RUNNING\"", runningFilter);
    }
  }


   LegacyFindByCondition createCondition(SearchJobsRequest searchJobsRequest) {

    final LegacyFindByCondition condition = new LegacyFindByCondition();
    VersionedDatasetPath versionedDatasetPath = searchJobsRequest.getDataset();

    if (!versionedDatasetPath.getPathList().isEmpty()) {
      final NamespaceKey namespaceKey = new NamespaceKey(versionedDatasetPath.getPathList());
      condition.setCondition(getDatasetFilter(namespaceKey.toString(), versionedDatasetPath.getVersion(), searchJobsRequest.getUserName()));
    } else {
      condition.setCondition(MapFilterToJobState.map(searchJobsRequest.getFilterString()), JobIndexKeys.MAPPING);
    }

    final int offset = searchJobsRequest.getOffset();
    if (offset > 0) {
      condition.setOffset(offset);
    }

    final int limit = searchJobsRequest.getLimit();
    if (limit > 0) {
      condition.setLimit(limit);
    }

    condition.setPageSize(SEARCH_JOBS_PAGE_SIZE);

    final String sortColumn = searchJobsRequest.getSortColumn();
    if (!Strings.isNullOrEmpty(sortColumn)) {
      condition.addSortings(buildSorter(sortColumn, JobsProtoUtil.toStoreSortOrder(searchJobsRequest.getSortOrder())));
    }

    return condition;
  }

  Iterable<JobSummary> searchJobs(LegacyFindByCondition condition) {
    final Iterable<Job> jobs =  toJobs(store.find(condition));
    return FluentIterable.from(jobs)
      .filter(job -> job.getJobAttempt() != null)
      .transform(job -> JobsServiceUtil.toJobSummary(job));
  }

  Iterable<JobSummary> searchJobs(SearchJobsRequest request) {
    LegacyFindByCondition condition = createCondition(request);
    return searchJobs(condition);
  }

  LegacyFindByCondition createActiveJobCondition(ActiveJobsRequest activeJobsRequest) {
    String filterString = "(jst==RUNNING,jst==QUEUED,jst==ENQUEUED,jst==PLANNING,jst==STARTING,jst==PENDING,jst==METADATA_RETRIEVAL,jst==ENGINE_START,jst==EXECUTION_PLANNING)";
    if (!activeJobsRequest.getUserName().equals("")) {
      filterString = filterString + ";usr==" + activeJobsRequest.getUserName();
    }
    final LegacyFindByCondition condition = new LegacyFindByCondition();
    condition.setCondition(filterString, JobIndexKeys.MAPPING);
    condition.setPageSize(SEARCH_JOBS_PAGE_SIZE);

    SearchProtos.SearchQuery query = activeJobsRequest.getQuery();
    if(query != null && query.getQueryCase() != SearchProtos.SearchQuery.QueryCase.QUERY_NOT_SET){
      SearchQuery filterQuery = condition.getCondition();
      SearchQuery pushDownQuery = IndexedSearchQueryConverterUtil.toSearchQuery(query, ActiveJobSearchUtils.FIELDS);
      SearchQuery finalQuery;
      if(pushDownQuery != null) {
        finalQuery = SearchQueryUtils.and(filterQuery, pushDownQuery);
      } else{
        finalQuery = filterQuery;
      }
      condition.setCondition(finalQuery);
    }

    return condition;
  }

  Iterable<ActiveJobSummary> getActiveJobs(LegacyFindByCondition condition) {
    final Iterable<Job> jobs =  toJobs(store.find(condition));
    return FluentIterable.from(jobs)
      .filter(job -> job.getJobAttempt() != null && !isTerminal(JobsServiceUtil.jobStatusToAttemptStatus(job.getJobAttempt().getState())))
      .transform(job -> {
        try {
          return JobsServiceUtil.toActiveJobSummary(job);
        } catch (Exception e) {
          logger.error("Exception while constructing ActiveJobSummary for job {}", job.getJobId(), e);
          return null;
        }
      });
  }

  Iterable<ActiveJobSummary> getActiveJobs(ActiveJobsRequest request) {
    LegacyFindByCondition condition = createActiveJobCondition(request);
    return getActiveJobs(condition);
  }

  Iterable<com.dremio.service.job.JobDetails> getJobsForParent(JobsWithParentDatasetRequest jobsWithParentDatasetRequest) {
    final VersionedDatasetPath versionedDatasetPath = jobsWithParentDatasetRequest.getDataset();
    if (!versionedDatasetPath.getPathList().isEmpty()) {
      final NamespaceKey namespaceKey = new NamespaceKey(versionedDatasetPath.getPathList());

      final SearchQuery query = SearchQueryUtils.and(
        SearchQueryUtils.newTermQuery(PARENT_DATASET, namespaceKey.getSchemaPath()),
        JobIndexKeys.UI_EXTERNAL_RUN_JOBS_FILTER);

      final LegacyFindByCondition condition = new LegacyFindByCondition()
        .setCondition(query)
        .setLimit(jobsWithParentDatasetRequest.getLimit());

      final Iterable<Job> jobs = toJobs(store.find(condition));
      return FluentIterable.from(jobs)
        .transform(job -> JobsServiceUtil.toJobDetails(job, false));
    }
    return null;
  }

  private Iterable<Job> toJobs(final Iterable<Entry<JobId, JobResult>> entries) {
    return Iterables.transform(entries, new Function<Entry<JobId, JobResult>, Job>() {
      @Override
      public Job apply(Entry<JobId, JobResult> input) {
        return new Job(input.getKey(), input.getValue(), jobResultsStore, input.getValue().getSessionId());
      }
    });
  }

  @Override
  public java.util.Optional<JobResultInfo> getJobResultInfo(String jobId, String username) {
    try {
      final Job job = getJob(GetJobRequest.newBuilder()
          .setJobId(new JobId(jobId))
          .setUserName(username)
          .build());
      if (job.isCompleted()) {
        final BatchSchema batchSchema = BatchSchema.deserialize(job.getJobAttempt().getInfo().getBatchSchema());
        final List<String> tableName = jobResultsStore.getOutputTablePath(job.getJobId());
        return java.util.Optional.of(new JobResultInfo(tableName, batchSchema));
      } // else, fall through
    } catch (JobNotFoundException ignored) {
      // fall through
    }
    return java.util.Optional.empty();
  }

  private static class JobConverter implements DocumentConverter<JobId, JobResult> {
    private Integer version = 0;

    @Override
    public Integer getVersion() {
      return version;
    }

    @Override
    public void convert(DocumentWriter writer, JobId key, JobResult job) {
      final Set<NamespaceKey> allDatasets = new HashSet<>();
      // we only care about the last attempt
      final int numAttempts = job.getAttemptsList().size();
      final JobAttempt jobAttempt = job.getAttemptsList().get(numAttempts - 1);


      final JobInfo jobInfo = jobAttempt.getInfo();
      final NamespaceKey datasetPath = new NamespaceKey(jobInfo.getDatasetPathList());
      allDatasets.add(datasetPath);

      writer.write(JOBID, key.getId());
      writer.write(USER, jobInfo.getUser());
      writer.write(SPACE, jobInfo.getSpace());
      writer.write(SQL, jobInfo.getSql());
      writer.write(QUERY_TYPE, jobInfo.getQueryType().name());
      writer.write(JOB_STATE, jobAttempt.getState().name());
      writer.write(DATASET, datasetPath.getSchemaPath());
      writer.write(DATASET_VERSION, jobInfo.getDatasetVersion());
      writer.write(START_TIME, jobInfo.getStartTime());
      writer.write(END_TIME, jobInfo.getFinishTime());
      if (jobInfo.getResourceSchedulingInfo() != null && jobInfo.getResourceSchedulingInfo().getQueueName() != null) {
        writer.write(QUEUE_NAME, jobInfo.getResourceSchedulingInfo().getQueueName());
      }

      final Long duration = jobInfo.getStartTime() == null || jobInfo.getFinishTime() == null ? null :
          jobInfo.getFinishTime() - jobInfo.getStartTime();
      writer.write(DURATION, duration);

      Set<NamespaceKey> parentDatasets = new HashSet<>();
      List<ParentDatasetInfo> parentsList = jobInfo.getParentsList();
      if (parentsList != null) {
        for (ParentDatasetInfo parentDatasetInfo : parentsList) {
          List<String> pathList = listNotNull(parentDatasetInfo.getDatasetPathList());
          final NamespaceKey parentDatasetPath = new NamespaceKey(pathList);
          parentDatasets.add(parentDatasetPath);
          allDatasets.add(parentDatasetPath);
        }
      }
      List<FieldOrigin> fieldOrigins = jobInfo.getFieldOriginsList();
      if (fieldOrigins != null) {
        for (FieldOrigin fieldOrigin : fieldOrigins) {
          for (Origin origin : listNotNull(fieldOrigin.getOriginsList())) {
            List<String> tableList = listNotNull(origin.getTableList());
            if (!tableList.isEmpty()) {
              final NamespaceKey parentDatasetPath = new NamespaceKey(tableList);
              parentDatasets.add(parentDatasetPath);
              allDatasets.add(parentDatasetPath);
            }
          }
        }
      }

      for (NamespaceKey parentDatasetPath : parentDatasets) {
        // DX-5119 Index unquoted dataset names along with quoted ones.
        // TODO (Amit H): DX-1563 We should be using analyzer to match both rather than indexing twice.
        writer.write(
            PARENT_DATASET,
            parentDatasetPath.getSchemaPath(),
            Joiner.on(PathUtils.getPathDelimiter()).join(parentDatasetPath.getPathComponents()));
      }

      // index all datasets accessed by this job
      for (ParentDataset parentDataset : listNotNull(jobInfo.getGrandParentsList())) {
        allDatasets.add(new NamespaceKey(parentDataset.getDatasetPathList()));
      }
      for (NamespaceKey allDatasetPath : allDatasets) {
        // DX-5119 Index unquoted dataset names along with quoted ones.
        // TODO (Amit H): DX-1563 We should be using analyzer to match both rather than indexing twice.
        writer.write(ALL_DATASETS,
            allDatasetPath.getSchemaPath(),
            Joiner.on(PathUtils.getPathDelimiter()).join(allDatasetPath.getPathComponents()));
      }
    }
  }

  private final class JobsObserverFactory implements QueryObserverFactory {

    @Override
    public QueryObserver createNewQueryObserver(ExternalId id, UserSession session, UserResponseHandler handler) {
      final JobId jobId = JobsServiceUtil.getExternalIdAsJobId(id);

      final RpcEndpointInfos clientInfos = session.getClientInfos();
      final QueryType queryType = QueryTypeUtils.getQueryType(clientInfos);

      final JobInfo jobInfo =  new JobInfo(jobId, "UNKNOWN", "UNKNOWN", queryType)
            .setUser(session.getCredentials().getUserName())
            .setDatasetPathList(Arrays.asList("UNKNOWN"))
            .setStartTime(System.currentTimeMillis());
        final JobAttempt jobAttempt = new JobAttempt()
            .setInfo(jobInfo)
            .setEndpoint(identity)
            .setState(PENDING)
            .setDetails(new JobDetails());

      final Job job = new Job(jobId, jobAttempt, null);

      QueryListener listener = new QueryListener(job, handler, session.getSessionOptionManager());
      runningJobs.put(jobId, listener);
      storeJob(job);

      return listener;
    }

  }

  private final class QueryListener extends AbstractQueryObserver {

    private final Job job;
    private final ExternalId externalId;
    private final UserResponseHandler responseHandler;
    private final JobEventCollatingObserver eventObserver;
    private final SessionObserver sessionObserver;
    private final PlanTransformationListener planTransformationListener;
    private final boolean isInternal;
    private final boolean streamResultsMode;
    private final ExternalListenerManager listeners = new ExternalListenerManager();
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private final DeferredException exception = new DeferredException();

    private JobResultListener attemptObserver;
    private SessionOptionManager sessionOptionManager = null;
    private ExecutionControls executionControls;


    private QueryListener(Job job, UserResponseHandler connection, SessionOptionManager sessionOptionManager) {
      this.job = job;
      this.externalId = JobsServiceUtil.getJobIdAsExternalId(job.getJobId());
      this.responseHandler = Preconditions.checkNotNull(connection, "handler cannot be null");
      this.eventObserver = null;
      this.sessionObserver = null;
      this.planTransformationListener = null;
      this.isInternal = false;
      this.job.setIsInternal(false);
      this.streamResultsMode = true;
      setupJobData();
      this.sessionOptionManager = sessionOptionManager;
    }

    private QueryListener(
        Job job,
        JobEventCollatingObserver eventObserver,
        SessionObserver sessionObserver,
        PlanTransformationListener planTransformationListener,
        boolean streamResultsMode
    ) {
      this.job = job;
      this.externalId = JobsServiceUtil.getJobIdAsExternalId(job.getJobId());
      this.responseHandler = null;
      this.eventObserver = Preconditions.checkNotNull(eventObserver, "eventObserver cannot be null");
      this.sessionObserver = Preconditions.checkNotNull(sessionObserver, "sessionObserver cannot be null");
      this.planTransformationListener = Preconditions.checkNotNull(planTransformationListener, "statusListener cannot be null");
      this.isInternal = true;
      this.job.setIsInternal(true);
      this.streamResultsMode = streamResultsMode;
      setupJobData();
    }

    private Job getJob(){
      return job;
    }

    private void setupJobData() {
      final JobLoader jobLoader = (isInternal && !streamResultsMode) ?
          new InternalJobLoader(exception, completionLatch, job.getJobId(), jobResultsStore, store) :
          new ExternalJobLoader(completionLatch, exception);
      JobData jobData = new JobDataImpl(jobLoader, job.getJobId(), job.getSessionId());
      job.setData(jobData);
    }

    @Override
    public AttemptObserver newAttempt(AttemptId attemptId, AttemptReason reason) {
      // first attempt is already part of the job
      if (attemptId.getAttemptNum() > 0) {
        // create a new JobAttempt for the new attempt
        final JobInfo jobInfo = ProtostuffUtil.copy(job.getJobAttempt().getInfo())
          .setStartTime(System.currentTimeMillis()) // use different startTime for every attempt
          .setFailureInfo(null)
          .setDetailedFailureInfo(null)
          .setResultMetadataList(new ArrayList<ArrowFileMetadata>());

        final JobAttempt jobAttempt = new JobAttempt()
                .setInfo(jobInfo)
                .setReason(reason)
                .setEndpoint(identity)
                .setDetails(new JobDetails())
                .setState(PENDING);

        job.addAttempt(jobAttempt);
      }

      job.getJobAttempt().setAttemptId(AttemptIdUtils.toString(attemptId));

      if (isInternal) {
        if (streamResultsMode) {
          attemptObserver = new InternalJobResultStreamingListener(attemptId, job, allocator, eventObserver, planTransformationListener, listeners);
        } else {
          attemptObserver = new JobResultListener(attemptId, job, allocator, eventObserver, planTransformationListener, listeners);
        }
      } else {
        attemptObserver = new ExternalJobResultListener(attemptId, responseHandler, job, allocator, listeners);
      }
      if (!isInternal) {
        this.executionControls = new ExecutionControls(sessionOptionManager, JobsServiceUtil.toPB(identity));
      } else {
        this.executionControls = new ExecutionControls(optionManagerProvider.get(), JobsServiceUtil.toPB(identity));
      }

      storeJob(job);

      return attemptObserver;
    }

    @Override
    public void execCompletion(UserResult userResult) {
      final QueryState state = userResult.getState();
      final QueryProfile profile = userResult.getProfile();
      final UserException ex = userResult.getException();
      try {
        // mark the job as completed
        job.setCompleted(true);

        if (state == QueryState.COMPLETED) {
          final OptionManager optionManager = optionManagerProvider.get();
          if (QueryTypeUtils.isAccelerationType(job.getJobAttempt().getInfo().getQueryType()) ||
            optionManager.getOption(ExecConstants.ENABLE_JOIN_ANALYSIS_POPULATOR)) {

            QueryProfile fullProfile = getProfileFromJob(job, job.getAttempts().size() - 1);
            attemptObserver.detailsPopulator.attemptCompleted(fullProfile);

            final JoinAnalysis joinAnalysis;
            if (attemptObserver.joinPreAnalyzer != null) {
              JoinAnalyzer joinAnalyzer = new JoinAnalyzer(fullProfile, attemptObserver.joinPreAnalyzer);
              joinAnalysis = joinAnalyzer.computeJoinAnalysis();
            } else {
              // If no prel, probably because user only asked for the plan
              joinAnalysis = null;
            }

            if (joinAnalysis != null) {
              job.getJobAttempt().getInfo().setJoinAnalysis(joinAnalysis);
            }
          }
        }

        injector.injectChecked(executionControls, INJECTOR_ATTEMPT_COMPLETION_ERROR, IOException.class);
        injector.injectChecked(executionControls, INJECTOR_ATTEMPT_COMPLETION_KV_ERROR, DatastoreException.class);
        // includes a call to storeJob()
        addAttemptToJob(job, state, profile, ex);

      } catch (Exception e) {
        exception.addException(e);
      }

      logger.debug("Removing job from running job list: {}", job.getJobId().getId());
      runningJobs.remove(job.getJobId());

      if (ex != null) {
        exception.addException(ex);
      }

      if (attemptObserver.getException() != null) {
        exception.addException(attemptObserver.getException());
      }

      this.completionLatch.countDown();

      if (isInternal) {
        try {
          switch (state) {
            case COMPLETED:
            case CANCELED:
              eventObserver.onFinalJobSummary(JobEvent.newBuilder()
                  .setFinalJobSummary(JobsServiceUtil.toJobSummary(job))
                  .build());
              eventObserver.onCompleted();
              sessionObserver.onCompleted();
              break;
            case FAILED:
              eventObserver.onError(ex);
              sessionObserver.onError(ex);
              break;

            default:
              logger.warn("Invalid completed state {}", state);
          }
        } catch (Exception e) {
          exception.addException(e);
        }
      } else {
        // send result to client.
        UserResult newResult = userResult.withNewQueryId(ExternalIdHelper.toQueryId(externalId));
        responseHandler.completed(newResult);
      }

      listeners.close(JobsServiceUtil.toJobSummary(job));
      try {
        AutoCloseables.close(eventObserver);
      } catch (Exception e) {
        logger.error("Exception while closing JobEventObserver: {}", job.getJobId(), e);
      }

      queryLoggerExecutorService.submit(() -> {
        //We dont want to load the query profile as part of job execution for DCS. Hence, separating this into a thread.
        populateJobDetailsFromFullProfile(job);
        jobResultLogger.info(String.format("Query: %s; outcome: %s",job.getJobId().getId(),job.getJobAttempt().getState()), job);
      });
    }
  }

  private static class ExternalJobLoader implements JobLoader {

    private final CountDownLatch completionLatch;
    private final DeferredException exception;

    public ExternalJobLoader(CountDownLatch completionLatch, DeferredException exception) {
      super();
      this.completionLatch = completionLatch;
      this.exception = exception;
    }

    @Override
    public RecordBatches load(int offset, int limit) {
      throw new UnsupportedOperationException("Loading external job results isn't currently supported.");
    }

    @Override
    public void waitForCompletion() {
      try {
        completionLatch.await();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        exception.addException(ex);
      }
    }

    @Override
    public String getJobResultsTable() {
      throw new UnsupportedOperationException("External job results are not stored.");
    }
  }

  private static class InternalJobLoader implements JobLoader {

    private final DeferredException exception;
    private final CountDownLatch completionLatch;
    private final JobId id;
    private final JobResultsStore jobResultsStore;
    private final LegacyIndexedStore<JobId, JobResult> store;


    public InternalJobLoader(DeferredException exception, CountDownLatch completionLatch, JobId id,
        JobResultsStore jobResultsStore, LegacyIndexedStore<JobId, JobResult> store) {
      super();
      this.exception = exception;
      this.completionLatch = completionLatch;
      this.id = id;
      this.jobResultsStore = jobResultsStore;
      this.store = store;
    }

    @Override
    public RecordBatches load(int offset, int limit) {
      try {
        completionLatch.await();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        exception.addException(ex);
      }

      exception.throwNoClearRuntime();

      return jobResultsStore.loadJobData(id, store.get(id), offset, limit);
    }

    @Override
    public void waitForCompletion() {
      try {
        completionLatch.await();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        exception.addException(ex);
      }

      exception.throwNoClearRuntime();
    }

    @Override
    public String getJobResultsTable() {
      return jobResultsStore.getJobResultsTableName(id);
    }
  }

  /**
   * A query observer for external queries. Delegates the data back to the original connection.
   */
  private final class ExternalJobResultListener extends JobResultListener {

    private final UserResponseHandler connection;
    private final ExternalId externalId;

    ExternalJobResultListener(
        AttemptId attemptId,
        UserResponseHandler connection,
        Job job,
        BufferAllocator allocator,
        ExternalListenerManager externalListenerManager
    ) {
      super(attemptId, job, allocator, new JobEventCollatingObserver(job.getJobId(), NoopStreamObserver.instance(), executorService, false),
          PlanTransformationListener.NO_OP, externalListenerManager);
      this.connection = connection;
      this.externalId = JobsServiceUtil.getJobIdAsExternalId(job.getJobId());
    }

    @Override
    public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
      connection.sendData(outcomeListener, ExternalIdHelper.replaceQueryId(result, externalId));

      // TODO: maybe capture and write the first few result batches to the job store so we can view those results?
    }
  }

  /**
   * A query observer for internal queries with streamResultsMode. Delegates the data back to the original grpc connection.
   */
  private final class InternalJobResultStreamingListener extends JobResultListener {

    private final JobEventCollatingObserver eventObserver;

    InternalJobResultStreamingListener(AttemptId attemptId,
                                       Job job,
                                       BufferAllocator allocator,
                                       JobEventCollatingObserver eventObserver,
                                       PlanTransformationListener planTransformationListener,
                                       ExternalListenerManager externalListenerManager) {
      super(attemptId, job, allocator, eventObserver, planTransformationListener,
        externalListenerManager);
      this.eventObserver = eventObserver;
    }

    @Override
    public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch data) {
      try {
        int totalSize = 0;
        for (int i = 0; i < data.getBuffers().length; i++) {
          totalSize += data.getBuffers()[i].readableBytes();
        }
        com.google.protobuf.ByteString.Output outputStream = com.google.protobuf.ByteString.newOutput(totalSize);

        for (int i = 0; i < data.getBuffers().length; i++) {
          data.getBuffers()[i].readBytes(outputStream, data.getBuffers()[i].readableBytes());
          data.getBuffers()[i].release();
        }
        // todo: fix ser/deser overhead (https://dremio.atlassian.net/browse/DX-46512)
        eventObserver.onData(JobEvent.newBuilder().setResultData(outputStream.toByteString()).build(), outcomeListener);
      } catch (IOException ex) {
        outcomeListener.failed(new RpcException(ex));
        getDeferredException().addException(ex);
      }
    }
  }

  @VisibleForTesting
  Iterable<Job> getAllJobs(){
    LegacyFindByCondition condition = new LegacyFindByCondition();
    condition.addSortings(DEFAULT_SORTER);
    return toJobs(store.find(condition));
  }

  @VisibleForTesting
  void storeJob(Job job) {
    store.put(job.getJobId(), job.toJobResult(job));
  }

  void recordJobResult(StoreJobResultRequest request) {
    final JobId jobId = JobsProtoUtil.toStuff(request.getJobId());
    final JobResult jobResult = JobsServiceUtil.toJobResult(request);
    store.put(jobId, jobResult);
  }

  /**
   * Listener for internally submitted jobs.
   */
  private class JobResultListener extends AbstractAttemptObserver {
    private final AttemptId attemptId;

    private final DeferredException exception = new DeferredException();
    private final Job job;
    private final JobId jobId;
    private final BufferAllocator allocator;
    private final JobEventCollatingObserver eventObserver;
    private final PlanTransformationListener planTransformationListener;
    private QueryMetadata.Builder builder;
    private final AccelerationDetailsPopulator detailsPopulator;
    private final ExternalListenerManager externalListenerManager;
    private JoinPreAnalyzer joinPreAnalyzer;

    JobResultListener(AttemptId attemptId, Job job, BufferAllocator allocator,
                      JobEventCollatingObserver eventObserver, PlanTransformationListener planTransformationListener,
        ExternalListenerManager externalListenerManager) {
      Preconditions.checkNotNull(jobResultsStore);
      this.attemptId = attemptId;
      this.job = job;
      this.jobId = job.getJobId();
      this.allocator = allocator;
      this.builder = QueryMetadata.builder(namespaceService, storageName)
          .addQuerySql(job.getJobAttempt().getInfo().getSql())
          .addQueryContext(job.getJobAttempt().getInfo().getContextList());
      this.eventObserver = eventObserver;
      this.planTransformationListener = planTransformationListener;
      this.detailsPopulator = accelerationManagerProvider.get().newPopulator();
      this.externalListenerManager = externalListenerManager;
    }

    Exception getException() {
      return exception.getException();
    }

    DeferredException getDeferredException() {
      return exception;
    }

    @Override
    public void recordsProcessed(long recordCount) {
      job.setRecordCount(recordCount);
      externalListenerManager.queryProgressed(JobsServiceUtil.toJobSummary(job));
    }

    @Override
    public void recordsOutput(long outputRecords) {
      JobAttempt jobAttempt = job.getJobAttempt();
      if (jobAttempt.getDetails() != null) {
        jobAttempt.getDetails().setOutputRecords(outputRecords);
      } else {
        JobDetails details = new JobDetails();
        jobAttempt.setDetails(details.setOutputRecords(outputRecords));
      }
      JobSummary summary = JobsServiceUtil.toJobSummary(job);
      externalListenerManager.queryProgressed(summary);
    }

    @Override
    public void queryStarted(UserRequest query, String user) {
      job.getJobAttempt().getInfo().setRequestType(query.getRequestType());
      job.getJobAttempt().getInfo().setSql(query.getSql());
      job.getJobAttempt().getInfo().setDescription(query.getDescription());
      storeJob(job);

      if (externalListenerManager != null) {
        externalListenerManager.queryProgressed(JobsServiceUtil.toJobSummary(job));
      }
    }

    @Override
    public void commandPoolWait(long waitInMillis) {
      final JobInfo jobInfo = job.getJobAttempt().getInfo();

      long currentWait = Optional.fromNullable(jobInfo.getCommandPoolWaitMillis()).or(0L);
      jobInfo.setCommandPoolWaitMillis(currentWait + waitInMillis);

      storeJob(job);

      if (externalListenerManager != null) {
        externalListenerManager.queryProgressed(JobsServiceUtil.toJobSummary(job));
      }
    }

    @Override
    public void planStart(String rawPlan) {
    }

    @Override
    public void planValidated(RelDataType rowType, SqlNode node, long millisTaken) {
      builder.addRowType(rowType).addParsedSql(node);
    }

    @Override
    public void planParallelized(final PlanningSet planningSet) {
      builder.setPlanningSet(planningSet);
    }

    @Override
    public void planSubstituted(DremioMaterialization materialization, List<RelNode> substitutions, RelNode target, long millisTaken, boolean defaultReflection) {
      detailsPopulator.planSubstituted(materialization, substitutions, target, millisTaken, defaultReflection);
    }

    @Override
    public void substitutionFailures(Iterable<String> errors) {
      detailsPopulator.substitutionFailures(errors);
    }

    @Override
    public void applyAccelDetails(final CachedAccelDetails accelDetails) {
      List<RelNode> dummy = new ArrayList<>();
      dummy.add(null);
      for (Map.Entry<DremioMaterialization, RelNode> entry : accelDetails.getMaterializationStore().entrySet()) {
        detailsPopulator.planSubstituted(
          entry.getKey(), dummy,
          entry.getValue(), 0, accelDetails.getLmvProfile(entry.getKey().getReflectionId()).getDefaultReflection());
      }
      planAccelerated(accelDetails.getSubstitutionInfo());
    }

    @Override
    public void planAccelerated(final SubstitutionInfo info) {
      final JobInfo jobInfo = job.getJobAttempt().getInfo();
      final Acceleration acceleration = new Acceleration(info.getAcceleratedCost());
      final List<Acceleration.Substitution> substitutions = Lists.transform(info.getSubstitutions(),
          new Function<SubstitutionInfo.Substitution, Acceleration.Substitution>() {
            @Nullable
            @Override
            public Acceleration.Substitution apply(@Nullable final SubstitutionInfo.Substitution sub) {
              final Acceleration.Substitution.Identifier id = new Acceleration.Substitution.Identifier(
                  "", sub.getMaterialization().getLayoutId());

              id.setMaterializationId(sub.getMaterialization().getMaterializationId());

              return new Acceleration.Substitution(id, sub.getMaterialization().getOriginalCost(), sub.getSpeedup())
                .setTablePathList(sub.getMaterialization().getPath());
            }
          });
      acceleration.setSubstitutionsList(substitutions);
      jobInfo.setAcceleration(acceleration);
      storeJob(job);

      detailsPopulator.planAccelerated(info);
    }

    @Override
    public void planCompleted(final ExecutionPlan plan) {
      if (plan != null) {
        try {
          builder.addBatchSchema(RootSchemaFinder.getSchema(plan.getRootOperator()));
        } catch (Exception e) {
          exception.addException(e);
        }
      }
      job.getJobAttempt().setAccelerationDetails(
        ByteString.copyFrom(detailsPopulator.computeAcceleration()));

      storeJob(job);

      if (externalListenerManager != null) {
        externalListenerManager.queryProgressed(JobsServiceUtil.toJobSummary(job));
      }

      // plan is parallelized after physical planning is done so we need to finalize metadata here
      finalizeMetadata();
    }

    @Override
    public void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo) {
      final JobInfo jobInfo = job.getJobAttempt().getInfo();
      if (resourceSchedulingDecisionInfo != null) {
        if (jobInfo.getResourceSchedulingInfo() == null) {
          jobInfo.setResourceSchedulingInfo(new ResourceSchedulingInfo());
        }
        jobInfo.getResourceSchedulingInfo()
          .setQueueName(resourceSchedulingDecisionInfo.getQueueName())
          .setQueueId(resourceSchedulingDecisionInfo.getQueueId())
          .setResourceSchedulingStart(resourceSchedulingDecisionInfo.getSchedulingStartTimeMs())
          .setResourceSchedulingEnd(resourceSchedulingDecisionInfo.getSchedulingEndTimeMs())
          .setQueryCost(resourceSchedulingDecisionInfo.getResourceSchedulingProperties().getQueryCost())
          .setEngineName(resourceSchedulingDecisionInfo.getEngineName());
        storeJob(job);
      }
    }

    @Override
    public void execStarted(QueryProfile profile) {
      try (TimedBlock b = Timer.time("execStarted")) {
        b.addID("attempt=" + attemptId);

        final JobInfo jobInfo = job.getJobAttempt().getInfo();
        if(profile != null){
          jobInfo.setStartTime(profile.getStart());

          final QueryProfileParser profileParser = new QueryProfileParser(jobId, profile);
          if (profile.getResourceSchedulingProfile() != null) {
            if (jobInfo.getResourceSchedulingInfo() == null) {
              jobInfo.setResourceSchedulingInfo(new ResourceSchedulingInfo());
            }
            jobInfo.getResourceSchedulingInfo().setQueueName(profile.getResourceSchedulingProfile().getQueueName());
            jobInfo.getResourceSchedulingInfo().setQueueId(profile.getResourceSchedulingProfile().getQueueId());
            jobInfo.getResourceSchedulingInfo().setEngineName(profile.getResourceSchedulingProfile().getEngineName());
          }
          job.getJobAttempt().setStats(profileParser.getJobStats());
          job.getJobAttempt().setDetails(profileParser.getJobDetails());
          storeJob(job);

          if (externalListenerManager != null) {
            externalListenerManager.queryProgressed(JobsServiceUtil.toJobSummary(job));
          }
        }
      } catch (IOException e) {
        exception.addException(e);
      }
    }

    private void finalizeMetadata(){
      try {
        QueryMetadata metadata = builder.build();
        builder = null; // no longer needed.

        Optional<List<ParentDatasetInfo>> parents = metadata.getParents();
        JobInfo jobInfo = job.getJobAttempt().getInfo();
        if (parents.isPresent()) {
          jobInfo.setParentsList(parents.get());
        }
        Optional<List<JoinInfo>> joins = metadata.getJoins();
        if (joins.isPresent()) {
          jobInfo.setJoinsList(joins.get());
        }
        Optional<List<FieldOrigin>> fieldOrigins = metadata.getFieldOrigins();
        if (fieldOrigins.isPresent()) {
          jobInfo.setFieldOriginsList(fieldOrigins.get());
        }
        Optional<List<ParentDataset>> grandParents = metadata.getGrandParents();
        if (grandParents.isPresent()) {
          jobInfo.setGrandParentsList(grandParents.get());
        }
        final Optional<RelOptCost> cost = metadata.getCost();
        if (cost.isPresent()) {
          final double aggCost = DremioCost.aggregateCost(cost.get());
          jobInfo.setOriginalCost(aggCost);
        }
        final Optional<PlanningSet> planningSet = metadata.getPlanningSet();
        if (planningSet.isPresent()) {
          final List<String> partitions = JobsServiceUtil.getPartitions(planningSet.get());
          jobInfo.setPartitionsList(partitions);
        }

        if (metadata.getScanPaths() != null) {
          jobInfo.setScanPathsList(metadata.getScanPaths());
        }
        Optional<BatchSchema> schema = metadata.getBatchSchema();
        if (schema.isPresent()) {
          // There is DX-14280. We will be able to remove clone call, when it would be resolved.
          jobInfo.setBatchSchema(schema.get().clone(BatchSchema.SelectionVectorMode.NONE).toByteString());
        }
        if (metadata.getSourceNames() != null) {
          jobInfo.setSourceNamesList(metadata.getSourceNames());
        }
        if (metadata.getSinkPath() != null) {
          jobInfo.setSinkPathList(metadata.getSinkPath());
        }

        storeJob(job);
        eventObserver.onQueryMetadata(JobEvent.newBuilder()
            .setQueryMetadata(JobsProtoUtil.toBuf(metadata))
            .build());
        externalListenerManager.metadataAvailable(JobsProtoUtil.toBuf(metadata));
      }catch(Exception ex){
        exception.addException(ex);
      }
    }

    @Override
    public void recordExtraInfo(String name, byte[] bytes) {
      //TODO DX-10977 the reflection manager should rely on its own observer to store this information in a separate store
      if(job.getJobAttempt().getExtraInfoList() == null) {
        job.getJobAttempt().setExtraInfoList(new ArrayList<ExtraInfo>());
      }

      job.getJobAttempt().getExtraInfoList().add(new ExtraInfo()
          .setData(ByteString.copyFrom(bytes))
          .setName(name));
      storeJob(job);
      super.recordExtraInfo(name, bytes);
    }

    @Override
    public void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after, long millisTaken) {
      planTransformationListener.onPhaseCompletion(phase, before, after, millisTaken);
      switch(phase){
      case LOGICAL:
        builder.addLogicalPlan(before, after);
        // set final pre-accelerated cost
        final RelOptCost cost = after.getCluster().getMetadataQuery().getCumulativeCost(after);
        builder.addCost(cost);
        break;
      case JOIN_PLANNING_MULTI_JOIN:
        // Join planning starts with multi-join analysis phase
        builder.addPreJoinPlan(before);
        break;
      case REDUCE_EXPRESSIONS:
        builder.addExpandedPlan(before);
        break;
      case PHYSICAL:
        builder.addPhysicalPlan(before);
        break;
      default:
        break;
      }
    }

    @Override
    public void finalPrel(Prel prel) {
      joinPreAnalyzer = JoinPreAnalyzer.prepare(prel);
    }

    @Override
    public synchronized void attemptCompletion(UserResult result) {
      try {
        final QueryState queryState = result.getState();
        if (queryState == QueryState.COMPLETED) {
          storeJob(job);
          detailsPopulator.attemptCompleted(result.getProfile());
          if (joinPreAnalyzer != null) {
            JoinAnalyzer joinAnalyzer = new JoinAnalyzer(result.getProfile(), joinPreAnalyzer);
            JoinAnalysis joinAnalysis = joinAnalyzer.computeJoinAnalysis();
            if (joinAnalysis != null) {
              job.getJobAttempt().getInfo().setJoinAnalysis(joinAnalysis);
            }
          }
        }
        addAttemptToJob(job, queryState, result.getProfile(), result.getException());
      } catch (IOException e) {
        exception.addException(e);
      }
    }

    @Override
    public synchronized void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
      try (TimedBlock b = Timer.time("dataMetadataArrived");
          QueryDataBatch dataBatch = LocalUserUtil.acquireData(allocator, outcomeListener, result);
          RecordBatchLoader loader = new RecordBatchLoader(allocator)) {
        b.addID("attempt=" + attemptId);
        loader.load(dataBatch.getHeader().getDef(), dataBatch.getData());

        // Query output just contains the batch unique id and number of records in the batch.
        try (RecordBatchData batch = new RecordBatchData(loader, allocator)) {
          List<ValueVector> vectors = batch.getVectors();

          if (vectors.size() < 4 || !(vectors.get(3) instanceof VarBinaryVector) ) {
            throw UserException.unsupportedError()
                .message("Job output contains invalid data")
                .build(logger);
          }

          VarBinaryVector metadataVector = (VarBinaryVector) vectors.get(3);

          for (int i = 0; i < batch.getRecordCount(); i++) {
            final ArrowFileFormat.ArrowFileMetadata metadata =
                ArrowFileFormat.ArrowFileMetadata.parseFrom(metadataVector.getObject(i));
            job.getJobAttempt().getInfo().getResultMetadataList().add(ArrowFileReader.toBean(metadata));
          }
        }
      } catch (Exception ex) {
        exception.addException(ex);
      }
    }

    @Override
    public void beginState(AttemptEvent event) {
      final JobAttempt jobAttempt = job.getJobAttempt();
      synchronized (jobAttempt) {
        if (!isTerminal(event.getState())) {
          jobAttempt.setState(JobsServiceUtil.attemptStatusToJobStatus(event.getState()));
        }

        if (jobAttempt.getStateListList() == null) {
          jobAttempt.setStateListList(new ArrayList<>());
        }
        jobAttempt.getStateListList().add(JobsServiceUtil.createAttemptEvent(event.getState(), event.getStartTime()));
      }
      storeJob(job);

      if (externalListenerManager != null) {
        externalListenerManager.queryProgressed(JobsServiceUtil.toJobSummary(job));
      }
    }

    @Override
    public void updateReflectionsWithHints(ReflectionExplanationsAndQueryDistance reflectionExplanationsAndQueryDistance) {
      detailsPopulator.addReflectionHints(reflectionExplanationsAndQueryDistance);
    }
  }

  private boolean isTerminal(AttemptEvent.State state) {
    return (state == AttemptEvent.State.COMPLETED ||
      state == AttemptEvent.State.CANCELED ||
      state == AttemptEvent.State.FAILED);
  }

  private void addAttemptToJob(Job job, QueryState state, QueryProfile profile, UserException ex) throws IOException {

      final JobAttempt jobAttempt = job.getJobAttempt();
      final JobInfo jobInfo = jobAttempt.getInfo();
      jobInfo.setStartTime(profile.getStart());
      jobInfo.setFinishTime(profile.getEnd());
      if (profile.getResourceSchedulingProfile() != null) {
        if (jobInfo.getResourceSchedulingInfo() == null) {
          jobInfo.setResourceSchedulingInfo(new ResourceSchedulingInfo());
        }
        jobInfo.getResourceSchedulingInfo().setQueueName(profile.getResourceSchedulingProfile().getQueueName());
        jobInfo.getResourceSchedulingInfo().setQueueId(profile.getResourceSchedulingProfile().getQueueId());
        jobInfo.getResourceSchedulingInfo().setEngineName(profile.getResourceSchedulingProfile().getEngineName());
      }
      switch (state) {
      case FAILED:
        if (profile.hasError()) {
          jobInfo.setFailureInfo(profile.getError());
        } else if (ex != null) {
          jobInfo.setFailureInfo(ex.getVerboseMessage(false));
        }
        if (profile.hasVerboseError()) {
          jobInfo.setDetailedFailureInfo(JobsServiceUtil.toFailureInfo(profile.getVerboseError()));
        }
        break;
      case CANCELED:
        if (profile.hasCancelReason()) {
          final JobCancellationInfo cancellationInfo = new JobCancellationInfo();
          cancellationInfo.setMessage(profile.getCancelReason());
          jobInfo.setCancellationInfo(cancellationInfo);
        }
        break;
      default:
        // nothing
      }

      jobAttempt.getInfo().setOutputTableList(Arrays.asList(storageName, jobAttempt.getAttemptId()));

      updateJobDetails(job, profile);
      jobAttempt.setState(JobsServiceUtil.queryStatusToJobStatus(state));

      storeJob(job);
  }

  private void updateJobDetails(Job job, QueryProfile profile) throws IOException {
    JobAttempt jobAttempt = job.getJobAttempt();

    // continue if the profile is a full profile
    if (profile.getFragmentProfileList() != null && profile.getFragmentProfileList().size() != 0) {
      final QueryProfileParser profileParser = new QueryProfileParser(job.getJobId(), profile);

      jobAttempt.setDetails(profileParser.getJobDetails());

      jobAttempt.getInfo().setSpillJobDetails(profileParser.getSpillDetails());

      jobAttempt.getInfo().setExecutionCpuTimeNs(0L);
      jobAttempt.getInfo().setSetupTimeNs(0L);
      jobAttempt.getInfo().setWaitTimeNs(0L);
      jobAttempt.getInfo().setMemoryAllocated(0L);
      profile.getFragmentProfileList().forEach(majorFrag -> {
        if (majorFrag.getMinorFragmentProfileList() != null) {
          majorFrag.getMinorFragmentProfileList().forEach(minorFrag -> {
            if (minorFrag.getOperatorProfileList() != null) {
              minorFrag.getOperatorProfileList().forEach(opProfile -> {
                jobAttempt.getInfo().setExecutionCpuTimeNs(jobAttempt.getInfo().getExecutionCpuTimeNs()+opProfile.getProcessNanos());
                jobAttempt.getInfo().setSetupTimeNs(jobAttempt.getInfo().getSetupTimeNs() + opProfile.getSetupNanos());
                jobAttempt.getInfo().setWaitTimeNs(jobAttempt.getInfo().getWaitTimeNs() + opProfile.getWaitNanos());
                jobAttempt.getInfo().setMemoryAllocated(jobAttempt.getInfo().getMemoryAllocated() + opProfile.getPeakLocalMemoryAllocated());
              });
            }
          });
        }
      });

      jobAttempt.setStats(profileParser.getJobStats());
    }

    if (profile.getNodeProfileList() != null) {
      final List<ExecutionNode> executionNodes = new ArrayList<>();
      profile.getNodeProfileList().forEach(nodeProfile -> {
        ExecutionNode executionNode = new ExecutionNode();
        executionNode.setFabricPort(nodeProfile.getEndpoint().getFabricPort());
        executionNode.setHostName(nodeProfile.getEndpoint().getAddress());
        executionNode.setHostIp(nodeProfile.getEndpoint().getAddress());
        executionNode.setMaxMemoryUsedKb((int)(nodeProfile.getMaxMemoryUsed()/1000L));
        executionNodes.add(executionNode);
      });
      jobAttempt.getInfo().setNodeDetailsList(executionNodes);
    }
  }

  private boolean jobIsDone(JobAttempt config){
    switch(config.getState()){
    case CANCELED:
    case COMPLETED:
    case FAILED:
      return true;
    default:
      return false;
    }
  }

  QueryProfile getProfile(QueryProfileRequest queryProfileRequest)
    throws JobNotFoundException {
    JobId jobId = JobsProtoUtil.toStuff(queryProfileRequest.getJobId());
    int attempt = queryProfileRequest.getAttempt();

    GetJobRequest request = GetJobRequest.newBuilder()
      .setJobId(jobId)
      .setUserName(queryProfileRequest.getUserName())
      .build();
    Job job = getJob(request);

    return getProfileFromJob(job, attempt);
  }

  QueryProfile getProfileFromJob(Job job, int attempt) {
    final AttemptId attemptId = new AttemptId(JobsServiceUtil.getJobIdAsExternalId(job.getJobId()), attempt);
    if (jobIsDone(job.getJobAttempt())) {
      return jobTelemetryServiceStub.getQueryProfile(
        GetQueryProfileRequest.newBuilder()
          .setQueryId(attemptId.toQueryId())
          .build()
      ).getProfile();
    }

    // Check if the profile for given attempt already exists. Even if the job is not done, it is possible that
    // profile exists for previous attempts
    try {
      return jobTelemetryServiceStub.getQueryProfile(
        GetQueryProfileRequest.newBuilder()
          .setQueryId(attemptId.toQueryId())
          .build()
      ).getProfile();
    } catch (StatusRuntimeException ignored) {
    }

    final NodeEndpoint endpoint = job.getJobAttempt().getEndpoint();
    QueryProfile qp = null;

    if (!endpoint.equals(identity)) {
      final QueryProfileRequest request = QueryProfileRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(job.getJobId()))
        .setAttempt(attempt)
        .setUserName(SYSTEM_USERNAME)
        .build();
      qp = forwarder.getProfile(JobsServiceUtil.toPB(endpoint), request);
    }
    return qp;
  }

  void cancel(CancelJobRequest request) throws JobException {
    final String reason = request.getReason();
    final JobId jobId = JobsProtoUtil.toStuff(request.getJobId());

    final ForemenTool tool = this.foremenTool.get();
    final ExternalId id = ExternalIdHelper.toExternal(QueryIdHelper.getQueryIdFromString(jobId.getId()));
    if(tool.cancel(id, reason)){
      logger.debug("Job cancellation requested on current node.");
      return;
    }

    // now remote...
    final GetJobRequest getJobRequest = GetJobRequest.newBuilder()
        .setJobId(jobId)
        .setUserName(SYSTEM_USERNAME) //TODO (DX-17909): Add and use username in request
        .build();
    final Job job = getJob(getJobRequest);
    NodeEndpoint endpoint = job.getJobAttempt().getEndpoint();

    remoteCancel(jobId, id, endpoint, reason);
  }

  void remoteCancel(JobId jobId, ExternalId externalId, NodeEndpoint endpoint, String reason) throws JobException {
    if(endpoint.equals(identity)){
      throw new JobCancelException(String.format("Job %s may have completed and cannot be canceled.",jobId.getId()));
    }

    try{
      final CoordTunnel tunnel = coordTunnelCreator.get().getTunnel(JobsServiceUtil.toPB(endpoint));
      Ack ack = DremioFutures.getChecked(tunnel.requestCancelQuery(externalId, reason), RpcException.class, 15, TimeUnit.SECONDS, RpcException::mapException);
      if(ack.getOk()){
        logger.debug("Job cancellation requested on {}.", endpoint.getAddress());
        return;
      } else {
        throw new JobCancelException(String.format("Job %s may have completed and cannot be canceled.",jobId.getId()));
      }
    } catch(TimeoutException | RpcException | RuntimeException e){
      logger.info("Unable to cancel remote job for external id: {}", ExternalIdHelper.toString(externalId), e);
      throw new JobWarningException(jobId, String.format("Unable to cancel job on node %s.", endpoint.getAddress()));
    }
  }

  /**
   * Result of deleting jobs.
   */
  public static final class DeleteResult {
    private final long jobsDeleted;
    private final List<AttemptId> attemptIds;

    private DeleteResult(long jobsDeleted, List<AttemptId> attemptIds) {
      super();
      this.jobsDeleted = jobsDeleted;
      this.attemptIds = attemptIds;
    }

    public long getJobsDeleted() {
      return jobsDeleted;
    }

    public long getProfilesDeleted() {
      return attemptIds.size();
    }

    public List<AttemptId> getDeletedAttemptIds() {
      return attemptIds;
    }
  }

  /**
   * Online profile deletion using Job Telemetry Service
   * Schedule in the background
   */
  class OnlineProfileCleaner extends ExternalCleaner {

    @Override
    public void doGo(JobAttempt jobAttempt) {
      jobTelemetryServiceStub
        .deleteProfile(
          DeleteProfileRequest.newBuilder()
            .setQueryId(AttemptIdUtils.fromString(jobAttempt.getAttemptId()).toQueryId())
            .build()
        );
    }

  }

  /**
   * Delete job details older than provided number of ms.
   * <p>
   * Exposed as static so that cleanup tasks can do this without needing to start a jobs service and supporting daemon.
   *
   * @param externalCleaners defines an ordered sequence of external dependencies that also need to be cleaned up.
   *                         Each {@link ExternalCleaner} are called once per job attempt.
   *                         If an {@code externalCleanup} fails its successors will not be called for the job attempt
   *                         being processed.
   * @param provider         KVStore provider
   * @param maxMs            Age of job after which it is deleted.
   * @return A result reporting how many details, the corresponding attempt ids and how many times attempt id fails
   * to delete.
   */
  public static String deleteOldJobsAndDependencies(List<ExternalCleaner> externalCleaners,
    LegacyKVStoreProvider provider, long maxMs) {
    long jobsDeleted = 0;
    LegacyIndexedStore<JobId, JobResult> jobStore = provider.getStore(JobsStoreCreator.class);

    final LegacyFindByCondition oldJobs = getOldJobsCondition(System.currentTimeMillis() - maxMs)
      .setPageSize(MAX_NUMBER_JOBS_TO_FETCH);
    final ExternalCleanerRunner externalCleanerRunner = new ExternalCleanerRunner(externalCleaners);
    for (Entry<JobId, JobResult> entry : jobStore.find(oldJobs)) {
      JobResult result = entry.getValue();
      externalCleanerRunner.run(result);
      jobStore.delete(entry.getKey());
      jobsDeleted++;
    }
    logger.debug("Job cleanup task completed with [{}] jobs deleted and and [{}] profiles deleted.", jobsDeleted, 0L);
    if (externalCleanerRunner.hasErrors()) {
      externalCleanerRunner.printLastErrors();
    }
    return buildDeleteReport(jobsDeleted, externalCleanerRunner);
  }

  private static String buildDeleteReport(long jobsDeleted, ExternalCleanerRunner externalCleanerRunner) {
    StringBuilder sb = new StringBuilder("Completed.");
    sb.append(" Deleted ").append(jobsDeleted).append(" jobs.");
    sb.append(externalCleanerRunner.getReport());
    return sb.toString();
  }

  /**
   * For the master coordinator, periodically iterate over all jobs, only
   * marking jobs as FAILED if their issuing coordinator is no longer present.
   */
  class AbandonLocalJobsTask implements Runnable {
    @Override
    public void run() {
      manage();
    }

    public void manage() {
      setAbandonedJobsToFailedState(store,
        jobServiceInstances.get());
    }
  }

  /**
   * Removes the job results
   */
  class JobResultsCleanupTask implements Runnable {
    @Override
    public void run() {
      cleanup();
    }

    public void cleanup() {
      //obtain the max age values during each cleanup as the values could change.
      final OptionManager optionManager = optionManagerProvider.get();
      final long maxAgeInMillis = optionManager.getOption(ExecConstants.DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS);
      long maxAgeInDays = optionManager.getOption(ExecConstants.RESULTS_MAX_AGE_IN_DAYS);
      long jobResultsMaxAgeInMillis = (maxAgeInDays * ONE_DAY_IN_MILLIS) + maxAgeInMillis;
      long cutOffTime = System.currentTimeMillis() - jobResultsMaxAgeInMillis;
      if (maxAgeInDays != DISABLE_CLEANUP_VALUE) {
        cleanupJobs(cutOffTime);
      }
    }

    private void cleanupJobs(long cutOffTime) {
      //iterate through the job results and cleanup.
      final LegacyFindByCondition condition = getOldJobsCondition(cutOffTime).setPageSize(MAX_NUMBER_JOBS_TO_FETCH);

      for (Entry<JobId, JobResult> entry : store.find(condition)) {
        jobResultsStore.cleanup(entry.getKey());
      }
    }
  }

  public JobResultsCleanupTask createCleanupTask() {
    return new JobResultsCleanupTask();
  }

  /**
   * Removes the job details and profile
   */
  class JobDependenciesCleanupTask implements Runnable {
    private final List<ExternalCleaner> externalCleaners = Stream
      .concat(Stream.of(new OnlineProfileCleaner()), extraExternalCleaners.stream())
      .collect(Collectors.toList());

    @Override
    public void run() {
      cleanup();
    }

    public void cleanup() {
      //obtain the max age values during each cleanup as the values could change.
      final OptionManager optionManager = optionManagerProvider.get();
      final long maxAgeInDays = optionManager.getOption(ExecConstants.JOB_MAX_AGE_IN_DAYS);
      if (maxAgeInDays != DISABLE_CLEANUP_VALUE) {
        deleteOldJobsAndDependencies(externalCleaners, kvStoreProvider.get(),
          TimeUnit.DAYS.toMillis(maxAgeInDays));
      }
    }
  }

  /**
   * Get a condition that returns jobs that have either been completed before the cutoff time or that were started before the cutoff time and never ended.
   * @param cutOffTime The epoch millis cutoff time.
   * @return the condition for kvstore use.
   */
  private static final LegacyFindByCondition getOldJobsCondition(long cutOffTime) {
    SearchQuery searchQuery = SearchQueryUtils.or(
        SearchQueryUtils.and(
            SearchQueryUtils.newExistsQuery(JobIndexKeys.END_TIME.getIndexFieldName()),
            SearchQueryUtils.newRangeLong(JobIndexKeys.END_TIME.getIndexFieldName(), 0L, cutOffTime, true, true)),
        SearchQueryUtils.and(
            SearchQueryUtils.newDoesNotExistQuery(JobIndexKeys.END_TIME.getIndexFieldName()),
            SearchQueryUtils.newRangeLong(JobIndexKeys.END_TIME.getIndexFieldName(), 0L, cutOffTime, true, true)));

    return new LegacyFindByCondition().setCondition(searchQuery);
  }

  /**
   * Creator for jobs.
   */
  public static class JobsStoreCreator implements LegacyIndexedStoreCreationFunction<JobId, JobResult> {

    @SuppressWarnings("unchecked")
    @Override
    public LegacyIndexedStore<JobId, JobResult> build(LegacyStoreBuildingFactory factory) {
      return factory.<JobId, JobResult>newStore()
        .name(JOBS_NAME)
        .keyFormat(Format.wrapped(JobId.class, JobId::getId, JobId::new, Format.ofString()))
        .valueFormat(Format.ofProtostuff(JobResult.class))
        .buildIndexed(new JobConverter());
    }
  }

  Provider<OptionManager> getOptionManagerProvider() {
    return optionManagerProvider;
  }

  Provider<ForemenTool> getForemenTool() {
    return foremenTool;
  }

  void registerReflectionJobListener(ReflectionJobEventsRequest jobEventsRequest, StreamObserver<JobEvent> observer) {
    Job job;
    JobId jobId = JobsProtoUtil.toStuff(jobEventsRequest.getJobId());
    try {
      final GetJobRequest request = GetJobRequest.newBuilder()
        .setJobId(jobId)
        .setUserName(jobEventsRequest.getUserName())
        .build();
      job = getJob(request);
      validateReflectionIdWithJob(job, jobEventsRequest.getReflectionId());

      registerListenerWithJob(job, observer);
    } catch (JobNotFoundException e) {
      throw UserException.validationError()
        .message("Status requested for unknown job %s.", jobId.getId())
        .buildSilently();
    } catch (ReflectionJobValidationException e) {
      throw UserException.validationError()
        .message("Status requested for job %s that didn't materialize the reflection %s.", e.getJobId(), e.getReflectionId())
        .buildSilently();
    }
  }

  Iterable<JobSummary> searchReflectionJobs(SearchReflectionJobsRequest request) {
    String reflectionSearchQuery = getReflectionSearchQuery(request.getReflectionId());

    SearchJobsRequest searchJobsRequest = SearchJobsRequest.newBuilder()
      .setOffset(request.getOffset())
      .setLimit(request.getLimit())
      .setSortColumn("st")
      .setSortOrder(SearchJobsRequest.SortOrder.DESCENDING)
      .setFilterString(reflectionSearchQuery)
      .build();

    return searchJobs(searchJobsRequest);
  }

  com.dremio.service.job.JobSummary getReflectionJobSummary(ReflectionJobSummaryRequest request)
    throws JobNotFoundException, ReflectionJobValidationException {
    final GetJobRequest getJobRequest = GetJobRequest.newBuilder()
      .setJobId(JobsProtoUtil.toStuff(request.getJobSummaryRequest().getJobId()))
      .setUserName(request.getJobSummaryRequest().getUserName())
      .setFromStore(request.getJobSummaryRequest().getFromStore())
      .build();

    final Job job = getJob(getJobRequest);
    validateReflectionIdWithJob(job, request.getReflectionId());
    return JobsServiceUtil.toJobSummary(job);
  }

  com.dremio.service.job.JobDetails getReflectionJobDetails(ReflectionJobDetailsRequest request)
    throws JobNotFoundException, ReflectionJobValidationException {
    final GetJobRequest getJobRequest = GetJobRequest.newBuilder()
      .setJobId(JobsProtoUtil.toStuff(request.getJobDetailsRequest().getJobId()))
      .setUserName(request.getJobDetailsRequest().getUserName())
      .setFromStore(request.getJobDetailsRequest().getFromStore())
      .build();

    final Job job = getJob(getJobRequest);
    validateReflectionIdWithJob(job, request.getReflectionId());
    return JobsServiceUtil.toJobDetails(job, request.getJobDetailsRequest().getProvideResultInfo());
  }

  void cancelReflectionJob(CancelReflectionJobRequest request) throws JobException {
    JobId jobId = JobsProtoUtil.toStuff(request.getCancelJobRequest().getJobId());

    final GetJobRequest getJobRequest = GetJobRequest.newBuilder()
      .setJobId(jobId)
      .setUserName(request.getCancelJobRequest().getUsername())
      .build();

    final Job job = getJob(getJobRequest);
    validateReflectionIdWithJob(job, request.getReflectionId());

    final String reason = request.getCancelJobRequest().getReason();

    final ForemenTool tool = getForemenTool().get();
    final UserBitShared.ExternalId externalId = ExternalIdHelper.toExternal(QueryIdHelper.getQueryIdFromString(jobId.getId()));
    if(tool.cancel(externalId, reason)){
      return;
    }

    NodeEndpoint endpoint = job.getJobAttempt().getEndpoint();
    remoteCancel(jobId, externalId, endpoint, reason);
  }

  UserBitShared.QueryProfile getReflectionJobProfile(ReflectionJobProfileRequest request)
    throws JobNotFoundException, ReflectionJobValidationException {

    GetJobRequest getJobRequest = GetJobRequest.newBuilder()
      .setJobId(JobsProtoUtil.toStuff(request.getQueryProfileRequest().getJobId()))
      .setUserName(request.getQueryProfileRequest().getUserName())
      .build();
    Job job = getJob(getJobRequest);
    validateReflectionIdWithJob(job, request.getReflectionId());

    return getProfileFromJob(job, request.getQueryProfileRequest().getAttempt());
  }

  String getReflectionSearchQuery(String reflectionId) {
    StringBuilder stringBuilder = new StringBuilder().append("(qt==\"ACCELERATION\");(")
      .append("*")
      .append("=contains=")
      .append(reflectionId)
      .append(")");
    return stringBuilder.toString();
  }

  void validateReflectionIdWithJob(Job job, String reflectionId) throws JobNotFoundException, ReflectionJobValidationException {
    if((job.getJobAttempt().getInfo() == null
      || job.getJobAttempt().getInfo().getMaterializationFor() == null
      || job.getJobAttempt().getInfo().getMaterializationFor().getReflectionId() == null
      || !job.getJobAttempt().getInfo().getMaterializationFor().getReflectionId().equals(reflectionId))) {
      throw new ReflectionJobValidationException(job.getJobId(), reflectionId);
    }
  }

  private boolean mustForwardRequest(final Job job) {
    if (job.getJobAttempt().getEndpoint() == null) { //for UTs
      return false;
    }

    final OptionManager optionManager = optionManagerProvider.get();
    final boolean enabled = optionManager.getOption(ExecConstants.ENABLE_REMOTE_JOB_FETCH);
    final NodeEndpoint source = job.getJobAttempt().getEndpoint();
    if (enabled
      && !job.isCompleted()
      && !job.getJobAttempt().getEndpoint().equals(identity)
      && jobServiceInstances.get().contains(JobsProtoUtil.toBuf(source))){
      return true;
    }

    return false;
  }

  class LocalAbandonedJobsHandler implements  AutoCloseable {

    private ScheduledFuture abandonedJobsTask;
    private final CloseableSchedulerThreadPool threadPool;

    public LocalAbandonedJobsHandler() {
      this.threadPool = new CloseableSchedulerThreadPool("local-abandoned-jobs-handler", 1);
    }

    void schedule(long scheduleInterval) {
      abandonedJobsTask = threadPool.scheduleAtFixedRate(() -> terminateLocalAbandonedJobs(), 0, scheduleInterval, TimeUnit.MILLISECONDS);
      logger.info("Scheduled abandonedJobsTask for interval {}", scheduleInterval);
    }

    @Override
    public void close() throws Exception {
      cancel();
      AutoCloseables.close(threadPool);
    }

    private void cancel() {
      if (abandonedJobsTask != null) {
        abandonedJobsTask.cancel(true);
        abandonedJobsTask = null;
      }
    }

    @VisibleForTesting
    public void reschedule(long scheduleInterval) {
      cancel();
      schedule(scheduleInterval);
    }

    private JobAttempt getJobAttemptIfNotFinalState(JobResult jobResult) {
      final List<JobAttempt> attempts = jobResult.getAttemptsList();
      final int numAttempts = attempts.size();
      if (numAttempts > 0) {
        final JobAttempt lastAttempt = attempts.get(numAttempts - 1);
        if (JobsServiceUtil.isNonFinalState(lastAttempt.getState())) {
          return lastAttempt;
        }
      }
      return null;
    }

    private void terminateLocalAbandonedJobs() {
      try {
        final Set<Entry<JobId, JobResult>> apparentlyAbandoned =
          StreamSupport.stream(store.find(new LegacyFindByCondition()
            .setCondition(JobsServiceUtil.getApparentlyAbandonedQuery())).spliterator(), false)
            .collect(Collectors.toSet());

        for (final Entry<JobId, JobResult> entry : apparentlyAbandoned) {
          JobResult jobResult = entry.getValue();
          JobAttempt lastAttempt = getJobAttemptIfNotFinalState(jobResult);
          if (lastAttempt != null) {
            boolean isLocalJob = lastAttempt.getEndpoint().equals(identity);
            boolean isJobInProgress = true;
            if (isLocalJob) {
              isJobInProgress = runningJobs.get(lastAttempt.getInfo().getJobId()) != null;
            }

            if (!isJobInProgress) {
              //Before updating the job to FAILED state check if the job status in store is not final state.
              //This is required because between the time apparentlyAbandoned jobs are retrieved and the time the job
              //is verified to be not running in runningJobs, the job status might have got changed.
              jobResult = store.get(lastAttempt.getInfo().getJobId());
              if (jobResult != null) {
                lastAttempt = getJobAttemptIfNotFinalState(jobResult);
                if (lastAttempt != null) {
                  logger.info("Failing abandoned job {}", lastAttempt.getInfo().getJobId().getId());
                  final long finishTimestamp = System.currentTimeMillis();
                  List<com.dremio.exec.proto.beans.AttemptEvent> attemptEventList = lastAttempt.getStateListList();
                  if (attemptEventList == null) {
                    attemptEventList = new ArrayList<>();
                  }
                  attemptEventList.add(JobsServiceUtil.createAttemptEvent(AttemptEvent.State.FAILED, finishTimestamp));
                  final JobAttempt newLastAttempt = lastAttempt.setState(JobState.FAILED)
                    .setStateListList(attemptEventList)
                    .setInfo(lastAttempt.getInfo().setFinishTime(finishTimestamp)
                    .setFailureInfo("Query failed due to kvstore or network errors. Details and profile information for this job may be partial or missing."));
                  final List<JobAttempt> attempts = jobResult.getAttemptsList();
                  final int numAttempts = attempts.size();
                  attempts.remove(numAttempts - 1);
                  attempts.add(newLastAttempt);
                  jobResult.setCompleted(true); // mark the job as completed
                  store.put(entry.getKey(), jobResult);
                }
              }
            }
          }
        }
      } catch (Exception e) {
        logger.error("Error while setting FAILED state for any abandoned jobs that may be present. Will attempt in next invocation", e);
      }
    }
  }

  @VisibleForTesting
  public LocalAbandonedJobsHandler getLocalAbandonedJobsHandler() {
    return localAbandonedJobsHandler;
  }

  private class JobSubmissionHelper {
    private final SubmitJobRequest jobRequest;
    private final StreamObserver<JobEvent> eventObserver;
    private final PlanTransformationListener planTransformationListener;

    JobSubmissionHelper(SubmitJobRequest jobRequest, StreamObserver<JobEvent> eventObserver, PlanTransformationListener planTransformationListener) {
      this.jobRequest = jobRequest;
      this.eventObserver = eventObserver;
      this.planTransformationListener = planTransformationListener;
    }

    JobSubmission submitJobToCommandPool() {
      return submitJob(this.jobRequest, this.eventObserver, this.planTransformationListener);
    }

    Closeable releaseAndReacquireCommandPool() {
      // return a no-op closeable
      return () -> {};
    }
  }

  private class SubmitJobToReleasableCommandPool extends JobSubmissionHelper {
    private final ReleasableCommandPool releasableCommandPool;

    SubmitJobToReleasableCommandPool(SubmitJobRequest jobRequest, StreamObserver<JobEvent> eventObserver, PlanTransformationListener planTransformationListener, ReleasableCommandPool releasableCommandPool) {
      super(jobRequest, eventObserver, planTransformationListener);
      this.releasableCommandPool = releasableCommandPool;
    }

    @Override
    Closeable releaseAndReacquireCommandPool() {
      return releasableCommandPool.releaseAndReacquireSlot();
    }
  }
}
