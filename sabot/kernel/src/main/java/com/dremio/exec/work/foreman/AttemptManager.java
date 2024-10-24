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

import static com.dremio.telemetry.api.metrics.MeterProviders.newCounterProvider;

import com.dremio.common.EventProcessor;
import com.dremio.common.ProcessExit;
import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.OutOfMemoryOrResourceExceptionContext;
import com.dremio.common.exceptions.UserCancellationException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.maestro.MaestroObserver;
import com.dremio.exec.maestro.MaestroService;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.sql.handlers.commands.AsyncCommand;
import com.dremio.exec.planner.sql.handlers.commands.CommandCreator;
import com.dremio.exec.planner.sql.handlers.commands.CommandRunner;
import com.dremio.exec.planner.sql.handlers.commands.CommandRunner.CommandType;
import com.dremio.exec.planner.sql.handlers.commands.PreparedPlan;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.UserBitShared.DremioPBError;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.util.Utilities;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.exec.work.user.OptionProvider;
import com.dremio.options.OptionManager;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.RuleBasedEngineSelector;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.resource.exception.ResourceUnavailableException;
import com.dremio.service.Pointer;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.jobtelemetry.instrumentation.MetricLabel;
import com.dremio.telemetry.api.metrics.MeterProviders;
import com.dremio.telemetry.api.metrics.SimpleCounter;
import com.dremio.telemetry.api.metrics.SimpleDistributionSummary;
import com.dremio.telemetry.api.metrics.SimpleUpdatableTimer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import io.grpc.Context;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AttemptManager manages all the fragments (local and remote) for a single query where this is the
 * driving node.
 *
 * <p>The flow is as follows: - AttemptManager is submitted as a runnable. - Runnable does query
 * planning. - state changes from PENDING to RUNNING - Status listener are activated - Runnable
 * sends out starting fragments - The Runnable's run() completes, but the AttemptManager stays
 * around - AttemptManager listens for state change messages. - state change messages can drive the
 * state to FAILED or CANCELED, in which case messages are sent to running fragments to terminate -
 * when all fragments complete, state change messages drive the state to COMPLETED
 */
public class AttemptManager implements Runnable, MaestroObserver.ExecutionStageChangeListener {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(AttemptManager.class);
  private static final ControlsInjector injector =
      ControlsInjectorFactory.getInjector(AttemptManager.class);

  private static final SimpleCounter SERVER_ERROR =
      SimpleCounter.of("jobs.server_error", "Number of jobs that resulted in a server error");
  private static final MeterProvider<Counter> JOBS_TOTAL =
      MeterProviders.newCounterProvider("jobs", "Number of jobs attempted,");

  private static final Set<UserBitShared.DremioPBError.ErrorType> CLIENT_ERRORS =
      ImmutableSet.of(
          UserBitShared.DremioPBError.ErrorType.PARSE,
          UserBitShared.DremioPBError.ErrorType.PERMISSION,
          UserBitShared.DremioPBError.ErrorType.VALIDATION,
          UserBitShared.DremioPBError.ErrorType.FUNCTION);

  @VisibleForTesting public static final String INJECTOR_CONSTRUCTOR_ERROR = "constructor-error";

  @VisibleForTesting public static final String INJECTOR_TRY_BEGINNING_ERROR = "run-try-beginning";

  @VisibleForTesting public static final String INJECTOR_TRY_END_ERROR = "run-try-end";

  @VisibleForTesting public static final String INJECTOR_PENDING_ERROR = "pending-error";

  @VisibleForTesting public static final String INJECTOR_PENDING_PAUSE = "pending-pause";

  @VisibleForTesting public static final String INJECTOR_PLAN_PAUSE = "plan-pause";

  @VisibleForTesting public static final String INJECTOR_PLAN_ERROR = "plan-error";

  @VisibleForTesting public static final String INJECTOR_TAIL_PROFLE_ERROR = "tail-profile-error";

  @VisibleForTesting
  public static final String INJECTOR_METADATA_RETRIEVAL_PAUSE = "metadata-retrieval-pause";

  @VisibleForTesting
  public static final String INJECTOR_DURING_PLANNING_PAUSE = "during-planning-pause";

  @VisibleForTesting public static final String INJECTOR_COMMIT_FAILURE = "commit-failure";

  @VisibleForTesting public static final String INJECTOR_CLEANING_FAILURE = "cleaning-failure";

  @VisibleForTesting
  public static final String INJECTOR_RESOURCE_ALLOCATION_EXCEPTION =
      "resource-allocation-exception";

  public static final String EXECUTION_COMPLETE_TO_QUERY_COMPLETE_TIME_LABEL =
      "execution_complete_to_query_complete_time";
  public static final String LAST_NODE_COMPLETE_TO_QUERY_COMPLETE_TIME_LABEL =
      "last_node_complete_to_query_complete_time";
  public static final String SCREEN_COMPLETE_TO_LAST_NODE_COMPLETE_TIME_LABEL =
      "screen_complete_to_last_node_complete_time";
  public static final String EXEC_TO_COORD_SCREEN_COMPLETE_TIME_LABEL =
      "exec_to_coord_screen_complete_time";
  private static final SimpleUpdatableTimer EXECUTION_COMPLETE_TO_QUERY_COMPLETE_TIMER =
      SimpleUpdatableTimer.of(EXECUTION_COMPLETE_TO_QUERY_COMPLETE_TIME_LABEL);

  private static final SimpleUpdatableTimer LAST_NODE_COMPLETE_TO_QUERY_COMPLETE_TIMER =
      SimpleUpdatableTimer.of(LAST_NODE_COMPLETE_TO_QUERY_COMPLETE_TIME_LABEL);

  private static final SimpleUpdatableTimer SCREEN_COMPLETE_TO_LAST_NODE_COMPLETE_TIMER =
      SimpleUpdatableTimer.of(SCREEN_COMPLETE_TO_LAST_NODE_COMPLETE_TIME_LABEL);

  private static final SimpleUpdatableTimer EXEC_TO_COORD_SCREEN_COMPLETE_TIMER =
      SimpleUpdatableTimer.of(EXEC_TO_COORD_SCREEN_COMPLETE_TIME_LABEL);

  private final AttemptId attemptId;
  private final AttemptReason attemptReason;
  private final QueryId queryId;
  private RuleBasedEngineSelector ruleBasedEngineSelector;
  private final String queryIdString;
  private final UserRequest queryRequest;
  private final QueryContext queryContext;
  private final SabotContext sabotContext;
  private final MaestroService maestroService;
  private final JobTelemetryClient jobTelemetryClient;
  private final Cache<Long, PreparedPlan> preparedPlans;
  private volatile QueryState state;
  private volatile boolean clientCancelled;

  private final StateSwitch stateSwitch = new StateSwitch();
  private final AttemptResult foremanResult = new AttemptResult();
  private Object extraResultData;
  private final AttemptProfileTracker profileTracker;
  private final Pointer<QueryId> prepareId;
  private final CommandPool commandPool;
  // Since there are two threads (REST api thread and foreman) vying, make this volatile. It is
  // possible that
  // the move to currentExecutionStage can race with incoming cancel REST API request. Due to this
  // whenever execution
  // stage is moved to the next stage by foreman, the cancel flag is checked again and cancel
  // exception thrown.
  // See clientCancelled flag above.
  private volatile AttemptEvent.State currentExecutionStage;
  private CommandRunner<?> command;
  private Optional<Runnable> committer = Optional.empty();
  private Optional<Runnable> queryCleaner = Optional.empty();

  // set to true, if the query failed due to an engine timeout
  private boolean timedoutWaitingForEngine = false;
  private boolean runTimeExceeded = false;

  /** if set to true, query is not going to be scheduled on a separate thread */
  private final boolean runInSameThread;

  private final MeterProvider<Counter> jobsFailedCounter;

  /**
   * Constructor. Sets up the AttemptManager, but does not initiate any execution.
   *
   * @param attemptId the id for the query
   * @param queryRequest the query to execute
   */
  public AttemptManager(
      final SabotContext sabotContext,
      final AttemptId attemptId,
      final AttemptReason attemptReason,
      final UserRequest queryRequest,
      final AttemptObserver attemptObserver,
      final OptionProvider options,
      final Cache<Long, PreparedPlan> preparedPlans,
      final QueryContext queryContext,
      final CommandPool commandPool,
      final MaestroService maestroService,
      final JobTelemetryClient jobTelemetryClient,
      final RuleBasedEngineSelector ruleBasedEngineSelector,
      final boolean runInSameThread) {
    this.sabotContext = sabotContext;
    this.attemptId = attemptId;
    this.attemptReason = attemptReason;
    this.queryId = attemptId.toQueryId();
    this.ruleBasedEngineSelector = ruleBasedEngineSelector;
    this.queryIdString = QueryIdHelper.getQueryId(queryId);
    this.queryRequest = queryRequest;
    this.preparedPlans = preparedPlans;
    this.queryContext = queryContext;
    this.commandPool = commandPool;
    this.maestroService = maestroService;
    this.runInSameThread = runInSameThread;
    this.currentExecutionStage = AttemptEvent.State.INVALID_STATE;
    this.jobTelemetryClient = jobTelemetryClient;

    prepareId = new Pointer<>();
    final OptionManager optionManager = this.queryContext.getOptions();
    if (options != null) {
      options.applyOptions(optionManager);
    }
    profileTracker =
        new AttemptProfileTracker(
            queryId,
            queryContext,
            queryRequest.getDescription(),
            () -> state,
            attemptObserver,
            jobTelemetryClient);

    JOBS_TOTAL
        .withTags(
            PlannerMetrics.WORKLOAD_TYPE_KEY,
            queryContext.getWorkloadType().name(),
            PlannerMetrics.USER_TYPE_KEY,
            PlannerMetrics.getUserKindLabel(queryContext.getQueryUserName()))
        .increment();

    jobsFailedCounter =
        newCounterProvider(
            PlannerMetrics.createName(PlannerMetrics.PREFIX_JOBS, PlannerMetrics.JOB_FAILED),
            "Number of failed jobs categorized by failed types and origin component of the error");
    recordNewState(QueryState.ENQUEUED);
    injector.injectUnchecked(queryContext.getExecutionControls(), INJECTOR_CONSTRUCTOR_ERROR);
  }

  protected AttemptProfileTracker getProfileTracker() {
    return this.profileTracker;
  }

  protected AttemptObservers getObserver() {
    return getProfileTracker().getObserver();
  }

  @Override
  public void moveToNextStage(AttemptEvent.State nextStage) {
    this.currentExecutionStage = nextStage;
    if (clientCancelled && !isTerminalStage(nextStage)) {
      // double check here if client has cancelled before we moved to next stage.
      // this will remove the window where the system is just entering a stage and therefor
      // fails to get interrupted from its wait states as it is just entering it.
      // for now, do this only when client has issued a cancel.
      throw new UserCancellationException(getProfileTracker().getCancelReason());
    }
  }

  private class CompletionListenerImpl implements CompletionListener {

    @Override
    public void succeeded(
        long screenOperatorCompletionTime,
        long screenCompletionRpcReceivedAt,
        long lastNodeCompletionRpcReceivedAt,
        long lastNodeCompletionRpcStartedAt) {
      getProfileTracker()
          .markExecutionTime(
              screenOperatorCompletionTime,
              screenCompletionRpcReceivedAt,
              lastNodeCompletionRpcReceivedAt,
              lastNodeCompletionRpcStartedAt);
      addToEventQueue(QueryState.COMPLETED, null);
    }

    @Override
    public void failed(Exception ex) {
      addToEventQueue(QueryState.FAILED, ex);
    }
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public QueryState getState() {
    return state;
  }

  public void dataFromScreenArrived(QueryData header, ResponseSender sender, ByteBuf... data) {
    if (data != null && data.length > 0 && Arrays.stream(data).filter(d -> d != null).count() > 0) {
      // we're going to send this some place, we need increment to ensure this is around long enough
      // to send.
      Arrays.stream(data).filter(d -> d != null).forEach(d -> d.retain());
      getObserver()
          .execDataArrived(new ScreenShuttle(sender), new QueryWritableBatch(header, data));
    } else {
      getObserver().execDataArrived(new ScreenShuttle(sender), new QueryWritableBatch(header));
    }
  }

  /**
   * Shuttles acknowledgments back to screen fragment as user acks messages. Fragments therefore
   * cannot complete until the acknowledgement is shuttled back to the sending node.
   */
  private class ScreenShuttle implements RpcOutcomeListener<Ack> {
    private final ResponseSender sender;
    private AtomicBoolean acked = new AtomicBoolean(false);

    public ScreenShuttle(ResponseSender sender) {
      this.sender = sender;
    }

    @Override
    public void failed(RpcException paramRpcException) {
      ackit();
      addToEventQueue(QueryState.FAILED, paramRpcException);
    }

    @Override
    public void interrupted(InterruptedException paramInterruptedException) {
      ackit();
      logger.info("Connection interrupted for query {}", queryIdString);
      addToEventQueue(QueryState.CANCELED, paramInterruptedException);
    }

    @Override
    public void dataOnWireCallback() {
      // Sending early ack as soon as packet is on wire.
      // The difference between ack here vs ack in success callback is
      // success callback is called when packet is received by
      // the destination.

      if (queryContext.getOptions().getOption(ExecConstants.EARLY_ACK_ENABLED)) {
        ackit();
      } else {
        logger.trace("Early ack is disabled");
      }
    }

    @Override
    public void success(Ack paramV, ByteBuf paramByteBuf) {
      ackit();
    }

    private void ackit() {
      // No matter what, we ack back to the original node. Since we manage
      // execution, we can cancel on failure from here. We don't ack until we've
      // passed data through, ensuring that we have full backpressure.

      if (acked.compareAndSet(false, true)) {
        sender.send(new Response(RpcType.ACK, Acks.OK));
      }
    }
  }

  /**
   * Get the latest full profile for the query.
   *
   * @return profile
   */
  public QueryProfile getQueryProfile() {
    return getProfileTracker().getFullProfile();
  }

  /**
   * Send the planning profile for the query to JobTelemetryService.
   *
   * @return future
   */
  public ListenableFuture<Empty> sendPlanningProfile() {
    return getProfileTracker().sendPlanningProfile();
  }

  /**
   * Cancel the query. Asynchronous -- it may take some time for all remote fragments to be
   * terminated.
   *
   * @param reason description of the cancellation
   * @param clientCancelled true if the client application explicitly issued a cancellation (via end
   *     user action), or false otherwise (i.e. when pushing the cancellation notification to the
   *     end user)
   */
  public void cancel(
      String reason,
      boolean clientCancelled,
      String cancelContext,
      boolean isCancelledByHeapMonitor,
      boolean runTimeExceeded,
      boolean connectionClosed) {
    // Note this can be called from outside of run() on another thread, or after run() completes
    getProfileTracker().setCancelReason(reason);
    this.clientCancelled = clientCancelled;
    this.runTimeExceeded = runTimeExceeded;
    // Set the cancelFlag, so that query in planning phase will be canceled
    // by super.checkCancel() in DremioVolcanoPlanner and DremioHepPlanner
    queryContext
        .getPlannerSettings()
        .cancelPlanning(
            reason, queryContext.getCurrentEndpoint(), cancelContext, isCancelledByHeapMonitor);
    // Do not cancel queries in running or queued state when canceled by coordinator heap monitor
    if (!isCancelledByHeapMonitor) {
      // interrupt execution immediately if query is blocked in any of the stages where it can get
      // blocked.
      // For instance, in ENGINE START stage, query could be blocked for engine to start or in
      // QUEUED stage,
      // the query could be blocked on a slot to become available when number of concurrent queries
      // exceeds number of
      // available slots (max concurrency).
      maestroService.interruptExecutionInWaitStates(queryId, currentExecutionStage);
      // Put the cancel in the event queue:
      // Note: Since the event processor only processes events after the attempt manager has
      // completed all coordinator
      // stages (including maestro's executeQuery), it is assumed that the interruptions done above
      // will make the
      // meastro end the executeQuery prematurely so that the state machine gets started and all
      // pending events
      // including this cancel gets processed.
      addToEventQueue(QueryState.CANCELED, null);
    }
    try {
      String cancelType = PlannerMetrics.CANCEL_UNCLASSIFIED;
      if (isCancelledByHeapMonitor) {
        cancelType = PlannerMetrics.COORDINATOR_CANCEL_HEAP_MONITOR;
      } else if (runTimeExceeded) {
        cancelType = PlannerMetrics.CANCEL_EXECUTION_RUNTIME_EXCEEDED;
      } else if (clientCancelled) {
        cancelType = PlannerMetrics.CANCEL_USER_INITIATED;
      } else if (connectionClosed) {
        cancelType = PlannerMetrics.CANCEL_CONNECTION_CLOSED;
      } else {
        // Preserve cancelType - CANCEL_UNCLASSIFIED
        logger.error(
            "Query canceled with {} has reason {}", PlannerMetrics.CANCEL_UNCLASSIFIED, reason);
      }
      jobsFailedCounter
          .withTags(
              PlannerMetrics.ERROR_TYPE_KEY,
              cancelType,
              PlannerMetrics.ERROR_ORIGIN_KEY,
              ClusterCoordinator.Role.COORDINATOR.name(),
              PlannerMetrics.WORKLOAD_TYPE_KEY,
              queryContext.getWorkloadType().name(),
              PlannerMetrics.USER_TYPE_KEY,
              PlannerMetrics.getUserKindLabel(queryContext.getQueryUserName()))
          .increment();
    } catch (Exception e) {
      logger.error("Error while incrementing the jobsFailedCounter", e);
    }
  }

  /*
   * Cancel the query just by changing the state of the query.
   * No need to send the cancel request to remote fragments because this resource error happens before
   * the executor is started or fragments are sent to the executors.
   */
  public void cancelLocal(String reason, Throwable e) {
    getProfileTracker().setCancelReason(reason + e.getMessage());
    moveToState(QueryState.CANCELED, null);
    jobsFailedCounter
        .withTags(
            PlannerMetrics.ERROR_TYPE_KEY,
            PlannerMetrics.CANCEL_RESOURCE_UNAVAILABLE,
            PlannerMetrics.ERROR_ORIGIN_KEY,
            ClusterCoordinator.Role.COORDINATOR.name(),
            PlannerMetrics.WORKLOAD_TYPE_KEY,
            queryContext.getWorkloadType().name(),
            PlannerMetrics.USER_TYPE_KEY,
            PlannerMetrics.getUserKindLabel(queryContext.getQueryUserName()))
        .increment();
  }

  public boolean canCancelByHeapMonitor() {
    return state == QueryState.ENQUEUED
        || state == QueryState.STARTING
            && command != null
            && command.getCommandType() != CommandType.ASYNC_QUERY;
  }

  /** Resume a paused query */
  public void resume() {
    queryContext.getExecutionControls().unpauseAll();
  }

  /**
   * Checks for required privileges on the Engine before running query.
   *
   * @param groupResourceInformation
   * @throws UserException
   */
  protected void checkRunQueryAccessPrivilege(GroupResourceInformation groupResourceInformation)
      throws UserException {
    return;
  }

  @Override
  public void run() {
    // rename the thread we're using for debugging purposes
    final Thread currentThread = Thread.currentThread();
    final String originalName = currentThread.getName();
    currentThread.setName(queryIdString + ":foreman");
    final MaestroObserverWrapper maestroObserver = new MaestroObserverWrapper(getObserver(), this);

    try {
      try {
        injector.injectChecked(
            queryContext.getExecutionControls(),
            INJECTOR_TRY_BEGINNING_ERROR,
            ForemanException.class);

        getObserver()
            .queryStarted(queryRequest, queryContext.getSession().getCredentials().getUserName());
        // Attempt submission time for first attempt should be same as Job submission time
        // For subsequent attempt submission time should be current time
        long attemptSubmissionTime =
            attemptId.getAttemptNum() == 0
                ? queryRequest.getJobSubmissionTime()
                : System.currentTimeMillis();
        getProfileTracker().markStartTime(attemptSubmissionTime);
        maestroObserver.beginState(
            AttemptObserver.toEvent(AttemptEvent.State.PENDING, attemptSubmissionTime));
        try {
          // planning is done in the command pool
          commandPool
              .submit(
                  CommandPool.Priority.MEDIUM,
                  attemptId.toString() + ":foreman-planning",
                  "foreman-planning",
                  (waitInMillis) -> {
                    getObserver().commandPoolWait(waitInMillis);

                    injector.injectPause(
                        queryContext.getExecutionControls(), INJECTOR_PENDING_PAUSE, logger);
                    injector.injectChecked(
                        queryContext.getExecutionControls(),
                        INJECTOR_PENDING_ERROR,
                        ForemanException.class);

                    plan();
                    injector.injectPause(
                        queryContext.getExecutionControls(), INJECTOR_PLAN_PAUSE, logger);
                    injector.injectChecked(
                        queryContext.getExecutionControls(),
                        INJECTOR_PLAN_ERROR,
                        ForemanException.class);
                    return null;
                  },
                  runInSameThread)
              .get();
        } catch (UserException ue) {
          ue.addErrorOrigin(ClusterCoordinator.Role.COORDINATOR.name());
          throw ue;
        }
        if (command.getCommandType() == CommandType.ASYNC_QUERY) {
          AsyncCommand asyncCommand = (AsyncCommand) command;
          committer = asyncCommand.getPhysicalPlan().getCommitter();
          queryCleaner = asyncCommand.getPhysicalPlan().getCleaner();

          moveToState(QueryState.STARTING, null);
          maestroService.executeQuery(
              queryId,
              queryContext,
              asyncCommand.getPhysicalPlan(),
              runInSameThread,
              maestroObserver,
              new CompletionListenerImpl());
          asyncCommand.executionStarted();
        }

        maestroObserver.beginState(AttemptObserver.toEvent(AttemptEvent.State.RUNNING));
        moveToState(QueryState.RUNNING, null);

        injector.injectChecked(
            queryContext.getExecutionControls(), INJECTOR_TRY_END_ERROR, ForemanException.class);
      } catch (ExecutionException ee) {
        if (ee.getCause() != null) {
          throw ee.getCause();
        } else {
          throw ee;
        }
      }
    } catch (ResourceUnavailableException e) {
      timedoutWaitingForEngine = true;
      // resource allocation failure is treated as a cancellation and not a failure
      try {
        // the caller (JobEventCollatingObserver) expects metadata event before a cancel/complete
        // event.
        getObserver().planCompleted(null, null);
      } catch (Exception ignore) {
      }
      cancelLocal("Resource Unavailable, ", e); // ENQUEUED/STARTING -> CANCELED transition
    } catch (ResourceAllocationException e) {
      UserException ue = UserException.resourceError(e).message(e.getMessage()).build(logger);
      moveToState(QueryState.FAILED, ue);
    } catch (final UserException | ForemanException e) {
      moveToState(QueryState.FAILED, e);
    } catch (final OutOfMemoryError e) {
      if (ErrorHelper.isDirectMemoryException(e)) {
        moveToState(
            QueryState.FAILED,
            UserException.memoryError(e)
                .setAdditionalExceptionContext(
                    new OutOfMemoryOrResourceExceptionContext(
                        OutOfMemoryOrResourceExceptionContext.MemoryType.DIRECT_MEMORY, null))
                .addErrorOrigin(ClusterCoordinator.Role.COORDINATOR.name())
                .build(logger));
      } else {
        /*
         * FragmentExecutors use a NodeStatusListener to watch out for the death of their query's AttemptManager. So, if we
         * die here, they should get notified about that, and cancel themselves; we don't have to attempt to notify
         * them, which might not work under these conditions.
         */
        ProcessExit.exitHeap(e);
      }
    } catch (Throwable ex) {
      UserCancellationException t =
          ErrorHelper.findWrappedCause(ex, UserCancellationException.class);
      if (t != null) {
        moveToState(QueryState.CANCELED, null);
      } else {
        UserException uex = ErrorHelper.findWrappedCause(ex, UserException.class);
        if (uex != null) {
          moveToState(QueryState.FAILED, uex);
        } else {
          String errorMsg = "Unexpected exception during fragment initialization: ";
          ForemanException fe = new ForemanException(errorMsg, ex);
          logger.error(errorMsg, fe);
          moveToState(QueryState.FAILED, fe);
        }
      }

    } finally {
      /*
       * Begin accepting external events.
       *
       * Doing this here in the finally clause will guarantee that it occurs. Otherwise, if there
       * is an exception anywhere during setup, it wouldn't occur, and any events that are generated
       * as a result of any partial setup that was done (such as the FragmentSubmitListener,
       * the ResponseSendListener, or an external call to cancel()), will hang the thread that makes the
       * event delivery call.
       *
       * If we do throw an exception during setup, and have already moved to QueryState.FAILED, we just need to
       * make sure that we can't make things any worse as those events are delivered, but allow
       * any necessary remaining cleanup to proceed.
       *
       * Note that cancellations cannot be simulated before this point, i.e. pauses can be injected, because AttemptManager
       * would wait on the cancelling thread to signal a resume and the cancelling thread would wait on the AttemptManager
       * to accept events.
       */
      try {
        stateSwitch.start();
      } catch (Exception e) {
        moveToState(QueryState.FAILED, e);
      }

      // restore the thread's original name
      currentThread.setName(originalName);
    }

    /*
     * Note that despite the run() completing, the AttemptManager could continue to exist, and receives
     * events about fragment completions. It won't go away until everything is completed, failed, or cancelled.
     */
  }

  private void plan() throws Exception {
    // query parsing and dataset retrieval (both from source and kvstore).
    getObserver().beginState(AttemptObserver.toEvent(AttemptEvent.State.METADATA_RETRIEVAL));
    moveToNextStage(AttemptEvent.State.METADATA_RETRIEVAL);

    CommandCreator creator = newCommandCreator(getObserver(), prepareId);
    command = creator.toCommand();
    logger.debug("Using command: {}.", command);

    final Stopwatch stopwatch = Stopwatch.createStarted();
    String ruleSetEngine = ruleBasedEngineSelector.resolveAndUpdateEngine(queryContext);
    ResourceSchedulingProperties resourceSchedulingProperties = new ResourceSchedulingProperties();
    resourceSchedulingProperties.setRoutingEngine(queryContext.getSession().getRoutingEngine());
    resourceSchedulingProperties.setRuleSetEngine(ruleSetEngine);
    final GroupResourceInformation groupResourceInformation =
        maestroService.getGroupResourceInformation(
            queryContext.getOptions(), resourceSchedulingProperties);

    /* throwing a ResourceAllocationException here to test executor engines disabled or deleted scenario. This exception
     * can be potentially thrown by the maestroService.getGroupResourceInformation call above */
    injector.injectChecked(
        queryContext.getExecutionControls(),
        INJECTOR_RESOURCE_ALLOCATION_EXCEPTION,
        ResourceAllocationException.class);
    queryContext.setGroupResourceInformation(groupResourceInformation);
    getObserver()
        .resourcesPlanned(groupResourceInformation, stopwatch.elapsed(TimeUnit.MILLISECONDS));

    // Checks for Run Query privileges for the selected Engine
    checkRunQueryAccessPrivilege(groupResourceInformation);

    injector.injectPause(
        queryContext.getExecutionControls(), INJECTOR_METADATA_RETRIEVAL_PAUSE, logger);

    switch (command.getCommandType()) {
      case ASYNC_QUERY:
        Preconditions.checkState(
            command instanceof AsyncCommand, "Asynchronous query must be an AsyncCommand");
        command.plan();
        break;

      case SYNC_QUERY:
      case SYNC_RESPONSE:
        moveToState(QueryState.STARTING, null);
        command.plan();
        extraResultData = command.execute();
        addToEventQueue(QueryState.COMPLETED, null);
        break;

      default:
        throw new IllegalStateException(
            String.format("command type %s not supported in plan()", command.getCommandType()));
    }
    getProfileTracker().setPrepareId(prepareId.value);
  }

  protected CommandCreator newCommandCreator(
      AttemptObservers observer, Pointer<QueryId> prepareId) {
    return new CommandCreator(
        this.sabotContext,
        this.queryContext,
        queryRequest,
        observer,
        preparedPlans,
        prepareId,
        attemptId.getAttemptNum(),
        attemptReason);
  }

  /**
   * Manages the end-state processing for AttemptManager.
   *
   * <p>End-state processing is tricky, because even if a query appears to succeed, but we then
   * encounter a problem during cleanup, we still want to mark the query as failed. So we have to
   * construct the successful result we would send, and then clean up before we send that result,
   * possibly changing that result if we encounter a problem during cleanup. We only send the result
   * when there is nothing left to do, so it will account for any possible problems.
   *
   * <p>The idea here is to make close()ing the ForemanResult do the final cleanup and sending.
   * Closing the result must be the last thing that is done by AttemptManager.
   */
  private class AttemptResult implements AutoCloseable {
    private QueryState resultState = null;
    private volatile Exception resultException = null;
    private boolean isClosed = false;

    /**
     * Set up the result for a COMPLETED or CANCELED state.
     *
     * <p>Note that before sending this result, we execute cleanup steps that could result in this
     * result still being changed to a FAILED state.
     *
     * @param queryState one of COMPLETED or CANCELED
     */
    public void setCompleted(final QueryState queryState) {
      Preconditions.checkArgument(
          (queryState == QueryState.COMPLETED) || (queryState == QueryState.CANCELED));
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState == null);

      resultState = queryState;
    }

    /**
     * Set up the result for a FAILED state.
     *
     * <p>Failures that occur during cleanup processing will be added as suppressed exceptions.
     *
     * @param exception the exception that led to the FAILED state
     */
    public void setFailed(final Exception exception) {
      Preconditions.checkArgument(exception != null);
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState == null);
      try {
        // increment server error only if it's not a known client error
        UserException ue = ErrorHelper.findWrappedCause(exception, UserException.class);
        if (ue != null) {
          String errorOrigin;
          String errorTypeStr;
          ClusterCoordinator.Role role = null;
          DremioPBError.ErrorType errorType = ue.getErrorType();
          errorTypeStr = errorType.name();
          errorOrigin = ue.getErrorOrigin();
          if (errorOrigin == null) {
            logger.warn(
                "Unable to get the error origin for error Classification for the error type {} with message \" {} \"",
                ue.getErrorType().name(),
                ue.getMessage());
            errorOrigin = UserException.UNCLASSIFIED_ERROR_ORIGIN;
          } else {
            try {
              role = Enum.valueOf(ClusterCoordinator.Role.class, errorOrigin);
            } catch (IllegalArgumentException e) {
              logger.warn("Unable to get the role for error Classification ", e);
            }
          }
          if (errorType == DremioPBError.ErrorType.OUT_OF_MEMORY) {
            OutOfMemoryOrResourceExceptionContext oomExceptionContext =
                OutOfMemoryOrResourceExceptionContext.fromUserException(ue);
            if (oomExceptionContext != null) {
              switch (oomExceptionContext.getMemoryType()) {
                case DIRECT_MEMORY:
                  errorTypeStr = PlannerMetrics.CANCEL_DIRECT_MEMORY_EXCEEDED;
                  if (role != null) {
                    if (role == ClusterCoordinator.Role.EXECUTOR) {
                      errorTypeStr = PlannerMetrics.EXECUTOR_CANCEL_DIRECT_MEMORY_EXCEEDED;
                    } else {
                      errorTypeStr = PlannerMetrics.COORDINATOR_CANCEL_DIRECT_MEMORY_EXCEEDED;
                    }
                  }
                  break;
                case HEAP_MEMORY:
                  errorTypeStr = PlannerMetrics.CANCEL_HEAP_MONITOR;
                  if (role != null) {
                    if (role == ClusterCoordinator.Role.EXECUTOR) {
                      errorTypeStr = PlannerMetrics.EXECUTOR_CANCEL_HEAP_MONITOR;
                    } else {
                      errorTypeStr = PlannerMetrics.COORDINATOR_CANCEL_HEAP_MONITOR;
                    }
                  }
                  break;
                default:
                  break;
              }
            }
            jobsFailedCounter
                .withTags(
                    PlannerMetrics.ERROR_TYPE_KEY,
                    errorTypeStr,
                    PlannerMetrics.ERROR_ORIGIN_KEY,
                    errorOrigin,
                    PlannerMetrics.WORKLOAD_TYPE_KEY,
                    queryContext.getWorkloadType().name(),
                    PlannerMetrics.USER_TYPE_KEY,
                    PlannerMetrics.getUserKindLabel(queryContext.getQueryUserName()))
                .increment();
          } else {
            jobsFailedCounter
                .withTags(
                    PlannerMetrics.ERROR_TYPE_KEY,
                    errorType.name(),
                    PlannerMetrics.ERROR_ORIGIN_KEY,
                    errorOrigin,
                    PlannerMetrics.WORKLOAD_TYPE_KEY,
                    queryContext.getWorkloadType().name(),
                    PlannerMetrics.USER_TYPE_KEY,
                    PlannerMetrics.getUserKindLabel(queryContext.getQueryUserName()))
                .increment();
          }
          if (!CLIENT_ERRORS.contains(errorType)) {
            SERVER_ERROR.increment();
          }
        } else {
          jobsFailedCounter
              .withTags(
                  PlannerMetrics.ERROR_TYPE_KEY,
                  PlannerMetrics.UNKNOWN_ERROR_TYPE,
                  PlannerMetrics.ERROR_ORIGIN_KEY,
                  UserException.UNCLASSIFIED_ERROR_ORIGIN,
                  PlannerMetrics.WORKLOAD_TYPE_KEY,
                  queryContext.getWorkloadType().name(),
                  PlannerMetrics.USER_TYPE_KEY,
                  PlannerMetrics.getUserKindLabel(queryContext.getQueryUserName()))
              .increment();
          String exceptionDetails =
              String.format(
                  "Main: %s, Cause: %s",
                  exception.getClass(),
                  (exception.getCause() != null ? exception.getCause().getClass() : "None"));
          logger.error(
              "Query failed with {} has exception of type {}",
              PlannerMetrics.UNKNOWN_ERROR_TYPE,
              exceptionDetails);
          SERVER_ERROR.increment();
        }
      } catch (Exception e) {
        logger.warn("Error while incrementing the jobsFailedCounter", e);
      } finally {
        resultState = QueryState.FAILED;
        resultException = exception;
      }
    }

    /** Ignore the current status and force the given failure as current status. */
    public void setForceFailure(final Exception exception) {
      Preconditions.checkArgument(exception != null);
      Preconditions.checkState(!isClosed);

      resultState = QueryState.FAILED;
      resultException = exception;
    }

    /**
     * Add an exception to the result. All exceptions after the first become suppressed exceptions
     * hanging off the first.
     *
     * @param exception the exception to add
     */
    private void addException(final Exception exception) {
      Preconditions.checkNotNull(exception);

      if (resultException == null) {
        resultException = exception;
      } else {
        resultException.addSuppressed(exception);
      }
    }

    /**
     * Close the given resource, catching and adding any caught exceptions via {@link
     * #addException(Exception)}. If an exception is caught, it will change the result state to
     * FAILED, regardless of what its current value.
     *
     * @param autoCloseable the resource to close
     */
    private void suppressingClose(final AutoCloseable autoCloseable) {
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState != null);

      if (autoCloseable == null) {
        return;
      }

      try {
        autoCloseable.close();
      } catch (final Exception e) {
        /*
         * Even if the query completed successfully, we'll still report failure if we have
         * problems cleaning up.
         */
        resultState = QueryState.FAILED;
        addException(e);
      }
    }

    @Override
    @WithSpan("attempt-result-cleanup")
    public void close() {
      if (isClosed) {
        // This can happen if the AttemptManager closes the result first (on error), and later,
        // receives the completion
        // callback from maestro.
        return;
      }

      Preconditions.checkState(resultState != null);
      final Thread currentThread = Thread.currentThread();
      final String originalName = currentThread.getName();
      try {
        // rename the thread we're using for debugging purposes
        currentThread.setName(queryIdString + ":foreman");

        try {
          injector.injectChecked(
              queryContext.getExecutionControls(),
              INJECTOR_CLEANING_FAILURE,
              UnsupportedOperationException.class);

          if (resultState == QueryState.CANCELED || resultState == QueryState.FAILED) {
            Context.current().fork().run(() -> queryCleaner.ifPresent(Runnable::run));
          }
        } catch (Exception e) {
          addException(e);
          logger.warn("Exception during cleaning after attempt completion", resultException);
          recordNewState(QueryState.FAILED);
          foremanResult.setForceFailure(e);
        }

        // to track how long the query takes
        long endTime = System.currentTimeMillis();
        getProfileTracker().markEndTime(endTime);

        if (queryRequest.getDescription() != null) {
          SimpleDistributionSummary.of("jobs.long_running", queryRequest.getDescription())
              .recordAmount(getProfileTracker().getTime());
        }
        sendFinalQueryTimeMetrics();

        logger.debug(queryIdString + ": cleaning up.");
        injector.injectPause(queryContext.getExecutionControls(), "foreman-cleanup", logger);

        suppressingClose(queryContext);

        /*
         * We do our best to write the latest state, but even that could fail. If it does, we can't write
         * the (possibly newly failing) state, so we continue on anyway.
         *
         * We only need to do this if the resultState differs from the last recorded state
         */
        if (resultState != state) {
          recordNewState(resultState);
        }
        AttemptEvent.State terminalStage = convertTerminalToAttemptState(resultState);
        getObserver().beginState(AttemptObserver.toEvent(terminalStage, endTime));
        moveToNextStage(terminalStage);

        UserException uex;
        if (resultException != null) {
          uex = ErrorHelper.findWrappedCause(resultException, UserException.class);
          if (uex == null) {
            uex =
                UserException.systemError(resultException)
                    .addIdentity(queryContext.getCurrentEndpoint())
                    .build(logger);
          }
        } else {
          uex = null;
        }

        /*
         * If sending the result fails, we don't really have any way to modify the result we tried to send;
         * it is possible it got sent but the result came from a later part of the code path. It is also
         * possible the connection has gone away, so this is irrelevant because there's nowhere to
         * send anything to.
         */

        QueryProfile queryProfile = null;
        boolean sendTailProfileFailed = false;
        try {
          // send whatever result we ended up with
          injector.injectUnchecked(queryContext.getExecutionControls(), INJECTOR_TAIL_PROFLE_ERROR);
          queryProfile = getProfileTracker().sendTailProfile(uex);
        } catch (Exception e) {
          jobTelemetryClient
              .getSuppressedErrorCounter()
              .withTags(
                  MetricLabel.JTS_METRIC_TAG_KEY_RPC,
                  MetricLabel.JTS_METRIC_TAG_VALUE_RPC_SEND_TAIL_PROFILE,
                  MetricLabel.JTS_METRIC_TAG_KEY_ERROR_ORIGIN,
                  MetricLabel.JTS_METRIC_TAG_VALUE_ATTEMPT_CLOSE)
              .increment();
          logger.warn("Exception sending tail profile. ", e);
          sendTailProfileFailed = true;
          getObserver().putProfileFailed();
          // As tail profile cannot be retrieved, let us
          // get the planning profile to use in query result.
          queryProfile = profileTracker.getPlanningProfileNoException();
        }

        // Reflection queries are dependant on stats in Executor Profile, so fetching full profile
        // instead of just planning profile
        if (Utilities.isAccelerationType(queryContext.getWorkloadType())) {
          try {
            logger.debug("Fetching full profile for Acceleration type queries.");
            queryProfile = getProfileTracker().getFullProfile();

            // full profile from store will not have latest info when sendTailProfile fails, so
            // update with in-memory state
            if (sendTailProfileFailed) {
              QueryProfile.Builder profileBuilder = queryProfile.toBuilder();
              getProfileTracker().addLatestState(profileBuilder);
              queryProfile = profileBuilder.build();
            }
          } catch (Exception e) {
            logger.warn("Exception while getting full profile. ", e);
            // As full profile cannot be retrieved and as we are marking the query as failed, let us
            // get the planning profile
            // to use in query result.
            queryProfile = getProfileTracker().getPlanningProfileNoException();
          }
        }

        try {
          UserResult result =
              new UserResult(
                  extraResultData,
                  queryId,
                  resultState,
                  queryProfile,
                  uex,
                  getProfileTracker().getCancelReason(),
                  clientCancelled,
                  timedoutWaitingForEngine,
                  runTimeExceeded);
          getObserver().attemptCompletion(result);
        } catch (final Exception e) {
          addException(e);
          logger.warn("Exception sending result to client", resultException);
        }

        try {
          if (command != null) {
            command.close();
          }
        } catch (final Exception e) {
          logger.error("Exception while invoking 'close' on command {}", command, e);
        } finally {
          isClosed = true;
        }
      } finally {
        // restore the thread's original name
        currentThread.setName(originalName);
      }
    }
  }

  private static class StateEvent {
    final QueryState newState;
    final Exception exception;

    StateEvent(final QueryState newState, final Exception exception) {
      this.newState = newState;
      this.exception = exception;
    }
  }

  /**
   * Tells the foreman to move to a new state. Do not call it directly from external, all the state
   * changes should go through {@link #addToEventQueue(QueryState, Exception)} and they will be
   * synchronized as events to prevent unpredictable failure.
   *
   * @param newState the state to move to
   * @param exception if not null, the exception that drove this state transition (usually a
   *     failure)
   */
  @WithSpan
  private void moveToState(final QueryState newState, final Exception exception) {
    Span.current()
        .setAttribute("dremio.attempt_manager.query_id", queryIdString)
        .setAttribute("dremio.attempt_manager.current_query_state", state.toString())
        .setAttribute("dremio.attempt_manager.requested_query_state", newState.toString());
    if (exception == null) {
      logger.debug(queryIdString + ": State change requested {} --> {}", state, newState);
    } else {
      logger.info(
          queryIdString + ": State change requested {} --> {}, Exception {}",
          state,
          newState,
          exception.toString());
      Span.current().recordException(exception);
    }
    switch (state) {
      case ENQUEUED:
        switch (newState) {
          case FAILED:
            Preconditions.checkNotNull(
                exception, "exception cannot be null when new state is failed");
            recordNewState(newState);
            foremanResult.setFailed(exception);
            foremanResult.close();
            return;

          case STARTING:
            recordNewState(newState);
            return;

          case CANCELED:
            {
              assert exception == null;
              recordNewState(QueryState.CANCELED);
              foremanResult.setCompleted(QueryState.CANCELED);
              foremanResult.close();
              return;
            }
        }
        break;

      case STARTING:
        switch (newState) {
          case RUNNING:
            {
              recordNewState(QueryState.RUNNING);
              try {
                getObserver().execStarted(getProfileTracker().getPlanningProfile());
              } catch (UserCancellationException ucx) {
                // Ignore this exception here.
              } finally {
                try {
                  // Always send planning profile as soon as query move to RUNNING state
                  sendPlanningProfile();
                } catch (Exception e) {
                  logger.error("Failure while sending Planning Profile.", e);
                }
              }
              return;
            }

          case CANCELED:
            {
              assert exception == null;
              recordNewState(QueryState.CANCELED);
              try {
                maestroService.cancelQuery(queryId);
              } catch (Exception ex) {
                logger.error(
                    "Error observed calling maestroService.cancelQuery for query {}",
                    queryIdString,
                    ex);
                Span.current().recordException(ex);
              } finally {
                foremanResult.setCompleted(QueryState.CANCELED);
                foremanResult.close();
              }
              return;
            }
        }

        // $FALL-THROUGH$

      case RUNNING:
        {
          /*
           * For cases that cancel executing fragments, we have to record the new
           * state first, because the cancellation of the local root fragment will
           * cause this to be called recursively.
           */
          switch (newState) {
            case CANCELED:
              {
                assert exception == null;
                recordNewState(QueryState.CANCELED);
                try {
                  maestroService.cancelQuery(queryId);
                } catch (Exception ex) {
                  logger.error(
                      "Error observed calling maestroService.cancelQuery for query {}",
                      queryIdString,
                      ex);
                  Span.current().recordException(ex);
                } finally {
                  foremanResult.setCompleted(QueryState.CANCELED);
                }
                /*
                 * We don't close the foremanResult until we've gotten
                 * acknowledgements, which happens below in the case for current state
                 * == CANCELLATION_REQUESTED.
                 */
                return;
              }

            case COMPLETED:
              {
                assert exception == null;

                try {
                  injector.injectChecked(
                      queryContext.getExecutionControls(),
                      INJECTOR_COMMIT_FAILURE,
                      ForemanException.class);
                  // The commit handler can internally invoke other grpcs. So, forking the context
                  // here.
                  Context.current().fork().run(() -> committer.ifPresent(Runnable::run));
                } catch (ForemanException e) {
                  logger.error(
                      "Error running plan committer for query {}. Moving query state from RUNNING --> FAILED instead of RUNNING --> COMPLETED",
                      queryIdString,
                      e);
                  moveToState(QueryState.FAILED, e);
                  return;
                }

                recordNewState(QueryState.COMPLETED);
                foremanResult.setCompleted(QueryState.COMPLETED);
                foremanResult.close();
                return;
              }

            case FAILED:
              {
                assert exception != null;
                recordNewState(QueryState.FAILED);
                try {
                  maestroService.cancelQuery(queryId);
                } catch (Exception ex) {
                  logger.error(
                      "Error observed calling maestroService.cancelQuery for query {}",
                      queryIdString,
                      ex);
                  Span.current().recordException(ex);
                } finally {
                  foremanResult.setFailed(exception);
                  foremanResult.close();
                }
                return;
              }
          }
          break;
        }

      case CANCELED:
        if ((newState == QueryState.CANCELED)
            || (newState == QueryState.COMPLETED)
            || (newState == QueryState.FAILED)) {

          if (sabotContext
              .getConfig()
              .getBoolean(ExecConstants.RETURN_ERROR_FOR_FAILURE_IN_CANCELLED_FRAGMENTS)) {
            if (newState == QueryState.FAILED) {
              assert exception != null;
              recordNewState(QueryState.FAILED);
              foremanResult.setForceFailure(exception);
            }
          }
          /*
           * These amount to a completion of the cancellation requests' cleanup;
           * now we can clean up and send the result.
           */
          foremanResult.close();
        }
        return;

      case COMPLETED:
      case FAILED:
        logger.warn(
            "Dropping request to move to {} state as query {} is already at {} state (which is terminal).",
            newState,
            queryIdString,
            state);
        return;
    }

    IllegalStateException illegalStateException =
        new IllegalStateException(
            String.format(
                "Failure trying to change states: %s --> %s", state.name(), newState.name()));

    /* Set span status to ERROR to indicate query state transition has failed */
    Span.current().recordException(illegalStateException).setStatus(StatusCode.ERROR);
    throw illegalStateException;
  }

  private class StateSwitch extends EventProcessor<StateEvent> {

    void addEvent(final QueryState newState, final Exception exception) {
      sendEvent(new StateEvent(newState, exception));
    }

    @Override
    protected void processEvent(StateEvent event) {
      moveToState(event.newState, event.exception);
    }
  }

  void addToEventQueue(final QueryState newState, final Exception exception) {
    stateSwitch.addEvent(newState, exception);
  }

  @WithSpan("record-query-state-transition")
  private void recordNewState(final QueryState newState) {
    state = newState;
    Span.current()
        .setAttribute("dremio.attempt_manager.query_id", queryIdString)
        .setAttribute("dremio.attempt_manager.current_query_state", state.toString())
        .setAttribute("dremio.attempt_manager.new_query_state", newState.toString());
  }

  private boolean isTerminalStage(AttemptEvent.State stage) {
    switch (stage) {
      case CANCELED:
      case COMPLETED:
      case FAILED:
      case INVALID_STATE:
        return true;
      default:
        return false;
    }
  }

  private AttemptEvent.State convertTerminalToAttemptState(final QueryState state) {
    Preconditions.checkArgument(
        (state == QueryState.COMPLETED
            || state == QueryState.CANCELED
            || state == QueryState.FAILED));

    switch (state) {
      case COMPLETED:
        return AttemptEvent.State.COMPLETED;
      case CANCELED:
        return AttemptEvent.State.CANCELED;
      case FAILED:
        return AttemptEvent.State.FAILED;
      default:
        return AttemptEvent.State.INVALID_STATE;
    }
  }

  private void sendFinalQueryTimeMetrics() {
    long endTime = getProfileTracker().getEndTime();
    long screenOperatorCompletionTime = getProfileTracker().getScreenOperatorCompletionTime();
    long screenCompletionRpcReceivedAt = getProfileTracker().getScreenCompletionRpcReceivedAt();
    long lastNodeCompletionRpcStartedAt = getProfileTracker().getLastNodeCompletionRpcStartedAt();
    long lastNodeCompletionRpcReceivedAt = getProfileTracker().getLastNodeCompletionRpcReceivedAt();
    logger.debug("{} : endTime : {}", queryIdString, endTime);
    logger.debug(
        "{} : screenOperatorCompletionTime : {}", queryIdString, screenOperatorCompletionTime);
    logger.debug(
        "{} : screenCompletionRpcReceivedAt : {}", queryIdString, screenCompletionRpcReceivedAt);
    logger.debug(
        "{} : lastNodeCompletionRpcStartedAt : {}", queryIdString, lastNodeCompletionRpcStartedAt);
    logger.debug(
        "{} : lastNodeCompletionRpcReceivedAt : {}",
        queryIdString,
        lastNodeCompletionRpcReceivedAt);
    try {
      long executionCompleteToQueryCompleteTime =
          screenOperatorCompletionTime == 0
              ? endTime - lastNodeCompletionRpcStartedAt
              : endTime - screenOperatorCompletionTime;
      EXECUTION_COMPLETE_TO_QUERY_COMPLETE_TIMER.update(
          executionCompleteToQueryCompleteTime, TimeUnit.MILLISECONDS);
      logger.debug(
          "{} : endTime - lastNodeCompletionRpcStartedAt : {}",
          queryIdString,
          endTime - lastNodeCompletionRpcStartedAt);
      logger.debug(
          "{} : endTime - screenOperatorCompletionTime : {}",
          queryIdString,
          endTime - screenOperatorCompletionTime);
    } catch (Exception e) {
      logger.warn("Failure updating {} metric", EXECUTION_COMPLETE_TO_QUERY_COMPLETE_TIMER, e);
    }
    try {
      long lastNodeCompleteToQueryCompleteTime = endTime - lastNodeCompletionRpcReceivedAt;
      logger.debug(
          "{} : endTime - lastNodeCompletionRpcReceivedAt : {}",
          queryIdString,
          endTime - lastNodeCompletionRpcReceivedAt);
      LAST_NODE_COMPLETE_TO_QUERY_COMPLETE_TIMER.update(
          lastNodeCompleteToQueryCompleteTime, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      logger.warn("Failure updating {} metric", LAST_NODE_COMPLETE_TO_QUERY_COMPLETE_TIMER, e);
    }
    try {
      long screenCompleteToLastNodeCompleteTime =
          screenCompletionRpcReceivedAt == 0
              ? 0
              : lastNodeCompletionRpcReceivedAt - screenCompletionRpcReceivedAt;
      SCREEN_COMPLETE_TO_LAST_NODE_COMPLETE_TIMER.update(
          screenCompleteToLastNodeCompleteTime, TimeUnit.MILLISECONDS);
      logger.debug(
          "{} : lastNodeCompletionRpcReceivedAt - screenCompletionRpcReceivedAt : {}",
          queryIdString,
          lastNodeCompletionRpcReceivedAt - screenCompletionRpcReceivedAt);
    } catch (Exception e) {
      logger.warn("Failure updating {} metric", SCREEN_COMPLETE_TO_LAST_NODE_COMPLETE_TIMER, e);
    }
    try {
      long execToCoordScreenCompleteTime =
          screenOperatorCompletionTime == 0
              ? lastNodeCompletionRpcReceivedAt - lastNodeCompletionRpcStartedAt
              : screenCompletionRpcReceivedAt - screenOperatorCompletionTime;
      EXEC_TO_COORD_SCREEN_COMPLETE_TIMER.update(
          execToCoordScreenCompleteTime, TimeUnit.MILLISECONDS);
      logger.debug(
          "{} : lastNodeCompletionRpcReceivedAt - lastNodeCompletionRpcStartedAt : {}",
          queryIdString,
          lastNodeCompletionRpcReceivedAt - lastNodeCompletionRpcStartedAt);
      logger.debug(
          "{} : screenCompletionRpcReceivedAt - screenOperatorCompletionTime : {}",
          queryIdString,
          screenCompletionRpcReceivedAt - screenOperatorCompletionTime);
    } catch (Exception e) {
      logger.warn("Failure updating {} metric", EXEC_TO_COORD_SCREEN_COMPLETE_TIMER, e);
    }
  }
}
