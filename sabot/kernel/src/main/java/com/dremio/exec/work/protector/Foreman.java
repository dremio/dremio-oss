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
package com.dremio.exec.work.protector;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.exception.JsonFieldChangeExceptionContext;
import com.dremio.exec.exception.SchemaChangeExceptionContext;
import com.dremio.exec.maestro.MaestroService;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.DelegatingAttemptObserver;
import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.planner.physical.HashAggPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.handlers.commands.PreparedPlan;
import com.dremio.exec.proto.GeneralRPCProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.work.foreman.AttemptManager;
import com.dremio.exec.work.user.OptionProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.jobtelemetry.PutTailProfileRequest;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;

import io.netty.buffer.ByteBuf;

/**
 * Can re-run a query if needed/possible without the user noticing.
 *
 * Handles anything related to external queryId. An external queryId is a regular queryId but with it's
 * last byte set to 0. This byte is reserved for internal queries to store the mode (fast/protected) and
 * the attempt number
 *
 * Dremio only knows about this class if we are sending/receiving message to/from the client. Everything
 * else uses AttemptManager and QueryId, which is an internal queryId
 *
 * Keeps track of all the info we need to instantiate a new attemptManager
 */
public class Foreman {
  private static final Logger logger = LoggerFactory.getLogger(Foreman.class);

  // we need all these to start new attemptManager instances if we have to
  private final Executor executor; // unlimited thread pool
  private final CommandPool commandPool;
  private final SabotContext context;
  private final CompletionListener listener;
  private final UserSession session;
  private final UserRequest request;
  private final OptionProvider config;
  private final QueryObserver observer;
  private final ReAttemptHandler attemptHandler;
  private final Cache<Long, PreparedPlan> plans;
  protected final MaestroService maestroService;
  protected final JobTelemetryClient jobTelemetryClient;

  private AttemptId attemptId; // id of last attempt

  private volatile AttemptManager attemptManager; // last running query


  private volatile boolean canceled; // did the user cancel the query ?

  protected Foreman(
    final SabotContext context,
    final Executor executor,
    final CommandPool commandPool,
    final CompletionListener listener,
    final ExternalId externalId,
    final QueryObserver observer,
    final UserSession session,
    final UserRequest request,
    final OptionProvider config,
    final ReAttemptHandler attemptHandler,
    Cache<Long, PreparedPlan> plans,
    final MaestroService maestroService,
    final JobTelemetryClient jobTelemetryClient) {
    this.attemptId = AttemptId.of(externalId);
    this.executor = executor;
    this.commandPool = commandPool;
    this.context = context;
    this.listener = listener;
    this.session = session;
    this.request = request;
    this.config = config;
    this.observer = observer;
    this.attemptHandler = attemptHandler;
    this.plans = plans;
    this.maestroService = maestroService;
    this.jobTelemetryClient = jobTelemetryClient;
  }

  public void start() {
    try {
      newAttempt(AttemptReason.NONE, Predicates.alwaysTrue());
    } catch (Exception e) {
      listener.completed();
      throw e;
    }
  }

  private void newAttempt(AttemptReason reason, Predicate<DatasetConfig> datasetValidityChecker) {
    try {
      // we should ideally check if the query wasn't cancelled before starting a new attempt but this will over-complicate
      // things as the observer expects a query profile at completion and this may not be available if the cancellation
      // is too early
      final AttemptObserver attemptObserver = new Observer(observer.newAttempt(attemptId, reason));

      attemptHandler.newAttempt();

      OptionProvider optionProvider = config;
      if (reason != AttemptReason.NONE && attemptHandler.hasOOM()) {
        optionProvider = new LowMemOptionProvider(config);
      }

      attemptManager = newAttemptManager(context, attemptId, request, attemptObserver, session,
        optionProvider, plans, datasetValidityChecker, commandPool);

    } catch (Throwable t) {
      UserException uex = UserException.systemError(t).addContext("Failure while submitting the Query").build(logger);
      final QueryProfile.Builder profileBuilder = QueryProfile.newBuilder()
        .setStart(System.currentTimeMillis())
        .setId(attemptId.toQueryId())
        .setState(QueryState.FAILED)
        .setCommandPoolWaitMillis(0)
        .setQuery(request.getDescription())
        .setError(uex.getMessage())
        .setVerboseError(uex.getVerboseMessage(false))
        .setErrorId(uex.getErrorId())
        .setDremioVersion(DremioVersionInfo.getVersion())
        .setEnd(System.currentTimeMillis());
      try {
        jobTelemetryClient.getBlockingStub()
          .putQueryTailProfile(
            PutTailProfileRequest.newBuilder()
              .setQueryId(attemptId.toQueryId())
              .setProfile(profileBuilder.build())
              .build()
          );
      } catch (Exception telemetryEx) {
        uex.addSuppressed(telemetryEx);
      }

      final UserResult result = new UserResult(null, attemptId.toQueryId(), QueryState.FAILED,
        profileBuilder.build(), uex, null, false);
      observer.execCompletion(result);
      throw t;
    }

    if (request.runInSameThread()) {
      attemptManager.run();
    } else {
      executor.execute(attemptManager);
    }
  }

  protected AttemptManager newAttemptManager(SabotContext context, AttemptId attemptId, UserRequest queryRequest,
      AttemptObserver observer, UserSession session, OptionProvider options,
      Cache<Long, PreparedPlan> plans, Predicate<DatasetConfig> datasetValidityChecker,
      CommandPool commandPool) {
    final QueryContext queryContext = new QueryContext(session, context, attemptId.toQueryId(),
        queryRequest.getPriority(), queryRequest.getMaxAllocation(), datasetValidityChecker);
    return new AttemptManager(context, attemptId, queryRequest, observer, options, plans,
      queryContext, commandPool, maestroService, jobTelemetryClient,
      queryRequest.runInSameThread());
  }

  public void dataFromScreenArrived(QueryData header, ByteBuf data, ResponseSender sender) throws RpcException {
    final AttemptManager manager = attemptManager;
    if(manager == null){
      logger.warn("Dropping data from screen, no active attempt manager.");
      return;
    }

    manager.dataFromScreenArrived(header, data, sender);
  }

  private boolean recoverFromFailure(AttemptReason reason, Predicate<DatasetConfig> datasetValidityChecker) {
    // request a new attemptId
    attemptId = attemptId.nextAttempt();

    logger.info("{}: Starting new attempt because of {}", attemptId, reason);

    synchronized (this) {
      if (canceled) {
        return false; // no need to run a new attempt
      }
      newAttempt(reason, datasetValidityChecker);
    }

    return true;
  }

  public QueryState getState(){
    if(attemptManager == null){
      return null;
    }

    return attemptManager.getState();
  }

  /**
   * Get the currently active profile. Only returns value iff there is a current attempt and it is not in a terminal
   * state (COMPLETED or FAILED)
   *
   * @return QueryProfile.
   */
  public Optional<QueryProfile> getCurrentProfile() {
    if (attemptManager == null) {
      return Optional.empty();
    }

    QueryProfile profile = attemptManager.getQueryProfile();
    QueryState state = attemptManager.getState();

    if (state == QueryState.RUNNING || state == QueryState.STARTING || state == QueryState.ENQUEUED ||
        state == QueryState.CANCELED) {
      return Optional.of(profile);
    }

    return Optional.empty();
  }

  /**
   * Send the latest planning profile to JTS.
   * @return future.
   */
  public Optional<ListenableFuture<Empty>> sendPlanningProfile() {
    if (attemptManager == null) {
      return Optional.empty();
    }

    QueryState state = attemptManager.getState();
    if (state == QueryState.RUNNING || state == QueryState.STARTING ||
        state == QueryState.ENQUEUED || state == QueryState.CANCELED) {
      return Optional.of(attemptManager.sendPlanningProfile());
    }

    return Optional.empty();
  }

  public ExternalId getExternalId() {
    return attemptId.getExternalId();
  }

  public synchronized void cancel(String reason, boolean clientCancelled) {
    cancel(reason, clientCancelled, null, false);
  }

  public synchronized void cancel(String reason, boolean clientCancelled, String cancelContext,
                                  boolean isCancelledByHeapMonitor) {
    if (!canceled) {
      canceled = true;

      if (attemptManager != null) {
        attemptManager.cancel(reason, clientCancelled, cancelContext, isCancelledByHeapMonitor);
      }
    }
  }

  public synchronized void resume() {
    if (attemptManager != null) {
      attemptManager.resume();
    }
  }

  // checks if the plan (after physical transformation) contains hash aggregate
  private static boolean containsHashAggregate(final RelNode relNode) {
    if (relNode instanceof HashAggPrel) {
      return true;
    }
    else {
      for (final RelNode child : relNode.getInputs()) {
        if (containsHashAggregate(child)) {
          return true;
        } // else, continue
      }
    }
    return false;
  }

  private class Observer extends DelegatingAttemptObserver {

    private boolean isCTAS = false;
    private boolean containsHashAgg = false;

    Observer(final AttemptObserver delegate) {
      super(delegate);
    }

    @Override
    public void execDataArrived(RpcOutcomeListener<GeneralRPCProtos.Ack> outcomeListener, QueryWritableBatch result) {
      try {
        // any failures here should notify the listener and release the batches
        result = attemptHandler.convertIfNecessary(result);
      } catch (Exception ex) {
        outcomeListener.failed(RpcException.mapException(ex));
        for (ByteBuf byteBuf : result.getBuffers()) {
          byteBuf.release();
        }
        return;
      }

      super.execDataArrived(outcomeListener, result);
    }

    @Override
    public void planValidated(RelDataType rowType, SqlNode node, long millisTaken) {
      isCTAS = node.getKind() == SqlKind.CREATE_TABLE;
      super.planValidated(rowType, node, millisTaken);
    }

    @Override
    public void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after, long millisTaken) {
      if (phase == PlannerPhase.PHYSICAL) {
        containsHashAgg = containsHashAggregate(after);
      }
      super.planRelTransform(phase, planner, before, after, millisTaken);
    }

    private UserException handleSchemaChangeException(UserException schemaChange) {
      final SchemaChangeExceptionContext data = SchemaChangeExceptionContext.fromUserException(schemaChange);
      UserException result = null;
      if (data != null) {
        try {
          NamespaceKey datasetKey = new NamespaceKey(data.getTableSchemaPath());
          final String queryUserName = session.getCredentials().getUserName();
          final DatasetCatalog datasetCatalog =
              context.getCatalogService().getCatalog(MetadataRequestOptions.of(
                  SchemaConfig.newBuilder(queryUserName)
                      .build()));
          datasetCatalog.updateDatasetSchema(datasetKey, data.getNewSchema());

          // Update successful, populate return exception.
          result = UserException.schemaChangeError()
                                .message("New schema found and recorded. Please reattempt the query. Multiple attempts may be necessary to fully learn the schema.")
                                .build(logger);
        } catch (UserException e) {
          // SCHEMA_CHANGE could result in an INVALID_DATASET_METADATA exception (from ElasticSearch)
          result = e;
        } catch (Exception e) {
          logger.error("something went wrong when trying to persist schema change", e);
        }
      }

      return result;
    }

    private UserException handleJsonFieldChangeException(UserException schemaChange) {
      final JsonFieldChangeExceptionContext data = JsonFieldChangeExceptionContext.fromUserException(schemaChange);
      UserException result = null;
      if (data != null) {
        try {
          NamespaceKey datasetKey = new NamespaceKey(data.getOriginTablePath());
          final String queryUserName = session.getCredentials().getUserName();
          final DatasetCatalog datasetCatalog =
              context.getCatalogService()
                  .getCatalog(MetadataRequestOptions.of(SchemaConfig.newBuilder(queryUserName).build()));
          datasetCatalog.updateDatasetField(datasetKey, data.getFieldName(), data.getFieldSchema());

          // Update successful, populate return exception.
          result = UserException.jsonFieldChangeError()
            .message("New field in the JSON schema found.  Please reattempt the query.  Multiple attempts may be necessary to fully learn the schema.")
            .build(logger);
        } catch (Exception e) {
          logger.error("something went wrong when trying to persist field change", e);
        }
      }

      return result;
    }

    @Override
    public void attemptCompletion(UserResult result) {
      // NOTE to developers: adhere to these invariants:
      // (1) On last attempt, #execCompletion is invoked and #attemptCompletion is NOT invoked. The last attempt
      //     maybe the first and only attempt.
      // (2) #attemptCompletion is only invoked if there is going to be another attempt.
      // TODO(DX-10101): Define the guarantee of #attemptCompletion or rework #attemptCompletion

      attemptManager = null; // make sure we don't pass cancellation requests to this attemptManager anymore

      final QueryState queryState = result.getState();
      final boolean queryFailed = queryState == QueryState.FAILED;

      // if the query failed we may be able to recover from it
      if (queryFailed) {
        // if it wasn't canceled
        if (canceled) {
          logger.info("{}: cannot re-attempt the query, user already cancelled it", attemptId);
        } else {

          // We could have a non-recoverable attempt with a schema change, so do that work up front.
          UserException ex = result.getException();
          if (ex.getErrorType() == UserBitShared.DremioPBError.ErrorType.SCHEMA_CHANGE) {
            UserException e = handleSchemaChangeException(result.getException());
            if (e != null) {
              if (e.getErrorType() == UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION) {
                result = result.replaceException(e);
              } else {
                ex = e;
                result = result.withException(ex);
              }
            }
          } else if (ex.getErrorType() == UserBitShared.DremioPBError.ErrorType.JSON_FIELD_CHANGE) {
            UserException e = handleJsonFieldChangeException(result.getException());
            if (e != null) {
              ex = e;
              result = result.withException(ex);
            }
          }

          // Check if the attemptHandler allows the reattempt
          final AttemptReason reason = attemptHandler.isRecoverable(
            new ReAttemptContext(attemptId, ex, containsHashAgg, isCTAS));
          if (reason != AttemptReason.NONE) {
            super.attemptCompletion(result);

            Predicate<DatasetConfig> datasetValidityChecker = getDatasetValidityChecker(ex, reason);

            // run another attempt, after making the necessary changes to recover from the failure
            try {
              // if the query gets cancelled before we started the new attempt
              // we report query completed with last attempt's status
              if (recoverFromFailure(reason, datasetValidityChecker)) {
                return;
              }
            } catch (Exception e) {
              // if we fail to start a new attempt we log it and fail the query as if the previous failure was not recoverable
              logger.error("{}: something went wrong when re-attempting the query", attemptId.getExternalId(), e);
            }
          }
        }
      }

      // no more attempts are needed/possible
      observer.execCompletion(result);

      listener.completed();
    }

    @Override
    public void planParallelized(PlanningSet planningSet) {
      super.planParallelized(planningSet);
    }

  }

  @VisibleForTesting
  static Predicate<DatasetConfig> getDatasetValidityChecker(UserException ex, AttemptReason reason) {
    Predicate<DatasetConfig> datasetValidityChecker = Predicates.alwaysTrue();
    if (reason == AttemptReason.INVALID_DATASET_METADATA) {
      final InvalidMetadataErrorContext context = InvalidMetadataErrorContext.fromUserException(ex);
      if (context != null) {
        datasetValidityChecker = new Predicate<DatasetConfig>() {
          private final ImmutableSet<List<String>> keys = ImmutableSet.copyOf(context.getPathsToRefresh());

          @Override
          public boolean apply(DatasetConfig input) {
            return !keys.contains(input.getFullPathList());
          }
        };
      }
    }
    return datasetValidityChecker;
  }

  private static class LowMemOptionProvider implements OptionProvider {

    private final OptionProvider optionProvider;

    LowMemOptionProvider(OptionProvider optionProvider) {
      this.optionProvider = optionProvider;
    }

    @Override
    public void applyOptions(OptionManager manager) {
      if (optionProvider != null) {
        optionProvider.applyOptions(manager);
      }
      // TODO(DX-5912): disable hash join after merge join is implemented
      // manager.setOption(OptionValue.createBoolean(OptionValue.OptionType.QUERY,
      //    PlannerSettings.HASHJOIN.getOptionName(), false));
      manager.setOption(OptionValue.createBoolean(OptionValue.OptionType.QUERY,
        PlannerSettings.HASHAGG.getOptionName(), false));
    }
  }
}
