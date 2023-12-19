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

import static com.dremio.exec.ExecConstants.MAX_FOREMEN_PER_COORDINATOR;
import static com.dremio.proto.model.PartitionStats.PartitionStatsKey;
import static com.dremio.proto.model.PartitionStats.PartitionStatsValue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableExecutorService;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.concurrent.ContextMigratingExecutorService.ContextMigratingCloseableExecutorService;
import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.config.DremioConfig;
import com.dremio.context.RequestContext;
import com.dremio.datastore.format.Format;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.maestro.MaestroForwarder;
import com.dremio.exec.maestro.MaestroService;
import com.dremio.exec.planner.CachedPlan;
import com.dremio.exec.planner.PlanCache;
import com.dremio.exec.planner.observer.OutOfBandQueryObserver;
import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.planner.sql.handlers.commands.PreparedPlan;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.UserCredentials;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.work.SafeExit;
import com.dremio.exec.work.foreman.TerminationListenerRegistry;
import com.dremio.exec.work.rpc.CoordProtocol;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalExecutionConfig;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.exec.work.user.OptionProvider;
import com.dremio.options.OptionManager;
import com.dremio.partitionstats.cache.PartitionStatsCache;
import com.dremio.partitionstats.storeprovider.PartitionStatsCacheStoreProvider;
import com.dremio.resource.QueryCancelTool;
import com.dremio.resource.RuleBasedEngineSelector;
import com.dremio.sabot.exec.CancelQueryContext;
import com.dremio.sabot.rpc.CoordExecService.NoExecToCoordResultsHandler;
import com.dremio.sabot.rpc.ExecToCoordResultsHandler;
import com.dremio.sabot.rpc.user.UserRpcUtils;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Service;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.jobresults.JobResultsRequest;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.services.jobresults.common.JobResultsRequestWrapper;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.opentracing.Tracer;

/**
 * Manages all work associated with query oversight and coordination.
 */
public class ForemenWorkManager implements Service, SafeExit {
  private static final Logger logger = LoggerFactory.getLogger(ForemenWorkManager.class);

  // Not making this a system/session option as we initialize this in the beginning of the node start and
  // changing system/session option is not going to have any effect.
  private static final String PREPARE_HANDLE_TIMEOUT_MS = "dremio.prepare.handle.timeout_ms";
  private static final String PLAN_CACHE_TIMEOUT_S = "dremio.plan.cache.timeout_s";

  // send profile updates to the job-telemetry-service for all active queries at this
  // interval.
  private static final int PROFILE_SEND_INTERVAL_SECONDS = 5;

  // cache of prepared statement queries.
  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  private final Cache<Long, PreparedPlan> preparedHandles = CacheBuilder.newBuilder()
          .maximumSize(1000)
          // Prepared statement handles are memory intensive. If there is memory pressure,
          // let GC release them as last resort before running OOM.
          .softValues()
          .expireAfterWrite(Long.getLong(PREPARE_HANDLE_TIMEOUT_MS, 60_000L), TimeUnit.MILLISECONDS)
          .build();

  // single map of currently running queries, mapped by their external ids.
  private final ConcurrentMap<ExternalId, ManagedForeman> externalIdToForeman = Maps.newConcurrentMap();
  private final Provider<SabotContext> dbContext;
  private final Provider<FabricService> fabric;
  private final Provider<CommandPool> commandPool;
  protected final Provider<MaestroService> maestroService;
  protected final Provider<JobTelemetryClient> jobTelemetryClient;
  private final Provider<MaestroForwarder> forwarder;
  private final ForemenTool foremenTool;
  private final QueryCancelTool queryCancelTool;
  private final BufferAllocator jobResultsAllocator;

  private ExtendedLatch exitLatch = null; // This is used to wait to exit when things are still running
  private CloseableExecutorService pool;
  private Provider<RuleBasedEngineSelector> ruleBasedEngineSelector;
  private ExecToCoordResultsHandler execToCoordResultsHandler;
  private CoordTunnelCreator coordTunnelCreator;
  private UserWorker userWorker;
  private LocalQueryExecutor localQueryExecutor;
  private final CloseableSchedulerThreadPool profileSender;
  private Cache<String, CachedPlan> cachedPlans;
  private PlanCache planCache;
  private final Provider<RequestContext> requestContextProvider;
  private final Provider<PartitionStatsCacheStoreProvider> transientStoreProvider;
  private PartitionStatsCache partitionStatsCache;

  public ForemenWorkManager(
    final Provider<FabricService> fabric,
    final Provider<SabotContext> dbContext,
    final Provider<CommandPool> commandPool,
    final Provider<MaestroService> maestroService,
    final Provider<JobTelemetryClient> jobTelemetryClient,
    final Provider<MaestroForwarder> forwarder,
    final Tracer tracer,
    final Provider<RuleBasedEngineSelector> ruleBasedEngineSelector,
    final BufferAllocator jobResultsAllocator,
    final Provider<RequestContext> requestContextProvider,
    final Provider<PartitionStatsCacheStoreProvider> transientStoreProvider) {
    this.dbContext = dbContext;
    this.fabric = fabric;
    this.commandPool = commandPool;
    this.maestroService = maestroService;
    this.jobTelemetryClient = jobTelemetryClient;
    this.forwarder = forwarder;

    this.pool = new ContextMigratingCloseableExecutorService<>(new CloseableThreadPool("foreman"));
    this.ruleBasedEngineSelector = ruleBasedEngineSelector;
    this.execToCoordResultsHandler = new NoExecToCoordResultsHandler();
    this.foremenTool = new ForemenToolImpl();
    this.queryCancelTool = new QueryCancelToolImpl();
    this.profileSender = new CloseableSchedulerThreadPool("profile-sender", 1);
    this.jobResultsAllocator = jobResultsAllocator;
    this.requestContextProvider = requestContextProvider;
    this.transientStoreProvider = transientStoreProvider;
  }

  public ExecToCoordResultsHandler getExecToCoordResultsHandler() {
    return execToCoordResultsHandler;
  }

  public ForemenTool getForemenTool() {
    return foremenTool;
  }

  public CoordTunnelCreator getCoordTunnelCreator() {
    return coordTunnelCreator;
  }

  public QueryCancelTool getQueryCancelTool() {
    return queryCancelTool;
  }

  public UserWorker getUserWorker() {
    return userWorker;
  }

  public LocalQueryExecutor getLocalQueryExecutor() {
    return localQueryExecutor;
  }

  public PlanCache getPlanCacheHandle() {
    return planCache;
  }

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  @Override
  public void start() throws Exception {
    Metrics.newGauge(Metrics.join("jobs","active"), () -> externalIdToForeman.size());

    execToCoordResultsHandler = new ExecToCoordResultsHandlerImpl();

    final FabricRunnerFactory coordFactory = fabric.get()
            .registerProtocol(new CoordProtocol(dbContext.get().getAllocator(), foremenTool, dbContext.get().getConfig()));
    this.coordTunnelCreator = new CoordTunnelCreator(coordFactory);

    this.userWorker = new UserWorkerImpl(dbContext.get().getOptionManager(), pool);
    this.localQueryExecutor = new LocalQueryExecutorImpl(dbContext.get().getOptionManager(), pool);
    this.profileSender.scheduleWithFixedDelay(this::sendAllProfiles,
      PROFILE_SEND_INTERVAL_SECONDS, PROFILE_SEND_INTERVAL_SECONDS, TimeUnit.SECONDS);

    // cache for physical plans.
    cachedPlans = CacheBuilder.newBuilder()
      .maximumWeight(dbContext.get().getDremioConfig().getLong(DremioConfig.PLAN_CACHE_MAX_ENTRIES))
      .weigher((Weigher<String, CachedPlan>) (key, cachedPlan) -> cachedPlan.getEstimatedSize())
      // plan caches are memory intensive. If there is memory pressure,
      // let GC release them as last resort before running OOM.
      .softValues()
      .removalListener(
        new RemovalListener<String, CachedPlan>() {
          @Override
          public void onRemoval(RemovalNotification<String, CachedPlan> notification) {
            PlanCache.clearDatasetMapOnCacheGC(notification.getKey());
          }
        }
      )
      .expireAfterAccess(dbContext.get().getDremioConfig().getLong(DremioConfig.PLAN_CACHE_TIMEOUT_MINUTES), TimeUnit.MINUTES)
      .build();

    planCache = new PlanCache(cachedPlans, Multimaps.synchronizedListMultimap(ArrayListMultimap.create()));

    partitionStatsCache = new PartitionStatsCache(this.transientStoreProvider.get().getStore(
        Format.ofProtobuf(PartitionStatsKey.class),
        Format.ofProtobuf(PartitionStatsValue.class),
        dbContext.get().getDremioConfig().getInt(DremioConfig.PARTITION_STATS_CACHE_TTL)));
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(pool, profileSender, jobResultsAllocator);
  }

  @VisibleForTesting
  public Foreman getForemanByID(ExternalId id) {
    return externalIdToForeman.get(id).foreman;
  }

  private boolean canAcceptWork() {
    final long foremenLimit = dbContext.get().getOptionManager().getOption(MAX_FOREMEN_PER_COORDINATOR);
    return externalIdToForeman.size() < foremenLimit;
  }

  public void submit(
          final ExternalId externalId,
          final QueryObserver observer,
          final UserSession session,
          final UserRequest request,
          final TerminationListenerRegistry registry,
          final OptionProvider config,
          final ReAttemptHandler attemptHandler) {

    final DelegatingCompletionListener delegate = new DelegatingCompletionListener();
    final Foreman foreman = newForeman(pool, commandPool.get(), delegate, externalId, observer, session, request,
            config, attemptHandler, preparedHandles, planCache, partitionStatsCache);
    final ManagedForeman managed = new ManagedForeman(registry, foreman);
    externalIdToForeman.put(foreman.getExternalId(), managed);
    delegate.setListener(managed);
    foreman.start();
  }

  protected Foreman newForeman(Executor executor, CommandPool commandPool, CompletionListener listener, ExternalId externalId,
                               QueryObserver observer, UserSession session, UserRequest request, OptionProvider config,
                               ReAttemptHandler attemptHandler, Cache<Long, PreparedPlan> preparedPlans,
                               PlanCache planCache, PartitionStatsCache partitionStatsCache) {
    return new Foreman(dbContext.get(), executor, commandPool, listener, externalId, observer, session, request, config,
            attemptHandler, preparedPlans, planCache, maestroService.get(), jobTelemetryClient.get(), ruleBasedEngineSelector.get(),
            partitionStatsCache);
  }

  /**
   * Internal class that allows ForemanManager to indirectly reference its wrapper object.
   */
  private static class DelegatingCompletionListener implements CompletionListener {

    private CompletionListener listener;

    public void setListener(CompletionListener listener){
      this.listener = listener;
    }

    @Override
    public void completed() {
      if(listener == null){
        throw new NullPointerException("Completion listener was null when called. This should never happen.");
      }
      listener.completed();
    }

  }

  /**
   * A wrapper class that manages the lifecyle of foreman to ensure ForemanManager internal consistency.
   */
  private final class ManagedForeman implements CompletionListener {
    private final ConnectionClosedListener closeListener = new ConnectionClosedListener();
    private final Foreman foreman;
    private final TerminationListenerRegistry registry;

    public ManagedForeman(final TerminationListenerRegistry registry, final Foreman foreman) {
      this.foreman = Preconditions.checkNotNull(foreman, "foreman is null");

      registry.addTerminationListener(closeListener);
      this.registry = registry;
    }

    private class ConnectionClosedListener implements GenericFutureListener<Future<Void>> {

      public ConnectionClosedListener() {
        super();
      }

      @Override
      public void operationComplete(Future<Void> future) throws Exception {
        foreman.cancel("User - Connection closed", false, false);
      }
    }

    @Override
    public void completed() {
      registry.removeTerminationListener(closeListener);
      final ExternalId externalId = foreman.getExternalId();

      final ManagedForeman managed = externalIdToForeman.remove(externalId);
      if (managed == null) {
        logger.warn("Couldn't find retiring Foreman for query " + externalId);
      }

      indicateIfSafeToExit();
    }
  }

  /**
   * Cancel the query.
   *
   * @param externalId      id of the query
   * @param reason          description of the cancellation
   * @param clientCancelled true if the client application explicitly issued a cancellation (via end user action), or
   *                        false otherwise (i.e. when pushing the cancellation notification to the end user)
   * @param runTimeExceeded true if the query is being cancelled because the max runtime has been exceeded
   */
  public boolean cancel(ExternalId externalId, String reason, boolean clientCancelled, boolean runTimeExceeded) {
    final ManagedForeman managed = externalIdToForeman.get(externalId);
    if (managed != null) {
      managed.foreman.cancel(reason, clientCancelled, runTimeExceeded);
      return true;
    }

    return false;
  }

  /**
   * Cancel queries in given cancel query context
   *
   * @param cancelQueryContext
   */
  public void cancel(CancelQueryContext cancelQueryContext) {
    externalIdToForeman.values()
                       .stream()
                       .filter(mf->mf.foreman.canCancelByHeapMonitor())
                       .forEach(mf->mf.foreman.cancel(cancelQueryContext.getCancelReason(),
                         false,
                         cancelQueryContext.getCancelContext(),
                         cancelQueryContext.isCancelledByHeapMonitor(),
                         false));
  }

  public boolean resume(ExternalId externalId) {
    final ManagedForeman managed = externalIdToForeman.get(externalId);
    if (managed != null) {
      managed.foreman.resume();
      return true;
    }

    return false;
  }

  @VisibleForTesting
  public int getActiveQueryCount() {
    return externalIdToForeman.size();
  }

  private ReAttemptHandler newInternalAttemptHandler(OptionManager options, boolean failIfNonEmpty) {
    if (options.getOption(ExecConstants.ENABLE_REATTEMPTS)) {
      return new InternalAttemptHandler(options, failIfNonEmpty);
    } else {
      return new NoReAttemptHandler();
    }
  }

  private ReAttemptHandler newExternalAttemptHandler(OptionManager options) {
    if (options.getOption(ExecConstants.ENABLE_REATTEMPTS)) {
      return new ExternalAttemptHandler(options);
    } else {
      return new NoReAttemptHandler();
    }
  }

  private class ExecToCoordResultsHandlerImpl implements ExecToCoordResultsHandler {

    /*
     * For improving performance, avoid providing both "data" and "request" as non-null.
     * i.e have one of data or request as null.
     * If both are non-null
     * then
     *  a. If query is running locally, then data is used
     *  b. else forwarding is done and request is used
     */
    @Override
    public void dataArrived(QueryData header, ByteBuf data, JobResultsRequest request, ResponseSender sender) throws RpcException {
      Preconditions.checkNotNull(header, "header parameter cannot be null");

      String queryId = QueryIdHelper.getQueryId(header.getQueryId());
      ExternalId id = ExternalIdHelper.toExternal(header.getQueryId());
      logger.debug("ForemanWorkManager.dataArrived.request called for QueryId:{}", queryId);

      ManagedForeman managed = externalIdToForeman.get(id);
      if (managed != null) {
        logger.debug("User Data arrived for QueryId: {}.", queryId);
        // data is only needed when "managed" is not null
        if (data == null && request != null) {
          // Allocate buffer of data.size()
          try (ArrowBuf buf = jobResultsAllocator.buffer(request.getData().size())) {
            data = getData(request, buf);
            managed.foreman.dataFromScreenArrived(header, sender, data);
          }
        } else {
          managed.foreman.dataFromScreenArrived(header, sender, data);
        }

      } else if (request != null) {
        forwarder.get().dataArrived(request, sender);

      } else {
        logger.warn("User data arrived post query termination, dropping. Data was from QueryId: {}.", queryId);
        //  Return a Failure in this case, the query is already terminated and it will be cancelled
        // on the Executor, either response will unblock the caller.
        sender.sendFailure(new UserRpcException(dbContext.get().getEndpoint(),
          "Query Already Terminated", new Throwable("Query Already Terminated")));
      }
    }

    @Override
    public boolean dataArrived(JobResultsRequestWrapper request, ResponseSender sender) throws RpcException {
      Preconditions.checkNotNull(request, "jobResultsRequestWrapper parameter cannot be null");

      QueryData header = request.getHeader();

      String queryId = QueryIdHelper.getQueryId(header.getQueryId());
      logger.debug("ForemanWorkManager.dataArrived.RequestWrapper called for QueryId: {}", queryId);

      ExternalId id = ExternalIdHelper.toExternal(header.getQueryId());
      ManagedForeman managed = externalIdToForeman.get(id);

      if (managed != null) {
        logger.debug("RequestWrapper - User Data arrived for QueryId: {}.", queryId);
        managed.foreman.dataFromScreenArrived(header, sender, request.getByteBuffers());
        return false;
      } else {
        logger.debug("RequestWrapper - forward user data for QueryId: {}.", queryId);
        forwarder.get().dataArrived(request, sender);
        return true;
      }
    }
  }

  // copy data from request.getData() which is in heap to
  // byteBuf which is in direct memory, for further processing.
  protected ByteBuf getData(JobResultsRequest request, ArrowBuf buf) {
    // Get ByteBuf wrapper on top of ArrowBuf buf
    ByteBuf byteBuf = NettyArrowBuf.unwrapBuffer(buf);
    // Actually read request.data into byteBuf.
    byteBuf.writeBytes(request.getData().toByteArray());
    return byteBuf;
}

  /**
   * Waits until it is safe to exit. Blocks until all currently running fragments have completed.
   *
   * <p>This is intended to be used by com.dremio.exec.server.SabotNode#close(). </p>
   */
  @Override
  public void waitToExit() {
    synchronized(this) {
      if (externalIdToForeman.isEmpty()) {
        return;
      }

      exitLatch = new ExtendedLatch();
    }

    // Wait for at most the configured graceful timeout or until the latch is released.
    exitLatch.awaitUninterruptibly(dbContext.get().getDremioConfig().getLong(
      DremioConfig.DREMIO_TERMINATION_GRACE_PERIOD_SECONDS) * 1000);
  }

  /**
   * If it is safe to exit, and the exitLatch is in use, signals it so that waitToExit() will
   * unblock.
   */
  private void indicateIfSafeToExit() {
    synchronized(this) {
      if (exitLatch != null) {
        if (externalIdToForeman.isEmpty()) {
          exitLatch.countDown();
        }
      }
    }
  }

  /**
   * Handler for in-process queries
   */
  private class LocalQueryExecutorImpl implements LocalQueryExecutor {
    private final OptionManager options;
    private final Executor executor;


    public LocalQueryExecutorImpl(OptionManager options, Executor executor) {
      super();
      this.options = options;
      this.executor = executor;
    }

    @Override
    public boolean canAcceptWork() {
      return ForemenWorkManager.this.canAcceptWork();
    }

    @Override
    public void submitLocalQuery(
      ExternalId externalId,
      QueryObserver observer,
      Object query,
      boolean prepare,
      LocalExecutionConfig config,
      boolean runInSameThread,
      UserSession userSession) {
      try{
        // make sure we keep a local observer out of band.
        final QueryObserver oobJobObserver = new OutOfBandQueryObserver(observer, executor, requestContextProvider);

        if (userSession == null) {
          userSession = UserSession.Builder.newBuilder()
            .withSessionOptionManager(new SessionOptionManagerImpl(options.getOptionValidatorListing()), options)
            .setSupportComplexTypes(true)
            .withCredentials(UserCredentials
              .newBuilder()
              .setUserName(config.getUsername())
              .build())
            .exposeInternalSources(config.isExposingInternalSources())
            .withDefaultSchema(config.getSqlContext())
            .withSubstitutionSettings(config.getSubstitutionSettings())
            .withClientInfos(UserRpcUtils.getRpcEndpointInfos("Dremio Java local client"))
            .withEngineName(config.getEngineName())
            .withSourceVersionMapping(config.getSourceVersionMapping())
            .build();
        }

        final ReAttemptHandler attemptHandler = newInternalAttemptHandler(options, config.isFailIfNonEmptySent());
        final UserRequest userRequest = new UserRequest(prepare ? RpcType.CREATE_PREPARED_STATEMENT : RpcType.RUN_QUERY, query, runInSameThread);
        submit(externalId, oobJobObserver, userSession, userRequest, TerminationListenerRegistry.NOOP, config, attemptHandler);
      } catch(Exception ex){
        throw Throwables.propagate(ex);
      }
    }
  }

  @VisibleForTesting // package-protected for testing purposes. Don't make it public.
  void submitWork(ExternalId externalId,
                  UserSession session,
                  UserResponseHandler responseHandler,
                  UserRequest request,
                  TerminationListenerRegistry registry,
                  Executor executor) {
    commandPool.get().<Void>submit(CommandPool.Priority.HIGH,
      ExternalIdHelper.toString(externalId) + ":work-submission",
      "work-submission",
      (waitInMillis) -> submitWorkCommand(externalId, session, responseHandler, request, registry, executor, waitInMillis),
      request.runInSameThread())
      .whenComplete((o, e)-> {
        if (e != null) {
          QueryProfile profile = foremenTool.getProfile(externalId).isPresent() ?
            foremenTool.getProfile(externalId).get() : null;
          UserException exception = UserException.resourceError()
            .message(e.getMessage() + ". Root cause: " + Throwables.getRootCause(e).getMessage())
            .buildSilently();
          UserResult result = new UserResult(null,
            ExternalIdHelper.toQueryId(externalId),
            UserBitShared.QueryResult.QueryState.FAILED,
            profile,
            exception,
            null,
            false,
            false,
            false);
          responseHandler.completed(result);
        }
      });
  }

  @VisibleForTesting // package-protected for testing purposes. Don't make it public.
  Void submitWorkCommand(ExternalId externalId,
                         UserSession session,
                         UserResponseHandler responseHandler,
                         UserRequest request,
                         TerminationListenerRegistry registry,
                         Executor executor,
                         Long waitInMillis) {
    if (!canAcceptWork()) {
      throw UserException.resourceError()
        .message(UserException.QUERY_REJECTED_MSG)
        .buildSilently();
    }

    if (waitInMillis > CommandPool.WARN_DELAY_MS) {
      logger.warn("Work submission {} waited too long in the command pool: wait was {}ms",
        ExternalIdHelper.toString(externalId), waitInMillis);
    }
    session.incrementQueryCount();
    final QueryObserver observer = dbContext.get().getQueryObserverFactory().get().createNewQueryObserver(
      externalId, session, responseHandler);
    final QueryObserver oobObserver = new OutOfBandQueryObserver(observer, executor, requestContextProvider);
    final ReAttemptHandler attemptHandler = newExternalAttemptHandler(session.getOptions());
    submit(externalId, oobObserver, session, request, registry, null, attemptHandler);
    return null;
  }


  /**
   * Worker for queries coming from user layer.
   */
  public class UserWorkerImpl implements UserWorker {

    private final OptionManager systemOptions;
    private final Executor executor;


    public UserWorkerImpl(OptionManager systemOptions, Executor executor) {
      super();
      this.systemOptions = systemOptions;
      this.executor = executor;
    }

    @Override
    public void submitWork(ExternalId externalId, UserSession session,
                           UserResponseHandler responseHandler, UserRequest request, TerminationListenerRegistry registry) {
      ForemenWorkManager.this.submitWork(externalId, session, responseHandler, request, registry, executor);
    }

    @Override
    public Ack cancelQuery(ExternalId query, String username) {
      cancel(query, String.format("Query cancelled by user '%s'", username), true, false);
      return Acks.OK;
    }

    @Override
    public Ack resumeQuery(ExternalId query) {
      resume(query);
      return Acks.OK;
    }

    @Override
    public OptionManager getSystemOptions() {
      return systemOptions;
    }

  }

  private class ForemenToolImpl implements ForemenTool {

    // Used by REST APIs to cancel running queries
    @Override
    public boolean cancel(ExternalId id, String reason) {
      return ForemenWorkManager.this.cancel(id, reason, true, false);
    }

    @Override
    public Optional<QueryProfile> getProfile(ExternalId id) {
      ManagedForeman managed = externalIdToForeman.get(id);
      if(managed == null){
        return Optional.empty();
      }

      return managed.foreman.getCurrentProfile();
    }
  }

  private class QueryCancelToolImpl implements QueryCancelTool {

    // Not used when REST APIs are used to cancel queries
    // Hence, clientCancelled is false
    @Override
    public boolean cancel(ExternalId id, String reason, boolean runTimeExceeded) {
      return ForemenWorkManager.this.cancel(id, reason, false, runTimeExceeded);
    }
  }

  private void sendAllProfiles() {
    final List<ListenableFuture<Empty>> futures = Lists.newArrayList();

    for (ManagedForeman managedForeman : externalIdToForeman.values()) {
      try {
        Optional<ListenableFuture<Empty>> future =
         managedForeman.foreman.sendPlanningProfile();
        future.ifPresent(futures::add);
      } catch (final Exception e) {
        // Exception ignored. Profile sender thread should not die due to a random
        // exception
      }
    }

    // we'll wait to complete so we don't back up if the cluster is moving slowly.
    try {
      Futures.successfulAsList(futures).get();
    } catch (final Exception ex) {
      logger.info("Failure while sending profile to JobTelemetryService", ex);
    }
  }

  @VisibleForTesting

  /**
   * DX-27692: In some cases, when the client connection closes abruptly,
   *     the query gets canceled and before the cancel processing completes,
   *     it switches to the FAILED state (ScreenShuttle::failed).
   *     At this point, the screen operator on executor may still have
   *     outstanding messages.  Once the FAILED state processing is complete,
   *     the foreman is removed from the externalIdToForeman map.
   *     Any new msgs to the coordinator for this query get dropped without an ack,
   *     and so, the fragment is stuck forever.
   *     The fix is to ack the message from screen operator with a failure in this case.
   *
   *     Simulate the scenario by removing externalId from the map.
   */
  public void testMarkQueryFailed(ExternalId externalId) {
    final ManagedForeman managed = externalIdToForeman.remove(externalId);
    if (managed == null) {
      logger.warn("Couldn't find retiring Foreman for query " + externalId);
    }

  }

  @VisibleForTesting
  public List<QueryProfile> getActiveProfiles() {
    return externalIdToForeman.values()
      .stream()
      .map(managed -> managed.foreman.getCurrentProfile())
      .filter(Optional::isPresent)
      .map(Optional::get)
      .collect(Collectors.toList());
  }
}
