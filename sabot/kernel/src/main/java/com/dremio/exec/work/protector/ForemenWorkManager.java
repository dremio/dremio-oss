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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.arrow.util.VisibleForTesting;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.observer.OutOfBandQueryObserver;
import com.dremio.exec.planner.observer.QueryObserver;
import com.dremio.exec.planner.sql.handlers.commands.PreparedPlan;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.UserCredentials;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.work.SafeExit;
import com.dremio.exec.work.foreman.TerminationListenerRegistry;
import com.dremio.exec.work.rpc.CoordProtocol;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalExecutionConfig;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.exec.work.user.OptionProvider;
import com.dremio.options.OptionManager;
import com.dremio.resource.QueryCancelTool;
import com.dremio.resource.ResourceAllocator;
import com.dremio.sabot.rpc.ExecToCoordHandler;
import com.dremio.sabot.rpc.Protocols;
import com.dremio.sabot.rpc.user.UserRpcUtils;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.BindingCreator;
import com.dremio.service.Service;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.execselector.ExecutorSelectionService;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Manages all work associated with query oversight and coordination.
 */
public class ForemenWorkManager implements Service, SafeExit {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ForemenWorkManager.class);

  // Not making this a system/session option as we initialize this in the beginning of the node start and
  // changing system/session option is not going to have any effect.
  private static final String PREPARE_HANDLE_TIMEOUT_MS = "dremio.prepare.handle.timeout_ms";

  // cache of prepared statement queries.
  private final Cache<Long, PreparedPlan> preparedHandles = CacheBuilder.newBuilder()
      .maximumSize(1000)
      // Prepared statement handles are memory intensive. If there is memory pressure,
      // let GC release them as last resort before running OOM.
      .softValues()
      .expireAfterWrite(Long.getLong(PREPARE_HANDLE_TIMEOUT_MS, 60_000L), TimeUnit.MILLISECONDS)
      .build();

  // single map of currently running queries, mapped by their external ids.
  private final ConcurrentMap<ExternalId, ManagedForeman> externalIdToForeman = Maps.newConcurrentMap();
  private final NodeStatusListener nodeListener = new NodeStatusListener();
  private final Provider<ClusterCoordinator> coord;
  private final Provider<SabotContext> dbContext;
  private final Provider<FabricService> fabric;
  private final BindingCreator bindingCreator;
  private final Provider<ResourceAllocator> queryResourceManager;
  private final Provider<CommandPool> commandPool;
  private final Provider<ExecutorSelectionService> executorSelectionService;
  private final ForemanStatusThread statusThread;

  private ClusterCoordinator coordinator;
  private ExtendedLatch exitLatch = null; // This is used to wait to exit when things are still running
  private CloseableThreadPool pool = new CloseableThreadPool("foreman");
  private CoordToExecTunnelCreator tunnelCreator;

  public ForemenWorkManager(
      final Provider<ClusterCoordinator> coord,
      final Provider<FabricService> fabric,
      final Provider<SabotContext> dbContext,
      final Provider<ResourceAllocator> queryResourceManager,
      final Provider<CommandPool> commandPool,
      final Provider<ExecutorSelectionService> executorSelectionService,
      final BindingCreator bindingCreator) {
    this.coord = coord;
    this.dbContext = dbContext;
    this.fabric = fabric;
    this.bindingCreator = bindingCreator;
    this.queryResourceManager = queryResourceManager;
    this.commandPool = commandPool;
    this.executorSelectionService = executorSelectionService;
    this.statusThread = new ForemanStatusThread();
  }

  @Override
  public void start() throws Exception {
    coordinator = coord.get();
    coordinator.getServiceSet(ClusterCoordinator.Role.EXECUTOR).addNodeStatusListener(nodeListener);
    tunnelCreator = new CoordToExecTunnelCreator(fabric.get().getProtocol(Protocols.COORD_TO_EXEC));
    bindingCreator.replace(ExecToCoordHandler.class, new ExecToCoordHandlerImpl());

    statusThread.start();

    final ForemenTool tool = new ForemenToolImpl();
    boolean succeeded = bindingCreator.bindIfUnbound(ForemenTool.class, tool);
    if (!succeeded) {
      // then it is already bound, force set by replacing
      bindingCreator.replace(ForemenTool.class, tool);
    }

    final FabricRunnerFactory coordFactory = fabric.get()
        .registerProtocol(new CoordProtocol(dbContext.get().getAllocator(), tool, dbContext.get().getConfig()));
    bindingCreator.bindSelf(new CoordTunnelCreator(coordFactory));

    final QueryCancelTool queryCancelTool = new QueryCancelToolImpl();
    succeeded = bindingCreator.bindIfUnbound(QueryCancelTool.class, queryCancelTool);
    if (!succeeded) {
      bindingCreator.replace(QueryCancelTool.class, queryCancelTool);
    }

    // accept enduser rpc requests (replaces noop implementation).
    bindingCreator.bind(UserWorker.class, new UserWorkerImpl(dbContext.get().getOptionManager(), pool));

    // accept local query execution requests.
    bindingCreator.bind(LocalQueryExecutor.class, new LocalQueryExecutorImpl(dbContext.get().getOptionManager(), pool));
  }

  @Override
  public void close() throws Exception {
    if(coordinator != null){
      coordinator.getServiceSet(ClusterCoordinator.Role.EXECUTOR).removeNodeStatusListener(nodeListener);
    }
    AutoCloseables.close(statusThread, pool);
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
      config, attemptHandler, tunnelCreator, preparedHandles);
    final ManagedForeman managed = new ManagedForeman(registry, foreman);
    externalIdToForeman.put(foreman.getExternalId(), managed);
    delegate.setListener(managed);
    foreman.start();
  }

  protected Foreman newForeman(Executor executor, CommandPool commandPool, CompletionListener listener, ExternalId externalId,
      QueryObserver observer, UserSession session, UserRequest request, OptionProvider config,
      ReAttemptHandler attemptHandler, CoordToExecTunnelCreator tunnelCreator,
      Cache<Long, PreparedPlan> plans) {
    return new Foreman(dbContext.get(), executor, commandPool, listener, externalId, observer, session, request, config,
      attemptHandler, tunnelCreator, plans, queryResourceManager.get(), executorSelectionService.get());
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

    /**
     * Publish query progress.
     */
    private void publishProgress() {
      foreman.publishProgress();
    }

    private class ConnectionClosedListener implements GenericFutureListener<Future<Void>> {

      public ConnectionClosedListener() {
        super();
      }

      @Override
      public void operationComplete(Future<Void> future) throws Exception {
        foreman.cancel("User - Connection closed", false);
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
   * @return list of external Ids strings for all active foremen
   */
  private List<String> getForemenExternalIds() {
    return externalIdToForeman.keySet().stream().map((e) -> ExternalIdHelper.toString(e)).collect(Collectors.toList());
  }

  /*
   * publish progress over all foremen
   */
  @VisibleForTesting
  public void publishAllProgress() {
    try {
      for (ManagedForeman managedForeman : externalIdToForeman.values()) {
        managedForeman.publishProgress();
      }
    } catch (final Exception ignored) {
      // ignored
    }
  }

  /**
   * Thread to gather statistics about Job completeness. {@link ForemenWorkManager} looks
   * over each foreman and gathers statistics in the updateFragmentStatus method in
   * {@link QueryManager}.
   */
  private class ForemanStatusThread extends Thread implements AutoCloseable {
    private final long STATUS_PERIOD_MS = TimeUnit.SECONDS.toMillis(5);

    ForemanStatusThread() {
      setDaemon(true);
      setName("foreman-status-reporter");
    }

    @Override
    public void run() {
      while (true) {
        publishAllProgress();

        try {
          Thread.sleep(STATUS_PERIOD_MS);
        } catch (final InterruptedException ignored) {
          logger.debug("Status thread exiting.");
          break;
        }
      }
    }

    @Override
    public void close() {
      interrupt();
    }
  }

  /**
   * Alerts Foremen to node changes. Simplified code over constantly adding and deleting registrations for each foreman (since that would be wasteful).
   */
  private class NodeStatusListener implements com.dremio.service.coordinator.NodeStatusListener {

    @Override
    public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
      if (logger.isDebugEnabled()) {
        logger.debug("nodes Unregistered {}, all of the following queries are going to be cancelled {}",
          unregisteredNodes, getForemenExternalIds());
      }

      for (ManagedForeman f : externalIdToForeman.values()) {
        try {
          f.foreman.nodesUnregistered(unregisteredNodes);
        } catch (Exception e) {
          logger.warn("Foreman {} failed to handle unregistered nodes {}",
            ExternalIdHelper.toString(f.foreman.getExternalId()), unregisteredNodes, e);
        } catch (Throwable e) {
          // the assumption is that the system is in bad state and this will most likely cause it shutdown. That's why
          // we do nothing about the exception, but just in case it doesn't we want to at least get it in the log in case
          // some of those queries get stuck
          logger.error("Throwable exception was thrown in Foreman.nodesUnregistered, some of the following queries may get stuck {}",
            getForemenExternalIds(), e);
          throw e;
        }
      }
    }

    @Override
    public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
      for(ManagedForeman f : externalIdToForeman.values()){
        try {
          f.foreman.nodesRegistered(registeredNodes);
        } catch (Exception e) {
          logger.warn("Foreman {} failed to handle registered nodes {}", ExternalIdHelper.toString(f.foreman.getExternalId()), registeredNodes, e);
        }
      }
    }

  }

  /**
   * Cancel the query.
   *
   * @param externalId      id of the query
   * @param reason          description of the cancellation
   * @param clientCancelled true if the client application explicitly issued a cancellation (via end user action), or
   *                        false otherwise (i.e. when pushing the cancellation notification to the end user)
   */
  public boolean cancel(ExternalId externalId, String reason, boolean clientCancelled) {
    final ManagedForeman managed = externalIdToForeman.get(externalId);
    if (managed != null) {
      managed.foreman.cancel(reason, clientCancelled);
      return true;
    }

    return false;
  }

  public boolean resume(ExternalId externalId) {
    final ManagedForeman managed = externalIdToForeman.get(externalId);
    if (managed != null) {
      managed.foreman.resume();
      return true;
    }

    return false;
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

  private class ExecToCoordHandlerImpl implements ExecToCoordHandler {

    @Override
    public void fragmentStatusUpdate(FragmentStatus status) throws RpcException {
      ExternalId id = ExternalIdHelper.toExternal(status.getHandle().getQueryId());
      ManagedForeman managed = externalIdToForeman.get(id);
      if (managed == null) {
        logger.debug("A fragment status message arrived post query termination, dropping. Fragment [{}] reported a state of {}.", QueryIdHelper.getFragmentId(status.getHandle()), status.getProfile().getState());
      } else {
        managed.foreman.updateStatus(status);
      }
    }


    @Override
    public void dataArrived(QueryData header, ByteBuf data, ResponseSender sender) throws RpcException {
      ExternalId id = ExternalIdHelper.toExternal(header.getQueryId());
      ManagedForeman managed = externalIdToForeman.get(id);
      if (managed == null) {
        logger.debug("User data arrived post query termination, dropping. Data was from QueryId: {}.", QueryIdHelper.getQueryId(header.getQueryId()));
      } else {
        managed.foreman.dataFromScreenArrived(header, data, sender);
      }
    }

    @Override
    public void nodeQueryStatusUpdate(NodeQueryStatus status) throws RpcException {
      ExternalId id = ExternalIdHelper.toExternal(status.getId());
      ManagedForeman managed = externalIdToForeman.get(id);
      if (managed == null) {
        logger.debug("A node query status message arrived post query termination, dropping. Query [{}] from node {}.",
          QueryIdHelper.getQueryId(status.getId()), status.getEndpoint());
      } else {
        managed.foreman.updateNodeQueryStatus(status);
      }
    }
  }

  /**
   * Waits until it is safe to exit. Blocks until all currently running fragments have completed.
   *
   * <p>This is intended to be used by {@link com.dremio.exec.server.SabotNode#close()}.</p>
   */
  public void waitToExit() {
    synchronized(this) {
      if (externalIdToForeman.isEmpty()) {
        return;
      }

      exitLatch = new ExtendedLatch();
    }

    // Wait for at most 5 seconds or until the latch is released.
    exitLatch.awaitUninterruptibly(5000);
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
    private final SystemOptionManager options;
    private final Executor executor;


    public LocalQueryExecutorImpl(SystemOptionManager options, Executor executor) {
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
        boolean runInSameThread) {
      try{
        // make sure we keep a local observer out of band.
        final QueryObserver oobJobObserver = new OutOfBandQueryObserver(observer, executor);
        UserSession session = UserSession.Builder.newBuilder()
            .setSupportComplexTypes(true)
            .withCredentials(UserCredentials
                .newBuilder()
                .setUserName(config.getUsername())
                .build())
            .exposeInternalSources(config.isExposingInternalSources())
            .withDefaultSchema(config.getSqlContext())
            .withSubstitutionSettings(config.getSubstitutionSettings())
            .withOptionManager(options)
            .withClientInfos(UserRpcUtils.getRpcEndpointInfos("Dremio Java local client"))
            .build();

        final ReAttemptHandler attemptHandler = newInternalAttemptHandler(options, config.isFailIfNonEmptySent());
        final UserRequest userRequest = new UserRequest(prepare ? RpcType.CREATE_PREPARED_STATEMENT : RpcType.RUN_QUERY, query, runInSameThread);
        submit(externalId, oobJobObserver, session, userRequest, TerminationListenerRegistry.NOOP, config, attemptHandler);
      }catch(Exception ex){
        throw Throwables.propagate(ex);
      }
    }
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
      commandPool.get().<Void>submit(CommandPool.Priority.HIGH,
        ExternalIdHelper.toString(externalId) + ":work-submission",
        (waitInMillis) -> {
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
          final QueryObserver oobObserver = new OutOfBandQueryObserver(observer, executor);
          final ReAttemptHandler attemptHandler = newExternalAttemptHandler(session.getOptions());
          submit(externalId, oobObserver, session, request, registry, null, attemptHandler);
          return null;
        }, false);
    }

    @Override
    public Ack cancelQuery(ExternalId query, String username) {
      cancel(query, String.format("Query cancelled by user '%s'", username), true);
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

    @Override
    public boolean cancel(ExternalId id, String reason) {
      return ForemenWorkManager.this.cancel(id, reason, false);
    }

    @Override
    public Optional<QueryProfile> getProfile(ExternalId id) {
      ManagedForeman managed = externalIdToForeman.get(id);
      if(managed == null){
        return Optional.absent();
      }

      return managed.foreman.getCurrentProfile();
    }
  }

  private class QueryCancelToolImpl implements QueryCancelTool {

    @Override
    public boolean cancel(ExternalId id, String reason) {
      return ForemenWorkManager.this.cancel(id, reason, false);
    }
  }
}
