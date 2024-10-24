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
package com.dremio.sabot.exec;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.OutOfMemoryOrResourceExceptionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.common.util.LoadingCacheWithExpiry;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.maestro.FragmentSubmitListener;
import com.dremio.exec.maestro.FragmentSubmitListener.FragmentSubmitFailures;
import com.dremio.exec.maestro.FragmentSubmitListener.FragmentSubmitSuccess;
import com.dremio.exec.planner.fragment.CachedFragmentReader;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.fragment.PlanFragmentsIndex;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentSet;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordExecRPC.SchedulingInfo;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.DlrMessageType;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.NodeDebugContextProvider;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.FragmentWorkManager.ExitCallback;
import com.dremio.sabot.exec.context.DlrStatusHandler;
import com.dremio.sabot.exec.context.HeapAllocatedMXBeanWrapper;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.rpc.IncomingDataBatch;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.memory.MemoryArbiter;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.TaskMonitorObserver;
import com.dremio.sabot.task.TaskPool;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MultiGauge.Row;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;

/** A type of map used to help manage fragments. */
public class FragmentExecutors
    implements AutoCloseable, Iterable<FragmentExecutor>, TaskMonitorObserver {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FragmentExecutors.class);
  private static final int LOG_BUFFER_SIZE = 32 * 1024;
  private static final Response OK = new Response(RpcType.ACK, Acks.OK);

  private static final long RPC_WAIT_IN_MSECS_PER_FRAGMENT = 5000L;
  private static final long RPC_MIN_WAIT_IN_MSECS = 30000L;

  private final LoadingCacheWithExpiry<FragmentHandle, FragmentHandler> handlers;
  private final Set<FragmentHandle> fragmentsRequestingCancellation =
      Collections.synchronizedSet(new HashSet<>());
  private final AtomicInteger numRunningFragments = new AtomicInteger();
  private final TaskPool pool;
  private final ExitCallback callback;
  private final long evictionDelayMillis;
  private final MaestroProxy maestroProxy;
  private final int warnMaxTime;
  private final OptionManager options;
  private final MemoryArbiter memoryArbiter;

  private final BufferAllocator allocator;
  // pre-allocate a 1MB log buffer to log during low mem
  private final StringBuilder logBuffer;
  private final NodeDebugContextProvider nodeDebugContext;
  private final QueriesClerk clerk;

  public FragmentExecutors(
      final BootStrapContext context,
      final NodeDebugContextProvider nodeDebugContext,
      final SabotConfig sabotConfig,
      final QueriesClerk clerk,
      final MaestroProxy maestroProxy,
      final ExitCallback callback,
      final TaskPool pool,
      final OptionManager options) {
    this.maestroProxy = maestroProxy;
    this.callback = callback;
    this.pool = pool;
    this.evictionDelayMillis =
        TimeUnit.SECONDS.toMillis(options.getOption(ExecConstants.FRAGMENT_CACHE_EVICTION_DELAY_S));
    this.handlers =
        new LoadingCacheWithExpiry<>(
            "fragment-handler",
            new CacheLoader<FragmentHandle, FragmentHandler>() {
              @Override
              public FragmentHandler load(FragmentHandle key) throws Exception {
                // Underlying loading cache's refresh() calls reload() on this
                // cacheLoader, which indirectly calls this load() method.
                // So new FragmentHandler should not be created, instead
                // existing handler should be used. This will avoid using
                // extra heap memory.
                FragmentHandler exitingFragmentHandler = handlers.getIfPresent(key);
                if (exitingFragmentHandler == null) {
                  return new FragmentHandler(key, evictionDelayMillis);
                }
                return exitingFragmentHandler;
              }
            },
            null,
            evictionDelayMillis);

    this.warnMaxTime = (int) options.getOption(ExecConstants.SLICING_WARN_MAX_RUNTIME_MS);
    this.options = options;
    this.allocator = context.getAllocator();
    this.memoryArbiter =
        MemoryArbiter.newInstance(
            sabotConfig, (DremioRootAllocator) this.allocator, this, clerk, options);
    this.pool.getTaskMonitor().addObserver(this);
    this.logBuffer = new StringBuilder(LOG_BUFFER_SIZE);
    ExecutionMetrics.registerActiveFragmentsCurrentCount(this);
    this.nodeDebugContext = nodeDebugContext;
    this.clerk = clerk;
  }

  @VisibleForTesting
  void checkAndEvict() {
    handlers.checkAndEvict();
  }

  @Override
  public Iterator<FragmentExecutor> iterator() {
    return Iterators.unmodifiableIterator(
        FluentIterable.from(handlers.asMap().values())
            .transform(
                new Function<FragmentHandler, FragmentExecutor>() {
                  @Nullable
                  @Override
                  public FragmentExecutor apply(FragmentHandler input) {
                    return input.getExecutor();
                  }
                })
            .filter(Predicates.<FragmentExecutor>notNull())
            .iterator());
  }

  @Override
  public void observeTaskMonitorEvent(boolean statsUpdated) {
    // monitor cancelled fragments
    long currentTimeMs = System.currentTimeMillis();
    FragmentHandle[] cancelledFragments =
        fragmentsRequestingCancellation.toArray(new FragmentHandle[0]);
    for (FragmentHandle fragmentHandle : cancelledFragments) {
      FragmentHandler handler = handlers.getIfPresent(fragmentHandle);
      if (handler == null) {
        continue;
      }

      FragmentExecutor fragmentExecutor = handler.getExecutor();
      if (fragmentExecutor == null) {
        continue;
      }

      fragmentExecutor.logDebugInfoForCancelledTasks(currentTimeMs);
    }

    if (statsUpdated) {
      ExecutionMetrics.getFragmentStateGauge()
          .register(
              handlers.asMap().values().stream()
                  .map(FragmentHandler::getExecutor)
                  .filter(Predicates.<FragmentExecutor>notNull())
                  .collect(
                      Collectors.groupingBy(
                          FragmentExecutor::getMetricState, Collectors.counting()))
                  .entrySet() /* The map from the collect is a key per `getMetricState` with the       */
                  .stream() /*   value as the count of FragmentExecutor objects that returned that key */
                  .map((entry) -> Row.of(Tags.of("state", entry.getKey()), entry.getValue()))
                  .collect(
                      Collectors
                          .toList()), /* This constructs a row per `getMetricState` key as the tag
                                       * With the count as the metric value for that row. */
              true /* this says to overwrite previous values */);
    }
  }

  private static final String COLUMN_HEADING =
      "ID, State, Task State, Slices, Long Slices, Run Q Load,"
          + "Setup Duration (ms), Run Duration (ms), Input 0 Records, Input 0 Batches, Input 0 Size (B),"
          + "Input 1 Records, Input 1 Batches, Input 1 Size (B),"
          + "Output Records, Output Batches, Output Size (B),"
          + "Total Field Count, Direct Memory Allocated (KB), Other";

  private static final String HEADING_EXTENSION_ON_HEAP_USAGE_ENABLED =
      ", Heap Allocated (Setup)(KB), Average Heap Allocated (Eval)(KB), Peak Heap Allocated (Eval)(KB), "
          + "Total Heap Allocated (Eval)(KB)";

  public synchronized String activeQueriesToCsv(QueriesClerk queriesClerk) {
    int totalActiveQueries = 0;
    int totalActiveFragments = 0;
    int totalOperators = 0;
    logBuffer.append(COLUMN_HEADING);
    boolean dumpHeapUsage = HeapAllocatedMXBeanWrapper.isFeatureSupported();
    if (dumpHeapUsage) {
      logBuffer.append(HEADING_EXTENSION_ON_HEAP_USAGE_ENABLED);
    }
    logBuffer.append(System.lineSeparator());
    String ret = "";
    try {

      for (final WorkloadTicket workloadTicket : queriesClerk.getWorkloadTickets()) {
        for (final QueryTicket queryTicket : workloadTicket.getActiveQueryTickets()) {
          totalActiveQueries++;
          for (FragmentTicket fragmentTicket :
              queriesClerk.getFragmentTickets(queryTicket.getQueryId())) {
            FragmentExecutor executor =
                handlers.getUnchecked(fragmentTicket.getHandle()).getExecutor();
            if (executor != null) {
              totalActiveFragments++;
              totalOperators +=
                  executor.fillFragmentStats(
                      logBuffer, QueryIdHelper.getQueryId(queryTicket.getQueryId()), dumpHeapUsage);
            }
          }
        }
      }
      if (totalActiveQueries > 0) {
        logBuffer
            .append("Total Active Queries : ")
            .append(totalActiveQueries)
            .append(System.lineSeparator());
        logBuffer
            .append("Total Active Fragments : ")
            .append(totalActiveFragments)
            .append(System.lineSeparator());
        logBuffer
            .append("Total Active Operators : ")
            .append(totalOperators)
            .append(System.lineSeparator());
        ret = logBuffer.toString();
      }
    } catch (Exception e) {
      // if there is an exception here just ignore and continue
      ret = "Unable to dump fragment CSV due to exception " + e.getMessage();
    }
    logBuffer.setLength(0);
    return ret;
  }

  /**
   * @return number of running fragments
   */
  public int size() {
    return numRunningFragments.get();
  }

  public void startFragments(
      final InitializeFragments fragments,
      final FragmentExecutorBuilder builder,
      final StreamObserver<Empty> sender,
      final NodeEndpoint identity) {
    final SchedulingInfo schedulingInfo =
        fragments.hasSchedulingInfo() ? fragments.getSchedulingInfo() : null;
    QueryStarterImpl queryStarter =
        new QueryStarterImpl(fragments, builder, sender, identity, schedulingInfo);
    builder.buildAndStartQuery(queryStarter.getFirstFragment(), schedulingInfo, queryStarter);
  }

  public EventProvider getEventProvider(FragmentHandle handle) {
    return handlers.getUnchecked(handle);
  }

  public void sendMessage(
      QueryId queryId,
      TunnelProvider tunnelProvider,
      NodeEndpoint endpoint,
      DlrMessageType messageType,
      ExtendedLatch latch,
      FragmentSubmitFailures fragmentSubmitFailures,
      FragmentSubmitSuccess fragmentSubmitSuccess) {
    DynamicLoadRoutingMessage message =
        new DynamicLoadRoutingMessage(queryId, messageType.getNumber());
    final FragmentSubmitListener listener =
        new FragmentSubmitListener(
            null, endpoint, null, latch, fragmentSubmitFailures, fragmentSubmitSuccess, null, null);

    DlrStatusHandler dlrStatusHandler = new DlrStatusHandler(listener);

    tunnelProvider.getExecTunnelDlr(endpoint, dlrStatusHandler).sendDLRMessage(message);
    logger.debug("nodepoint {}", endpoint.getAddress());
  }

  public void sendMessageToAllNodes(
      QueryId queryId,
      TunnelProvider tunnelProvider,
      List<NodeEndpoint> endPointsIndexList,
      DlrMessageType messageType) {
    int numNodes = endPointsIndexList.size();
    final ExtendedLatch endpointLatch = new ExtendedLatch(numNodes);
    final FragmentSubmitFailures fragmentSubmitFailures = new FragmentSubmitFailures();
    final FragmentSubmitSuccess fragmentSubmitSuccess = new FragmentSubmitSuccess();

    for (NodeEndpoint endpoint : endPointsIndexList) {
      sendMessage(
          queryId,
          tunnelProvider,
          endpoint,
          messageType,
          endpointLatch,
          fragmentSubmitFailures,
          fragmentSubmitSuccess);
    }

    final long timeout =
        Long.max(
            RPC_WAIT_IN_MSECS_PER_FRAGMENT * numNodes,
            Long.max(
                RPC_MIN_WAIT_IN_MSECS,
                this.options.getOption(ExecConstants.FRAGMENT_STARTER_TIMEOUT)));

    try {
      FragmentSubmitListener.awaitUninterruptibly(
          endPointsIndexList,
          endpointLatch,
          fragmentSubmitFailures,
          fragmentSubmitSuccess,
          numNodes,
          timeout);
    } catch (Exception e) {
      sendReleaseResourceTimeOut(
          queryId,
          tunnelProvider,
          endPointsIndexList,
          fragmentSubmitFailures,
          fragmentSubmitSuccess);
      throw e;
    }

    try {
      FragmentSubmitListener.checkForExceptions(fragmentSubmitFailures);
    } catch (Exception e) {
      sendReleaseResourceException(
          queryId,
          tunnelProvider,
          endPointsIndexList,
          fragmentSubmitFailures,
          fragmentSubmitSuccess);
      throw e;
    }
  }

  public void sendReleaseResourceException(
      QueryId queryId,
      TunnelProvider tunnelProvider,
      List<NodeEndpoint> endPointsIndexList,
      FragmentSubmitFailures fragmentSubmitFailures,
      FragmentSubmitSuccess fragmentSubmitSuccess) {
    for (final CoordinationProtos.NodeEndpoint ep : endPointsIndexList) {
      if (fragmentSubmitSuccess.getSubmissionSuccesses().contains(ep)
          && !fragmentSubmitFailures.listContains(ep)) {
        logger.debug("Sending release resource to to {}", ep.getAddress());
        sendMessage(queryId, tunnelProvider, ep, DlrMessageType.RELEASE_RESOURCE, null, null, null);
      }
    }
  }

  public void sendReleaseResourceTimeOut(
      QueryId queryId,
      TunnelProvider tunnelProvider,
      List<NodeEndpoint> endPointsIndexList,
      FragmentSubmitFailures fragmentSubmitFailures,
      FragmentSubmitSuccess fragmentSubmitSuccess) {
    for (final NodeEndpoint ep : endPointsIndexList) {
      if (fragmentSubmitSuccess.getSubmissionSuccesses().contains(ep)) {
        logger.debug("Sending release resource to {}", ep.getAddress());
        sendMessage(queryId, tunnelProvider, ep, DlrMessageType.RELEASE_RESOURCE, null, null, null);
      }
    }
  }

  public synchronized void activateFragmentsDLR(QueryId queryId, QueriesClerk clerk) {
    // TODO: any other way to get endpointsIndex?
    EndpointsIndex e = clerk.getEndpointsIndex(queryId);
    TunnelProvider tunnelProvider = clerk.getTunnelProvider(queryId);

    try {
      sendMessageToAllNodes(
          queryId, tunnelProvider, e.getEndpoints(), DlrMessageType.RESERVE_RESOURCE);
    } catch (Exception ex) {
      throw ex;
    }

    try {
      sendMessageToAllNodes(
          queryId, tunnelProvider, e.getEndpoints(), DlrMessageType.ACTIVATE_FRAGMENTS);
    } catch (Exception ex) {
      throw ex;
    }
  }

  /**
   * Activate previously initialized fragments for the specified query. The fragments could have
   * already been activated if they received messages from other fragments.
   *
   * @param queryId
   * @param clerk
   */
  public void activateFragments(QueryId queryId, QueriesClerk clerk) {
    logger.debug("received activation for query {}", QueryIdHelper.getQueryId(queryId));
    if (options.getOption(ExecConstants.ENABLE_DYNAMIC_LOAD_ROUTING)) {
      activateFragmentsDLR(queryId, clerk);
      return;
    }
    for (FragmentTicket fragmentTicket : clerk.getFragmentTickets(queryId)) {
      activateFragment(fragmentTicket.getHandle());
    }
  }

  @VisibleForTesting
  void activateFragment(FragmentHandle handle) {
    handlers.getUnchecked(handle).activate();
  }

  TunnelProvider getTunnelProvider(FragmentHandle handle) {
    return handlers.getUnchecked(handle).getTunnelProvider();
  }

  /*
   * Cancel all fragments for the specified query.
   *
   * @param queryId
   * @param clerk
   */
  public void cancelFragments(QueryId queryId, QueriesClerk clerk) {
    logger.debug("received cancel for query {}", QueryIdHelper.getQueryId(queryId));
    maestroProxy.setQueryCancelled(queryId);
    for (FragmentTicket fragmentTicket : clerk.getFragmentTickets(queryId)) {
      cancelFragment(fragmentTicket.getHandle());
    }
  }

  /**
   * Determine queries to be cancelled based on activeQueryList and cancel those queries. See
   * implementation for actual algo.
   *
   * @param activeQueryList
   * @param clerk
   */
  public void reconcileActiveQueries(
      CoordExecRPC.ActiveQueryList activeQueryList, QueriesClerk clerk) {
    Set<QueryId> queryIdsToCancel = maestroProxy.reconcileActiveQueries(activeQueryList);
    cancelFragments(queryIdsToCancel, clerk);
  }

  @VisibleForTesting
  void cancelFragments(Set<QueryId> queryIdsToCancel, QueriesClerk clerk) {
    logger.debug("# queries to be cancelled:{}", queryIdsToCancel);
    for (QueryId queryId : queryIdsToCancel) {
      logger.info("cancelling queryId:{} as determined using ActiveQueryList.", queryId);
      cancelFragments(queryId, clerk);
    }
  }

  /*
   * Fail all fragments for the specified query.
   *
   * @param queryId
   * @param clerk
   */
  public void failFragments(
      QueryId queryId,
      QueriesClerk clerk,
      Throwable throwable,
      String failContext,
      HeapClawBackContext.Trigger trigger) {
    for (FragmentTicket fragmentTicket : clerk.getFragmentTickets(queryId)) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Failing Fragment {}", QueryIdHelper.getFragmentId(fragmentTicket.getHandle()));
      }
      failFragment(fragmentTicket.getHandle(), throwable, failContext, trigger);
    }
  }

  @VisibleForTesting
  void cancelFragment(FragmentHandle handle) {
    handlers.getUnchecked(handle).cancel();
    fragmentsRequestingCancellation.add(handle);
  }

  void failFragment(
      FragmentHandle handle,
      Throwable throwable,
      String failContext,
      HeapClawBackContext.Trigger trigger) {
    UserException.Builder builder;
    if (trigger == HeapClawBackContext.Trigger.MEMORY_ARBITER
        || ErrorHelper.isDirectMemoryException(throwable)) {
      builder =
          UserException.memoryError(throwable)
              .setAdditionalExceptionContext(
                  new OutOfMemoryOrResourceExceptionContext(
                      OutOfMemoryOrResourceExceptionContext.MemoryType.DIRECT_MEMORY, failContext));
      nodeDebugContext.addErrorOrigin(builder);
    } else {
      builder =
          UserException.memoryError(throwable)
              .setAdditionalExceptionContext(
                  new OutOfMemoryOrResourceExceptionContext(
                      OutOfMemoryOrResourceExceptionContext.MemoryType.HEAP_MEMORY, failContext));
      nodeDebugContext.addErrorOrigin(builder);
    }
    handlers.getUnchecked(handle).fail(builder.buildSilently());
  }

  public void receiverFinished(FragmentHandle sender, FragmentHandle receiver) {
    handlers.getUnchecked(sender).receiverFinished(receiver);
  }

  public void handle(FragmentHandle handle, FragmentStreamComplete completion) {
    handlers.getUnchecked(handle).handle(completion);
  }

  public void handle(FragmentHandle handle, IncomingDataBatch batch)
      throws IOException, FragmentSetupException {
    handlers.getUnchecked(handle).handle(batch);
  }

  public void handle(OutOfBandMessage message) {
    for (Integer minorFragmentId : message.getTargetMinorFragmentIds()) {
      FragmentHandle handle =
          FragmentHandle.newBuilder()
              .setQueryId(message.getQueryId())
              .setMajorFragmentId(message.getMajorFragmentId())
              .setMinorFragmentId(minorFragmentId)
              .build();
      handlers.getUnchecked(handle).handle(message);
    }
  }

  public void handle(DynamicLoadRoutingMessage message) {
    logger.error(
        "queryid {} command {}",
        QueryIdHelper.getQueryId(message.getQueryId()),
        message.getCommand());
    DlrMessageType dlrMessageType = DlrMessageType.values()[message.getCommand()];
    switch (dlrMessageType) {
      case ACTIVATE_FRAGMENTS:
        for (FragmentTicket fragmentTicket : clerk.getFragmentTickets(message.getQueryId())) {
          activateFragment(fragmentTicket.getHandle());
        }
        break;
      case RESERVE_RESOURCE:
        break;
      case RELEASE_RESOURCE:
        break;
      case CANCEL_FRAGMENTS:
        break;
      default:
        break;
    }
  }

  @Override
  public void close() throws Exception {
    // we could call handlers.cleanUp() to remove all expired elements but we don't really care as
    // we may still log a warning
    // anyway for fragments that finished less than 10 minutes ago (see
    // FragmentHandler.EVICTION_DELAY_MS)

    // retrieve all handlers that are either still running or didn't start at all
    Collection<FragmentHandler> unexpiredHandlers =
        FluentIterable.from(handlers.asMap().values())
            .filter(
                new Predicate<FragmentHandler>() {
                  @Override
                  public boolean apply(FragmentHandler input) {
                    return !input.hasStarted() || input.isRunning();
                  }
                })
            .toList();

    if (unexpiredHandlers.size() > 0) {
      logger.warn(
          "Closing FragmentExecutors but there are {} fragments that are either running or never started.",
          unexpiredHandlers.size());
      if (logger.isDebugEnabled()) {
        for (final FragmentHandler handler : unexpiredHandlers) {
          final FragmentExecutor executor = handler.getExecutor();
          if (executor != null) {
            logger.debug(
                "Fragment still running: {} status: {}",
                QueryIdHelper.getQueryIdentifier(handler.getHandle()),
                executor.getStatus());
          } else {
            handler.checkStateAndLogIfNecessary();
          }
        }
      }
    }

    AutoCloseables.close(handlers, memoryArbiter);
  }

  public void startFragmentOnLocal(
      PlanFragmentFull planFragmentFull, FragmentExecutorBuilder fragmentExecutorBuilder) {
    CoordExecRPC.InitializeFragments.Builder fb = CoordExecRPC.InitializeFragments.newBuilder();
    CoordExecRPC.PlanFragmentSet.Builder setB = fb.getFragmentSetBuilder();
    setB.addMajor(planFragmentFull.getMajor());
    setB.addMinor(planFragmentFull.getMinor());
    // No minor specific attributes since there is one minor
    // No endpoint index since this is supposed to run on localhost

    // Set the workload class to be background
    CoordExecRPC.SchedulingInfo.Builder scheduleB = fb.getSchedulingInfoBuilder();
    // Dont need to set queueId for WorkloadClass BACKGROUND
    scheduleB.setWorkloadClass(UserBitShared.WorkloadClass.BACKGROUND);

    CoordExecRPC.InitializeFragments initializeFragments = fb.build();

    UserBitShared.QueryId queryId = planFragmentFull.getHandle().getQueryId();
    maestroProxy.doNotTrack(queryId);

    startFragments(
        initializeFragments,
        fragmentExecutorBuilder,
        new StreamObserver<Empty>() {
          @Override
          public void onNext(Empty empty) {}

          @Override
          public void onError(Throwable throwable) {
            logger.error("Unable to execute query", throwable);
          }

          @Override
          public void onCompleted() {
            logger.info("Completed executing query");
          }
        },
        fragmentExecutorBuilder.getNodeEndpoint());

    activateFragments(queryId, fragmentExecutorBuilder.getClerk());
  }

  public QueryId getQueryIdForLocalQuery() {
    UUID queryUUID = UUID.randomUUID();
    return UserBitShared.QueryId.newBuilder()
        .setPart1(queryUUID.getMostSignificantBits())
        .setPart2(queryUUID.getLeastSignificantBits())
        .build();
  }

  private void startFragment(final FragmentExecutor executor, final boolean useMemoryArbiter) {
    final FragmentHandle fragmentHandle = executor.getHandle();
    numRunningFragments.incrementAndGet();
    final FragmentHandler handler = handlers.getUnchecked(fragmentHandle);

    // Create the task wrapper before adding the fragment to the list
    // of running fragments
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Starting fragment; Task weight W = {} H = {}",
          executor.getFragmentWeight(),
          QueryIdHelper.getQueryIdentifier(executor.getHandle()));
    }
    final AsyncTaskWrapper task =
        new AsyncTaskWrapper(
            executor.getSchedulingWeight(),
            executor.getSchedulingGroup(),
            executor.asAsyncTask(),
            () -> {
              fragmentsRequestingCancellation.remove(fragmentHandle);
              numRunningFragments.decrementAndGet();
              if (useMemoryArbiter) {
                memoryArbiter.taskDone(executor);
              }
              handler.invalidate();

              maestroProxy.markQueryAsDone(handler.getHandle().getQueryId());

              if (callback != null) {
                callback.indicateIfSafeToExit();
              }
            },
            warnMaxTime);

    handler.setExecutor(executor);
    pool.execute(task);
  }

  /** Initializes a query. Starts */
  private class QueryStarterImpl implements QueryStarter {
    final InitializeFragments initializeFragments;
    final FragmentExecutorBuilder builder;
    final StreamObserver<Empty> sender;
    final NodeEndpoint identity;
    final SchedulingInfo schedulingInfo;
    final CachedFragmentReader fragmentReader;
    final List<PlanFragmentFull> fullFragments;
    final boolean useWeightBasedScheduling;
    final boolean useMemoryArbiter;

    QueryStarterImpl(
        final InitializeFragments initializeFragments,
        final FragmentExecutorBuilder builder,
        final StreamObserver<Empty> sender,
        final NodeEndpoint identity,
        final SchedulingInfo schedulingInfo) {
      this.initializeFragments = initializeFragments;
      this.builder = builder;
      this.sender = sender;
      this.identity = identity;
      this.schedulingInfo = schedulingInfo;
      this.fragmentReader =
          new CachedFragmentReader(
              builder.getPlanReader(),
              new PlanFragmentsIndex(
                  initializeFragments.getFragmentSet().getEndpointsIndexList(),
                  initializeFragments.getFragmentSet().getAttrList()));
      final List<PlanFragmentFull> fragmentFulls = new ArrayList<>();

      // Create a map of the major fragments.
      PlanFragmentSet set = initializeFragments.getFragmentSet();
      Map<Integer, PlanFragmentMajor> map =
          FluentIterable.from(set.getMajorList())
              .uniqueIndex(major -> major.getHandle().getMajorFragmentId());

      // Build the full fragments.
      set.getMinorList()
          .forEach(
              minor -> {
                PlanFragmentMajor major = map.get(minor.getMajorFragmentId());
                Preconditions.checkNotNull(
                    major, "Missing major fragment for major id" + minor.getMajorFragmentId());

                fragmentFulls.add(new PlanFragmentFull(major, minor));
              });
      this.fullFragments = Collections.unmodifiableList(fragmentFulls);
      this.useWeightBasedScheduling =
          options.getOption(ExecConstants.SHOULD_ASSIGN_FRAGMENT_PRIORITY);
      this.useMemoryArbiter = options.getOption(ExecConstants.ENABLE_SPILLABLE_OPERATORS);
    }

    public PlanFragmentFull getFirstFragment() {
      return fullFragments.get(0);
    }

    @Override
    public boolean useWeightBasedScheduling() {
      return useWeightBasedScheduling;
    }

    @Override
    public int getApproximateQuerySize() {
      return fullFragments.size();
    }

    @Override
    @SuppressWarnings("DremioGRPCStreamObserverOnError")
    public void buildAndStartQuery(final QueryTicket queryTicket) {
      QueryId queryId = queryTicket.getQueryId();

      /*
       * To avoid race conditions between creation and deletion of phase/fragment tickets,
       * build all the fragments first (creates the tickets) and then, start the fragments (can
       * delete tickets).
       */
      List<FragmentExecutor> fragmentExecutors = new ArrayList<>();
      UserRpcException userRpcException = null;
      Set<FragmentHandle> fragmentHandlesForQuery = Sets.newHashSet();
      try {
        if (!maestroProxy.tryStartQuery(
            queryId, queryTicket, initializeFragments.getQuerySentTime())) {
          boolean isDuplicateStart = maestroProxy.isQueryStarted(queryId);
          if (isDuplicateStart) {
            // duplicate op, do nothing.
            return;
          } else {
            throw new IllegalStateException("query already cancelled");
          }
        }

        Map<Integer, Integer> priorityToWeightMap = buildPriorityToWeightMap();

        for (PlanFragmentFull fragment : fullFragments) {
          FragmentExecutor fe =
              buildFragment(
                  queryTicket,
                  fragment,
                  priorityToWeightMap.getOrDefault(fragment.getMajor().getFragmentExecWeight(), 1),
                  schedulingInfo);
          fragmentHandlesForQuery.add(fe.getHandle());
          fragmentExecutors.add(fe);
        }
      } catch (UserRpcException e) {
        userRpcException = e;
      } catch (Exception e) {
        userRpcException =
            new UserRpcException(NodeEndpoint.getDefaultInstance(), "Remote message leaked.", e);
      } finally {
        if (fragmentHandlesForQuery.size() > 0) {
          maestroProxy.initFragmentHandlesForQuery(queryId, fragmentHandlesForQuery);
        }
        // highest weight first with leaf fragments first bottom up
        fragmentExecutors.sort(weightBasedComparator());
        for (FragmentExecutor fe : fragmentExecutors) {
          startFragment(fe, useMemoryArbiter);
        }
        queryTicket.release();

        if (userRpcException == null) {
          sender.onNext(Empty.getDefaultInstance());
          sender.onCompleted();
        } else {
          sender.onError(userRpcException);
        }
      }

      // if there was a cancel while the start was in-progress, clean up.
      if (maestroProxy.isQueryCancelled(queryId)) {
        cancelFragments(queryId, builder.getClerk());
      }
    }

    @Override
    @SuppressWarnings("DremioGRPCStreamObserverOnError")
    public void unableToBuildQuery(Exception e) {
      if (e instanceof UserRpcException) {
        sender.onError((UserRpcException) e);
      } else {
        final UserRpcException genericException =
            new UserRpcException(NodeEndpoint.getDefaultInstance(), "Remote message leaked.", e);
        sender.onError(genericException);
      }
    }

    // TODO: DX-42847 will reduce the number of distinct weights used
    private Map<Integer, Integer> buildPriorityToWeightMap() {
      Map<Integer, Integer> priorityToWeightMap = new HashMap<>();
      if (fullFragments.get(0).getMajor().getFragmentExecWeight() <= 0) {
        return priorityToWeightMap;
      }

      for (PlanFragmentFull fragment : fullFragments) {
        int fragmentWeight = fragment.getMajor().getFragmentExecWeight();
        priorityToWeightMap.put(
            fragmentWeight, Math.min(fragmentWeight, QueryTicket.MAX_EXPECTED_SIZE));
      }
      return priorityToWeightMap;
    }

    private Comparator<FragmentExecutor> weightBasedComparator() {
      return (e1, e2) -> {
        if (e2.getFragmentWeight() < e1.getFragmentWeight()) {
          // higher priority first
          return -1;
        }
        if (e2.getFragmentWeight() == e1.getFragmentWeight()) {
          // when priorities are equal, order the fragments based on leaf first and then descending
          // order
          // of major fragment number. This ensures fragments are started in order of dependency
          if (e2.isLeafFragment() == e1.isLeafFragment()) {
            return Integer.compare(
                e2.getHandle().getMajorFragmentId(), e1.getHandle().getMajorFragmentId());
          } else {
            return e1.isLeafFragment() ? -1 : 1;
          }
        }
        return 1;
      };
    }

    private FragmentExecutor buildFragment(
        final QueryTicket queryTicket,
        final PlanFragmentFull fragment,
        final int schedulingWeight,
        final SchedulingInfo schedulingInfo)
        throws UserRpcException {

      if (fragment.getMajor().getFragmentExecWeight() <= 0) {
        logger.info(
            "Received remote fragment start instruction for {}",
            QueryIdHelper.getQueryIdentifier(fragment.getHandle()));
      } else {
        logger.info(
            "Received remote fragment start instruction for {} with assigned weight {} and scheduling weight {}",
            QueryIdHelper.getQueryIdentifier(fragment.getHandle()),
            fragment.getMajor().getFragmentExecWeight(),
            schedulingWeight);
      }

      try {
        final EventProvider eventProvider = getEventProvider(fragment.getHandle());
        return builder.build(
            queryTicket,
            fragment,
            schedulingWeight,
            useMemoryArbiter ? memoryArbiter : null,
            eventProvider,
            schedulingInfo,
            fragmentReader);
      } catch (final Exception e) {
        throw new UserRpcException(identity, "Failure while trying to start remote fragment", e);
      } catch (final OutOfMemoryError t) {
        if (t.getMessage().startsWith("Direct buffer")) {
          throw new UserRpcException(
              identity, "Out of direct memory while trying to start remote fragment", t);
        } else {
          throw t;
        }
      }
    }
  }

  @VisibleForTesting
  long getNumHandlers() {
    return handlers.size();
  }
}
