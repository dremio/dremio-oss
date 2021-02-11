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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.LoadingCacheWithExpiry;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.planner.fragment.CachedFragmentReader;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.fragment.PlanFragmentsIndex;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentSet;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordExecRPC.SchedulingInfo;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.FragmentWorkManager.ExitCallback;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.rpc.IncomingDataBatch;
import com.dremio.sabot.task.AsyncTaskWrapper;
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

/**
 * A type of map used to help manage fragments.
 */
public class FragmentExecutors implements AutoCloseable, Iterable<FragmentExecutor> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutors.class);
  private static final Response OK = new Response(RpcType.ACK, Acks.OK);

  private final LoadingCacheWithExpiry<FragmentHandle, FragmentHandler> handlers;
  private final AtomicInteger numRunningFragments = new AtomicInteger();

  private final TaskPool pool;
  private final ExitCallback callback;
  private final long evictionDelayMillis;
  private final MaestroProxy maestroProxy;
  private final int warnMaxTime;

  public FragmentExecutors(
    final MaestroProxy maestroProxy,
    final ExitCallback callback,
    final TaskPool pool,
    final OptionManager options) {
    this.maestroProxy = maestroProxy;
    this.callback = callback;
    this.pool = pool;
    this.evictionDelayMillis = TimeUnit.SECONDS.toMillis(
      options.getOption(ExecConstants.FRAGMENT_CACHE_EVICTION_DELAY_S));

    this.handlers = new LoadingCacheWithExpiry<>("fragment-handler",
      new CacheLoader<FragmentHandle, FragmentHandler>() {
        @Override
        public FragmentHandler load(FragmentHandle key) throws Exception {
          // Underlying loading cache's refresh() calls reload() on this
          // cacheLoader, which indirectly calls this load() method.
          // So new FragmentHandler should not be created, instead
          // existing handler should be used. This will avoid using
          // extra heap memory.
          FragmentHandler exitingFragmentHandler = handlers.getIfPresent(key);
          if(exitingFragmentHandler == null) {
            return new FragmentHandler(key, evictionDelayMillis);
          }
          return exitingFragmentHandler;
        }
      },
      null, evictionDelayMillis);

    this.warnMaxTime = (int) options.getOption(ExecConstants.SLICING_WARN_MAX_RUNTIME_MS);
  }

  @VisibleForTesting
  void checkAndEvict() {
    handlers.checkAndEvict();
  }

  @Override
  public Iterator<FragmentExecutor> iterator() {
    return Iterators.unmodifiableIterator(
      FluentIterable
        .from(handlers.asMap().values())
        .transform(new Function<FragmentHandler, FragmentExecutor>() {
          @Nullable
          @Override
          public FragmentExecutor apply(FragmentHandler input) {
            return input.getExecutor();
          }
        })
        .filter(Predicates.<FragmentExecutor>notNull())
        .iterator()
    );
  }

  /**
   * @return number of running fragments
   */
  public int size() {
    return numRunningFragments.get();
  }

  public void startFragments(final InitializeFragments fragments, final FragmentExecutorBuilder builder,
                             final StreamObserver<Empty> sender, final NodeEndpoint identity) {
    final SchedulingInfo schedulingInfo = fragments.hasSchedulingInfo() ? fragments.getSchedulingInfo() : null;
    QueryStarterImpl queryStarter = new QueryStarterImpl(fragments, builder, sender, identity, schedulingInfo);
    builder.buildAndStartQuery(queryStarter.getFirstFragment(), schedulingInfo, queryStarter);
  }

  public EventProvider getEventProvider(FragmentHandle handle) {
    return handlers.getUnchecked(handle);
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
    for (FragmentTicket fragmentTicket : clerk.getFragmentTickets(queryId)) {
      activateFragment(fragmentTicket.getHandle());
    }
  }

  @VisibleForTesting
  void activateFragment(FragmentHandle handle) { handlers.getUnchecked(handle).activate(); }

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
   * Determine queries to be cancelled based on activeQueryList and
   * cancel those queries. See implementation for actual algo.
   * @param activeQueryList
   * @param clerk
   */
  public void reconcileActiveQueries(CoordExecRPC.ActiveQueryList activeQueryList, QueriesClerk clerk) {
    Set<QueryId> queryIdsToCancel = maestroProxy.reconcileActiveQueries(activeQueryList);
    cancelFragments(queryIdsToCancel, clerk);
  }

  @VisibleForTesting
  void cancelFragments(Set<QueryId> queryIdsToCancel, QueriesClerk clerk) {
    logger.debug("# queries to be cancelled:{}", queryIdsToCancel);
    for(QueryId queryId: queryIdsToCancel) {
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
  public void failFragments(QueryId queryId, QueriesClerk clerk, Throwable throwable, String failContext) {
    for (FragmentTicket fragmentTicket : clerk.getFragmentTickets(queryId)) {
      failFragment(fragmentTicket.getHandle(), throwable, failContext);
    }
  }

  @VisibleForTesting
  void cancelFragment(FragmentHandle handle) { handlers.getUnchecked(handle).cancel(); }

  void failFragment(FragmentHandle handle, Throwable throwable, String failContext) {
    UserException.Builder builder = UserException
      .resourceError(throwable)
      .message(UserException.MEMORY_ERROR_MSG);
    if (failContext != null && !failContext.isEmpty()) {
      builder = builder.addContext(failContext);
    }
    handlers.getUnchecked(handle).fail(builder.buildSilently());
  }

  public void receiverFinished(FragmentHandle sender, FragmentHandle receiver) {
    handlers.getUnchecked(sender).receiverFinished(receiver);
  }

  public void handle(FragmentHandle handle, FragmentStreamComplete completion) {
    handlers.getUnchecked(handle).handle(completion);
  }

  public void handle(FragmentHandle handle, IncomingDataBatch batch) throws IOException, FragmentSetupException {
    handlers.getUnchecked(handle).handle(batch);
  }

  public void handle(OutOfBandMessage message) {
    for(Integer minorFragmentId : message.getTargetMinorFragmentIds()) {
      FragmentHandle handle = FragmentHandle.newBuilder().setQueryId(message.getQueryId()).setMajorFragmentId(message.getMajorFragmentId()).setMinorFragmentId(minorFragmentId).build();
      handlers.getUnchecked(handle).handle(message);
    }
  }

  @Override
  public void close() throws Exception {
    // we could call handlers.cleanUp() to remove all expired elements but we don't really care as we may still log a warning
    // anyway for fragments that finished less than 10 minutes ago (see FragmentHandler.EVICTION_DELAY_MS)

    // retrieve all handlers that are either still running or didn't start at all
    Collection<FragmentHandler> unexpiredHandlers = FluentIterable
      .from(handlers.asMap().values())
      .filter(new Predicate<FragmentHandler>() {
        @Override
        public boolean apply(FragmentHandler input) {
          return !input.hasStarted() || input.isRunning();
        }
      }).toList();

    if (unexpiredHandlers.size() > 0) {
      logger.warn("Closing FragmentExecutors but there are {} fragments that are either running or never started.", unexpiredHandlers.size());
      if (logger.isDebugEnabled()) {
        for (final FragmentHandler handler : unexpiredHandlers) {
          final FragmentExecutor executor = handler.getExecutor();
          if (executor != null) {
            logger.debug("Fragment still running: {} status: {}", QueryIdHelper.getQueryIdentifier(handler.getHandle()),
              executor.getStatus());
          } else {
            handler.checkStateAndLogIfNecessary();
          }
        }
      }
    }

    AutoCloseables.close(handlers);
  }

  public void startFragmentOnLocal(PlanFragmentFull planFragmentFull, FragmentExecutorBuilder fragmentExecutorBuilder) {
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

    startFragments(initializeFragments, fragmentExecutorBuilder, new StreamObserver<Empty>() {
      @Override
      public void onNext(Empty empty) {}

      @Override
      public void onError(Throwable throwable) {
        logger.error("Unable to execute query due to {}", throwable);
      }

      @Override
      public void onCompleted() {
        logger.info("Completed executing query");
      }
    }, fragmentExecutorBuilder.getNodeEndpoint());

    activateFragments(queryId, fragmentExecutorBuilder.getClerk());
  }

  public QueryId getQueryIdForLocalQuery() {
    UUID queryUUID = UUID.randomUUID();
    return UserBitShared.QueryId.newBuilder()
      .setPart1(queryUUID.getMostSignificantBits())
      .setPart2(queryUUID.getLeastSignificantBits())
      .build();
  }

  /**
   * Initializes a query. Starts
   */
  private class QueryStarterImpl implements QueryStarter {
    final InitializeFragments initializeFragments;
    final FragmentExecutorBuilder builder;
    final StreamObserver<Empty> sender;
    final NodeEndpoint identity;
    final SchedulingInfo schedulingInfo;
    final CachedFragmentReader fragmentReader;
    List<PlanFragmentFull> fullFragments;

    QueryStarterImpl(final InitializeFragments initializeFragments, final FragmentExecutorBuilder builder,
                     final StreamObserver<Empty> sender, final NodeEndpoint identity, final SchedulingInfo schedulingInfo) {
      this.initializeFragments = initializeFragments;
      this.builder = builder;
      this.sender = sender;
      this.identity = identity;
      this.schedulingInfo = schedulingInfo;
      this.fragmentReader = new CachedFragmentReader(builder.getPlanReader(),
        new PlanFragmentsIndex(initializeFragments.getFragmentSet().getEndpointsIndexList(),
          initializeFragments.getFragmentSet().getAttrList()));
      this.fullFragments = new ArrayList<>();

      // Create a map of the major fragments.
      PlanFragmentSet set = initializeFragments.getFragmentSet();
      Map<Integer, PlanFragmentMajor> map = FluentIterable.from(set.getMajorList())
        .uniqueIndex(major -> major.getHandle().getMajorFragmentId());

      // Build the full fragments.
      set.getMinorList().forEach(
        minor -> {
          PlanFragmentMajor major = map.get(minor.getMajorFragmentId());
          Preconditions.checkNotNull(major,
            "Missing major fragment for major id" + minor.getMajorFragmentId());

          fullFragments.add(new PlanFragmentFull(major, minor));
        });
    }

    public PlanFragmentFull getFirstFragment() {
      return fullFragments.get(0);
    }

    @Override
    public void buildAndStartQuery(final QueryTicket queryTicket) {
      QueryId queryId = queryTicket.getQueryId();

      /**
       * To avoid race conditions between creation and deletion of phase/fragment tickets,
       * build all the fragments first (creates the tickets) and then, start the fragments (can
       * delete tickets).
       */
      List<FragmentExecutor> fragmentExecutors = new ArrayList<>();
      UserRpcException userRpcException = null;
      Set<FragmentHandle> fragmentHandlesForQuery = Sets.newHashSet();
      try {
        if (!maestroProxy.tryStartQuery(queryId, queryTicket, initializeFragments.getQuerySentTime())) {
          boolean isDuplicateStart = maestroProxy.isQueryStarted(queryId);
          if (isDuplicateStart) {
            // duplicate op, do nothing.
            return;
          } else {
            throw new IllegalStateException("query already cancelled");
          }
        }
        for (PlanFragmentFull fragment : fullFragments) {
          FragmentExecutor fe = buildFragment(queryTicket, fragment, schedulingInfo);
          fragmentHandlesForQuery.add(fe.getHandle());
          fragmentExecutors.add(fe);
        }
      } catch (UserRpcException e) {
        userRpcException = e;
      } catch (Exception e) {
        userRpcException = new UserRpcException(NodeEndpoint.getDefaultInstance(), "Remote message leaked.", e);
      } finally {
        if (fragmentHandlesForQuery.size() > 0) {
          maestroProxy.initFragmentHandlesForQuery(queryId, fragmentHandlesForQuery);
        }
        for (FragmentExecutor fe : fragmentExecutors) {
          startFragment(fe);
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
    public void unableToBuildQuery(Exception e) {
      if (e instanceof UserRpcException) {
        sender.onError((UserRpcException) e);
      } else {
        final UserRpcException genericException = new UserRpcException(NodeEndpoint.getDefaultInstance(), "Remote message leaked.", e);
        sender.onError(genericException);
      }
    }

    private FragmentExecutor buildFragment(final QueryTicket queryTicket, final PlanFragmentFull fragment,
      final SchedulingInfo schedulingInfo) throws UserRpcException {

      logger.info("Received remote fragment start instruction for {}", QueryIdHelper.getQueryIdentifier(fragment.getHandle()));

      try {
        final EventProvider eventProvider = getEventProvider(fragment.getHandle());
        return builder.build(queryTicket, fragment, eventProvider, schedulingInfo, fragmentReader);
      } catch (final Exception e) {
        throw new UserRpcException(identity, "Failure while trying to start remote fragment", e);
      } catch (final OutOfMemoryError t) {
        if (t.getMessage().startsWith("Direct buffer")) {
          throw new UserRpcException(identity, "Out of direct memory while trying to start remote fragment", t);
        } else {
          throw t;
        }
      }
    }

    public void startFragment(final FragmentExecutor executor) {
      final FragmentHandle fragmentHandle = executor.getHandle();
      numRunningFragments.incrementAndGet();
      final FragmentHandler handler = handlers.getUnchecked(fragmentHandle);

      // Create the task wrapper before adding the fragment to the list
      // of running fragments
      final AsyncTaskWrapper task = new AsyncTaskWrapper(
        executor.getSchedulingGroup(),
        executor.asAsyncTask(),
        new AutoCloseable() {

          @Override
          public void close() throws Exception {
            numRunningFragments.decrementAndGet();
            handler.invalidate();

            maestroProxy.markQueryAsDone(handler.getHandle().getQueryId());

            if (callback != null) {
              callback.indicateIfSafeToExit();
            }
          }
        },
        warnMaxTime);

      handler.setExecutor(executor);
      pool.execute(task);
    }
  }

  @VisibleForTesting
  long getNumHandlers() {
    return handlers.size();
  }
}
