/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.CoordExecRPC.RpcType;
import com.dremio.exec.proto.CoordExecRPC.SchedulingInfo;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.metrics.Metrics;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.FragmentWorkManager.ExitCallback;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.exec.rpc.IncomingDataBatch;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.TaskPool;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;

/**
 * A type of map used to help manage fragments.
 */
public class FragmentExecutors implements AutoCloseable, Iterable<FragmentExecutor> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutors.class);
  private static final Response OK = new Response(RpcType.ACK, Acks.OK);

  private final LoadingCache<FragmentHandle, FragmentHandler> handlers = CacheBuilder.newBuilder()
    .build(new CacheLoader<FragmentHandle, FragmentHandler>() {
      @Override
      public FragmentHandler load(FragmentHandle key) throws Exception {
        return new FragmentHandler(key, evictionDelayMillis);
      }
    });
  private final AtomicInteger numRunningFragments = new AtomicInteger();

  private final CloseableSchedulerThreadPool scheduler = new CloseableSchedulerThreadPool("fragment-handler-cleaner", 1);

  private final TaskPool pool;
  private final ExitCallback callback;
  private final long evictionDelayMillis;

  public FragmentExecutors(
    final ExecToCoordTunnelCreator tunnelCreator,
    final ExitCallback callback,
    final TaskPool pool,
    final OptionManager options) {
    this.callback = callback;
    this.pool = pool;
    this.evictionDelayMillis = TimeUnit.SECONDS.toMillis(
      options.getOption(ExecConstants.FRAGMENT_CACHE_EVICTION_DELAY_S));

    Metrics.registerGauge(MetricRegistry.name("dremio.exec.work.running_fragments"), new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return size();
      }
    });

    initEvictionThread(evictionDelayMillis);
  }

  /**
   * schedules a thread that will asynchronously evict expired FragmentHandler from the cache.
   * First update will be scheduled after refreshDelayMs, and each subsequent update will start after
   * the previous update finishes + refreshDelayMs
   *
   * @param refreshDelayMs delay, in seconds, between successive eviction checks
   */
  private void initEvictionThread(long refreshDelayMs) {
    scheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
          for (FragmentHandler handler : handlers.asMap().values()) {
            try {
              if (handler.isExpired()) {
                handlers.invalidate(handler.getHandle());
              }
            } catch (Throwable e) {
              logger.warn("Failed to evict FragmentHandler for {}", QueryIdHelper.getQueryIdentifier(handler.getHandle()), e);
            }
          }
      }
    }, refreshDelayMs, refreshDelayMs, TimeUnit.MILLISECONDS);
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

  public void startQueryFragment(final InitializeFragments fragments, final FragmentExecutorBuilder builder,
                                 final ResponseSender sender, final NodeEndpoint identity) {
    final SchedulingInfo schedulingInfo = fragments.hasSchedulingInfo() ? fragments.getSchedulingInfo() : null;
    QueryStarterImpl queryStarter = new QueryStarterImpl(fragments, builder, sender, identity, schedulingInfo);
    builder.buildAndStartQuery(fragments.getFragment(0), schedulingInfo, queryStarter);
  }

  public EventProvider getEventProvider(FragmentHandle handle) {
    return handlers.getUnchecked(handle);
  }

  public void cancel(FragmentHandle handle) {
    handlers.getUnchecked(handle).cancel();
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

    AutoCloseables.close(scheduler);
  }

  /**
   * Initializes a query. Starts
   */
  private class QueryStarterImpl implements QueryStarter {
    final InitializeFragments fragments;
    final FragmentExecutorBuilder builder;
    final ResponseSender sender;
    final NodeEndpoint identity;
    final SchedulingInfo schedulingInfo;

    QueryStarterImpl(final InitializeFragments fragments, final FragmentExecutorBuilder builder,
                     final ResponseSender sender, final NodeEndpoint identity, final SchedulingInfo schedulingInfo) {
      this.fragments = fragments;
      this.builder = builder;
      this.sender = sender;
      this.identity = identity;
      this.schedulingInfo = schedulingInfo;
    }

    @Override
    public void buildAndStartQuery(final QueryTicket queryTicket) {
      try {
        for (int i = 0; i < fragments.getFragmentCount(); i++) {
          startFragment(queryTicket, fragments.getFragment(i), schedulingInfo);
        }
        sender.send(OK);
      } catch (UserRpcException e) {
        sender.sendFailure(e);
      } catch (Exception e) {
        final UserRpcException genericException = new UserRpcException(NodeEndpoint.getDefaultInstance(), "Remote message leaked.", e);
        sender.sendFailure(genericException);
      } finally {
        if (queryTicket != null) {
          queryTicket.release();
        }
      }
    }

    @Override
    public void unableToBuildQuery(Exception e) {
      if (e instanceof UserRpcException) {
        sender.sendFailure((UserRpcException) e);
      } else {
        final UserRpcException genericException = new UserRpcException(NodeEndpoint.getDefaultInstance(), "Remote message leaked.", e);
        sender.sendFailure(genericException);
      }
    }

    private void startFragment(final QueryTicket queryTicket, final PlanFragment fragment,
                               final SchedulingInfo schedulingInfo) throws UserRpcException {
      logger.info("Received remote fragment start instruction for {}", QueryIdHelper.getQueryIdentifier(fragment.getHandle()));

      try {
        final EventProvider eventProvider = getEventProvider(fragment.getHandle());
        startFragment(builder.build(queryTicket, fragment, eventProvider, schedulingInfo, fragments.getSharedDataList()));
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

            if (callback != null) {
              callback.indicateIfSafeToExit();
            }
          }
        });

      handler.setExecutor(executor);
      pool.execute(task);
    }
  }
}
