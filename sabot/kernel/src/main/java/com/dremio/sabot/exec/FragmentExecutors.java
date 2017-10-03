/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.FragmentSetupException;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.metrics.Metrics;
import com.dremio.sabot.exec.FragmentWorkManager.ExitCallback;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
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

import javax.annotation.Nullable;

/**
 * A type of map used to help manage fragments.
 */
public class FragmentExecutors implements AutoCloseable, Iterable<FragmentExecutor> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutors.class);

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

  public void startFragment(final FragmentExecutor executor) {
    final FragmentHandle fragmentHandle = executor.getHandle();
    numRunningFragments.incrementAndGet();
    final FragmentHandler handler = handlers.getUnchecked(fragmentHandle);

    // Create the task wrapper before adding the fragment to the list
    // of running fragments
    final AsyncTaskWrapper task = new AsyncTaskWrapper(
        executor.getPriority(),
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


}
