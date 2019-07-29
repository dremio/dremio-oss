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
package com.dremio.service.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Unit tests for {@code SchedulerService}
 */
public class TestLocalSchedulerService {

  /**
   * Verify that {@code ExecutorService#shutdown()} is called when service is closed
   */
  @Test
  public void close() throws Exception {
    final CloseableSchedulerThreadPool executorService = mock(CloseableSchedulerThreadPool.class);
    final LocalSchedulerService service = new LocalSchedulerService(executorService, null, null, false);

    service.close();

    verify(executorService).close();
  }

  /**
   * Verify that threads created by the thread factory are daemon threads
   */
  @Test
  public void newThread() {
    LocalSchedulerService service = new LocalSchedulerService(1);
    final Runnable runnable = mock(Runnable.class);
    final Thread thread = service.getExecutorService().getThreadFactory().newThread(runnable);

    assertTrue("thread should be a daemon thread", thread.isDaemon());
    assertTrue("thread name should start with scheduler-", thread.getName().startsWith("scheduler-"));
  }

  private final class MockScheduledFuture<V> implements ScheduledFuture<V>, Callable<V> {
    private final Clock clock;
    private final Callable<V> callable;
    private final Instant instant;
    private final SettableFuture<V> delegate = SettableFuture.create();

    private MockScheduledFuture(Clock clock, Callable<V> callable, long delay, TimeUnit unit) {
      this.clock = clock;
      this.callable = callable;
      this.instant = Instant.now(clock).plus(unit.toNanos(delay), ChronoUnit.NANOS);
    }

    @Override
    public V call() throws Exception {
      synchronized(this) {
        if (delegate.isDone()) {
          throw new IllegalStateException("future already ran");
        }
        try {
          V v = callable.call();
          delegate.set(v);
          return v;
        } catch(Exception e) {
          delegate.setException(e);
          throw e;
        }
      }
    }

    @Override
    public long getDelay(TimeUnit u) {
      return u.convert(Instant.now(clock).until(instant, ChronoUnit.NANOS), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      return Long.compare(getDelay(TimeUnit.NANOSECONDS), o.getDelay(TimeUnit.NANOSECONDS));
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
      return delegate.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return delegate.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return delegate.get(timeout, unit);
    }
  }

  /**
   * Verify that {@code SchedulerService#schedule(Runnable, long, java.util.concurrent.TimeUnit)}
   * is calling underlying {@code ScheduledExecutorService}
   */
  @Test
  public void schedule() throws Exception {
    final List<MockScheduledFuture<?>> futures = Lists.newArrayList();
    final CloseableSchedulerThreadPool executorService = mock(CloseableSchedulerThreadPool.class);

    doAnswer(new Answer<ScheduledFuture<Object>>() {
      @Override
      public ScheduledFuture<Object> answer(InvocationOnMock invocation) throws Throwable {
        final Object[] arguments = invocation.getArguments();
        MockScheduledFuture<Object> result =  new MockScheduledFuture<>(Clock.systemUTC(), Executors.callable((Runnable) arguments[0]), (long) arguments[1], (TimeUnit) arguments[2]);
        futures.add(result);

        return result;
      }
    }).when(executorService).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

    //new MockScheduledExecutorService();
    final AtomicInteger runCount = new AtomicInteger(0);
    final Runnable runnable = new Runnable() {
      @Override
      public void run() { runCount.incrementAndGet(); }
    };

    @SuppressWarnings("resource")
    final SchedulerService service = new LocalSchedulerService(executorService, null, null, false);
    @SuppressWarnings("unused")
    final Cancellable cancellable = service.schedule(Schedule.Builder.everyHours(1).build(), runnable);

    // Making a copy as running pending future will alter the list
    ImmutableList<MockScheduledFuture<?>> copyOfFutures = ImmutableList.copyOf(futures);
    for(MockScheduledFuture<?> future: copyOfFutures) {
      future.call();
    }
    // There should be two futures in the list, one set, one still pending
    assertEquals(2, futures.size());
    assertTrue("1st future should be completed", futures.get(0).isDone());
    assertFalse("2nd future should still be pending", futures.get(1).isDone());
    assertTrue("1st future delay should be shorted than 2nd future delay", futures.get(0).compareTo(futures.get(1)) < 0);
    assertEquals(1, runCount.get());
  }

  /**
   * Verify that cancelling before the task is run prevent it to run when executing
   */
  @Test
  public void scheduleCancelledBeforeRun() throws Exception {
    final List<MockScheduledFuture<?>> futures = Lists.newArrayList();
    final CloseableSchedulerThreadPool executorService = mock(CloseableSchedulerThreadPool.class);

    doAnswer(new Answer<ScheduledFuture<Object>>() {
      @Override
      public ScheduledFuture<Object> answer(InvocationOnMock invocation) throws Throwable {
        final Object[] arguments = invocation.getArguments();
        MockScheduledFuture<Object> result =  new MockScheduledFuture<>(Clock.systemUTC(), Executors.callable((Runnable) arguments[0]), (long) arguments[1], (TimeUnit) arguments[2]);
        futures.add(result);

        return result;
      }
    }).when(executorService).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));


    //new MockScheduledExecutorService();
    final AtomicInteger runCount = new AtomicInteger(0);
    final Runnable runnable = new Runnable() {
      @Override
      public void run() { runCount.incrementAndGet(); }
    };

    @SuppressWarnings("resource")
    final SchedulerService service = new LocalSchedulerService(executorService, null, null, false);
    final Cancellable cancellable = service.schedule(Schedule.Builder.everyHours(1).build(), runnable);
    cancellable.cancel(true);

    // Making a copy as running pending future will alter the list
    ImmutableList<MockScheduledFuture<?>> copyOfFutures = ImmutableList.copyOf(futures);
    for(MockScheduledFuture<?> future: copyOfFutures) {
      try {
        future.call();
      } catch (IllegalStateException e) {
        // it should have been cancelled already
      }
    }

    // There should be one future in the list, one set
    assertTrue("Cancellable should have been cancelled", cancellable.isCancelled());
    assertEquals(1, futures.size());
    assertTrue("1st future should be completed", futures.get(0).isDone());
    assertEquals(0, runCount.get());
  }

  /**
   * Verify that cancelling before the task is run prevent it to reschedule itself
   */
  @Test
  public void scheduleCancelledDuringRun() throws Exception {
    final List<MockScheduledFuture<?>> futures = Lists.newArrayList();
    final CloseableSchedulerThreadPool executorService = mock(CloseableSchedulerThreadPool.class);

    doAnswer(new Answer<ScheduledFuture<Object>>() {
      @Override
      public ScheduledFuture<Object> answer(InvocationOnMock invocation) throws Throwable {
        final Object[] arguments = invocation.getArguments();
        MockScheduledFuture<Object> result =  new MockScheduledFuture<>(Clock.systemUTC(), Executors.callable((Runnable) arguments[0]), (long) arguments[1], (TimeUnit) arguments[2]);
        futures.add(result);

        return result;
      }
    }).when(executorService).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));


    //new MockScheduledExecutorService();
    final CountDownLatch ready = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);
    final Runnable runnable = new Runnable() {
      @Override
      public void run() {
        ready.countDown();
        try {
          done.await();
        } catch (InterruptedException e) {
          // nothing
        }
      }
    };

    @SuppressWarnings("resource")
    final SchedulerService service = new LocalSchedulerService(executorService, null, null, false);
    final Cancellable cancellable = service.schedule(Schedule.Builder.everyHours(1).build(), runnable);

    // Making a copy as running pending future will alter the list
    ImmutableList<MockScheduledFuture<?>> copyOfFutures = ImmutableList.copyOf(futures);
    for(final MockScheduledFuture<?> future: copyOfFutures) {
      try {
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              future.call();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }, "test-TestLocalScheduler").start();
      } catch (IllegalStateException e) {
        // it should have been cancelled already
      }
    }


    ready.await(1, TimeUnit.SECONDS);

    cancellable.cancel(true);

    done.countDown();

    // There should be one future in the list, one set
    assertTrue("Cancellable should have been cancelled", cancellable.isCancelled());
    assertEquals(1, futures.size());
    assertTrue("1st future should be completed", futures.get(0).isDone());
  }
}
