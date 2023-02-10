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
package com.dremio.service.commandpool;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.concurrent.ContextMigratingExecutorService;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.util.Closeable;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.opentracing.Tracer;

/**
 * Implementation of a bound {@link ReleasableCommandPool} where the threads that hold the slot can release the slot.<br>
 *
 * Uses a priority queue and relies on the {@link CommandPool.Command} comparator to define the priority of the waiting tasks.
 */
public class ReleasableBoundCommandPool implements ReleasableCommandPool {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReleasableBoundCommandPool.class);

  // Set of threads holding the slots. These are threads that currently have a slot in the CommandPool
  private final Set<Long> threadsHoldingSlots = Sets.newConcurrentHashSet();

  private final Tracer tracer;

  // Thread pool to submit the tasks that have acquired a slot
  private final ContextMigratingExecutorService<ThreadPoolExecutor> executorService;

  // Priority queue for the waiters waiting to acquire a slot in the command pool. This uses the same ordering
  // used by BoundCommandPool
  //
  // A simpler implementation would be to maintain an array of queues - one for each priority; and append a waiter
  // into the appropriate queue. Assuming the order of insertion is the same as the order of submission time, this is
  // a simple append. While picking tasks to process, pick from the head of the queue with highest priority
  // This change is not done as part of this check-in to minimise the risk of the change
  private final PriorityBlockingQueue<CommandWrapper<?>> priorityBlockingQueue = new PriorityBlockingQueue();

  // Queue of waiters who have already released the slot and are waiting to re-acquire the slot again
  // These waiters will have the highest priority while re-acquiring the slot
  private final LinkedBlockingQueue<ReacquiringWaiter> waitingToReacquire = new LinkedBlockingQueue<>();

  private final int maxPoolSize;

  // Number of slot holders. Not using the size of threadsHoldingSlots because the thread-id is added to the
  // set once the task is running in the executorService
  private int numSlotHolders = 0;

  ReleasableBoundCommandPool(final int poolSize, Tracer tracer) {
    this.maxPoolSize = poolSize;
    this.tracer = tracer;
    this.executorService = new ContextMigratingExecutorService<>(new ThreadPoolExecutor(
      poolSize,
      // the max pool size is unbounded. Consider all tasks submitting another task of low priority. In the worst
      // all tasks will hold a thread and submit a lower priority task
      Integer.MAX_VALUE,
      30, // Terminate idle threads after 30 seconds
      TimeUnit.SECONDS,
      new SynchronousQueue<>(), // queue requests
      new NamedThreadFactory("rbound-command")
    ), tracer);
  }

  @Override
  public void start() throws Exception {
    Metrics.newGauge(Metrics.join("jobs", "command_pool", "active_threads"), () -> numSlotHolders);
    Metrics.newGauge(Metrics.join("jobs", "command_pool", "queue_size"), () -> priorityBlockingQueue.size());
    Metrics.newGauge(Metrics.join("jobs", "command_pool", "reacquire_queue_size"), () -> waitingToReacquire.size());
  }

  // Invoked when one of the submitted tasks is complete; or when a thread holding a slot wants to
  // release the slot and re-acquire it later
  void onCommandDone() {
    CommandWrapper<?> wrapper = null;
    synchronized (this) {
      ReacquiringWaiter reacquiringWaiter = waitingToReacquire.poll();
      // check if there is a waiter waiting to re-acquire the command pool slot
      // not changing the numSlotHolders since the slot is re-assigned immediately
      if (reacquiringWaiter != null) {
        // reAcquirer has a thread already. It will continue execution from there
        logger.debug("Waking up re-acquiring waiter {}", reacquiringWaiter.threadId);
        reacquiringWaiter.complete(null);
        return;
      }

      // check if there is any other command waiting to acquire a command pool slot
      wrapper = priorityBlockingQueue.poll();
      if (wrapper == null) {
        // no other task to schedule
        // give up the slot
        numSlotHolders--;
      }
    }

    if (wrapper != null) {
      // schedule the next command
      logger.debug("Waking up waiter with description {}", wrapper.getDescriptor());
      executorService.submit(wrapper);
    }
  }

  // Release slot and re-assign slot to a waiter
  void releaseSlot() {
    onCommandDone();
  }

  <V> Command<V> getWrappedCommand(Command<V> command, boolean runInSameThread) {
    if (runInSameThread) {
      return command;
    }

    // Keep track of the threadsHoldingSlots
    return waitInMillis -> {
      long currentThreadId = Thread.currentThread().getId();
      Preconditions.checkArgument(threadsHoldingSlots.add(currentThreadId), "Thread already holds a slot. This should runInSameThread");
      try {
        return command.get(waitInMillis);
      } finally {
        Preconditions.checkArgument(threadsHoldingSlots.remove(currentThreadId), "Thread was supposed to hold the slot");
      }
    };
  }

  @VisibleForTesting
  Set<Long> getThreadsHoldingSlots() {
    return threadsHoldingSlots;
  }

  @VisibleForTesting
  int getNumWaiters() {
    return waitingToReacquire.size() + priorityBlockingQueue.size();
  }

  @VisibleForTesting
  int getReacquireWaiters() {
    return waitingToReacquire.size();
  }

  @Override
  public <V> CompletableFuture<V> submit(Priority priority, String descriptor, String spanName, Command<V> command, boolean runInSameThread) {
    final long submittedTime = System.currentTimeMillis();
    final CommandWrapper<V> wrapper = new CommandWrapper<V>(
      priority, descriptor, spanName, submittedTime, getWrappedCommand(command, runInSameThread)
    );

    if (runInSameThread) {
      try(Closeable childSpan = ContextMigratingExecutorService.getCloseableSpan(tracer, tracer.activeSpan(), spanName)) {
        wrapper.run();
        return wrapper.getFuture();
      }
    }

    // schedule the next command once this command is done
    final CompletableFuture<V> future = wrapper.getFuture().whenComplete((r, e) -> onCommandDone());

    synchronized (this) {
      if (numSlotHolders >= maxPoolSize) {
        // all slots are taken. Need to wait
        // add to priority queue based on priority order
        //
        // addition to the priority queue has to be done in the synchronised block
        // other wise, on a release when the releaser checks the queue, it might be empty
        priorityBlockingQueue.add(wrapper);
        return future;
      }

      numSlotHolders++;
    }

    // submit to the thread pool
    executorService.submit(wrapper);
    return future;
  }

  void addToReacquiringWaiters(ReacquiringWaiter waiter) {
    synchronized (this) {
      if (numSlotHolders >= maxPoolSize) {
        // no slot available
        // add to waiter
        logger.debug("Adding {} to re-acquiring waiter queue because there are no slots available", waiter.threadId);
        waitingToReacquire.add(waiter);
        return;
      }

      if (!waitingToReacquire.isEmpty()) {
        // there are others ahead in the queue
        logger.debug("Adding {} to re-acquiring waiter queue because there are others ahead", waiter.threadId);
        waitingToReacquire.add(waiter);
        return;
      }

      // there is a slot available for this waiter
      numSlotHolders++;
    }

    // complete the future for the re-acquiring waiter
    waiter.complete(null);
  }

  @Override
  public boolean amHoldingSlot() {
    long currentThreadId = Thread.currentThread().getId();
    return threadsHoldingSlots.contains(currentThreadId);
  }

  @Override
  public Closeable releaseAndReacquireSlot() {
    long currentThreadId = Thread.currentThread().getId();

    Preconditions.checkArgument(threadsHoldingSlots.remove(currentThreadId), "Invalid call to release command pool slot");
    // release the slot. This may cause other waiting tasks (re-acquirers or commands to be submitted) to be scheduled
    releaseSlot();
    logger.debug("Released command pool slot acquired for {}", currentThreadId);

    // closeable implementation
    // add self to re-acquirers queue and wait
    return () -> {
      ReacquiringWaiter waiter = new ReacquiringWaiter(currentThreadId);
      CompletableFuture<?> future = waiter.whenComplete((r, e) -> {
        Preconditions.checkArgument((e == null), "Unexpected exception while re-acquiring command pool slot after release");
        Preconditions.checkArgument(threadsHoldingSlots.add(currentThreadId), "Thread already holds a command pool slot while re-acquiring after release");
      });
      addToReacquiringWaiters(waiter);
      // wait for the wake-up
      // wait for the threadId to be added back to threadsHoldingSlots set as well
      future.join();
      logger.debug("Re-acquired the command pool slot for {}", currentThreadId);
    };
  }

  @Override
  public void close() throws Exception {
    CloseableSchedulerThreadPool.close(executorService, logger);
  }

  private class ReacquiringWaiter extends CompletableFuture<Void> {
    private final long threadId;

    ReacquiringWaiter(long threadId) {
      this.threadId = threadId;
    }
  }
}
