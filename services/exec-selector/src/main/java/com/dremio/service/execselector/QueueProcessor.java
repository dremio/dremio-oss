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
package com.dremio.service.execselector;

import com.dremio.common.concurrent.AutoCloseableLock;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Processes events serially, in a separate thread. This is nothing but a wrapper around a
 * BlockingQueue Thread is spawned on 'start', and reaped at close
 *
 * @param <T> the event class
 */
public class QueueProcessor<T> implements AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(QueueProcessor.class);

  private final String name;
  private final Supplier<AutoCloseableLock> lockSupplier;
  private final Consumer<T> consumer;
  private final BlockingQueue<T> queue;
  private boolean completed;
  private Thread workerThread;
  private boolean isClosed;

  /**
   * Initialize a queue processor. When started, the thread that processes the events will be
   * renamed to 'name'
   *
   * @param name Name given to the thread that will process the events
   * @param lockSupplier Invoked before processing a batch of events. Individual calls to the
   *     consumer will be made under this lock. The lock will be closed once the events have been
   *     processed
   * @param consumer Invoked for every event found on the queue The consumer should not throw any
   *     exceptions. Any exceptions it does throw will be caught and ignored
   */
  public QueueProcessor(
      String name, Supplier<AutoCloseableLock> lockSupplier, Consumer<T> consumer) {
    this.name = name;
    this.lockSupplier = lockSupplier;
    this.consumer = consumer;
    this.queue = new LinkedBlockingQueue<>();
    this.workerThread = null;
    this.isClosed = false;
    this.completed = true;
  }

  /**
   * Add 'event' to the queue. The queue processor need not be started for events to be processed in
   * the queue
   */
  public void enqueue(T event) {
    try {
      this.completed = false;
      queue.put(event);
    } catch (InterruptedException e) {
      // Will not happen, because the queue is unlimited
    }
  }

  /** Spawn the event processing thread and start processing events. */
  public void start() {
    Preconditions.checkState(workerThread == null, "Queue processor already started");
    workerThread =
        new Thread(
            () -> {
              while (!isClosed) {
                try {
                  processBatch();
                } catch (InterruptedException e) {
                  // will definitely happen on close, but safe to ignore otherwise
                } catch (Exception e) {
                  logger.warn("Unhandled exception in queue processor {}", name, e);
                }
              }
            },
            name);
    workerThread.start();
  }

  private void processBatch() throws InterruptedException {
    T event = queue.take();
    try (AutoCloseableLock ignored = lockSupplier.get()) {
      while (event != null) {
        consumer.accept(event);
        event = queue.poll(0, TimeUnit.NANOSECONDS);
      }
      this.completed = true;
    }
  }

  @VisibleForTesting
  public boolean completed() {
    return completed;
  }

  /**
   * Reaps the event processing thread. The event processing thread might not finish processing the
   * events on the queue
   */
  @Override
  public void close() throws Exception {
    if (!isClosed) {
      isClosed = true;
      if (workerThread != null) {
        workerThread.interrupt();
        workerThread.join();
      }
    }
  }
}
