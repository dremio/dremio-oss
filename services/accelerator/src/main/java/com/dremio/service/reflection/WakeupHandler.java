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
package com.dremio.service.reflection;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

/**
 * handles wakeup events for the {@link ReflectionManager}.
 *
 * Ensures only a single instance of the manager is running and that no wakeup event is lost.
 */
public class WakeupHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WakeupHandler.class);

  private final AtomicBoolean wakeup = new AtomicBoolean();
  private final AtomicBoolean running = new AtomicBoolean();

  private final ReflectionManager manager;
  private final ExecutorService executor;

  WakeupHandler(ExecutorService executor, ReflectionManager manager) {
    this.executor = Preconditions.checkNotNull(executor, "executor service required");
    this.manager = Preconditions.checkNotNull(manager, "reflection manager required");
  }

  public void handle(String reason) {
    logger.debug("waking up manager, reason: {}", reason);
    if (!wakeup.compareAndSet(false, true)) {
      return; // wakeup event already queued for handling, nothing more to do
    }
    // following check if not necessary. It helps not submitting a thread if the manager is already running
    if (running.get()) {
      return;
    }

    executor.submit(new Runnable() {

      @Override
      public void run() {
        while (wakeup.get()) {
          if (!running.compareAndSet(false, true)) {
            return; // another thread is already running the manager
          }

          try {
            wakeup.set(false);
            manager.run();
          } finally {
            running.set(false);
          }
        }
        // thread can only exit if both wakeup and running are set to false. This ensures we never miss a wakeup event
      }

    });
  }
}
