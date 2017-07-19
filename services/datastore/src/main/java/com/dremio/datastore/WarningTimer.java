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
package com.dremio.datastore;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

/**
 * Fancy watch that computes the elapsed time between creation and closing and logs a warning if it exceeds
 * the given warnDelay
 */
public final class WarningTimer implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WarningTimer.class);

  private final Stopwatch stopwatch;
  private final String name;
  private final long warnDelay;

  public WarningTimer(String name, long warnDelay) {
    stopwatch = Stopwatch.createStarted();
    this.name = name;
    this.warnDelay = warnDelay;
  }

  @Override
  public void close() {
    long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    if (elapsed > warnDelay) {
      logger.warn("{} took {} ms", name, elapsed);
    }
  }
}
