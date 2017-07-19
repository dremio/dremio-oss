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
package com.dremio.service.accelerator;

import java.util.concurrent.TimeUnit;

import org.threeten.bp.temporal.TemporalAmount;

import com.google.common.base.Stopwatch;

/**
 * Async garbage collection task
 */
class GarbageCollectorTask implements Runnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GarbageCollectorTask.class);

  private final AccelerationServiceImpl acceleratorService;
  private final TemporalAmount gracePeriod;

  public GarbageCollectorTask(final AccelerationServiceImpl acceleratorService, final TemporalAmount gracePeriod) {
    this.acceleratorService = acceleratorService;
    this.gracePeriod = gracePeriod;
  }

  @Override
  public void run() {
    Stopwatch sw = Stopwatch.createStarted();
    logger.debug("Starting accelerator compaction");
    try {
      acceleratorService.compact(gracePeriod);
    } catch(Exception e) {
      logger.warn("Accelerator compaction failed", e);
    }

    logger.debug("Accelerator compaction completed. Took {} ms.", sw.elapsed(TimeUnit.MILLISECONDS));
  }

}
