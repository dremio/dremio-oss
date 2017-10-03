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
package com.dremio.common.concurrent;

import com.dremio.common.AutoCloseables;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * AutoCloseable implementation of {@link ScheduledThreadPoolExecutor}
 */
public class CloseableSchedulerThreadPool extends ScheduledThreadPoolExecutor implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CloseableSchedulerThreadPool.class);

  public CloseableSchedulerThreadPool(String name, int corePoolSize) {
    super(corePoolSize, new NamedThreadFactory(name));
  }

  @Override
  protected void afterExecute(final Runnable r, final Throwable t) {
    if (t != null) {
      logger.error("{}.run() leaked an exception.", r.getClass().getName(), t);
    }
    super.afterExecute(r, t);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(this, logger);
  }
}
