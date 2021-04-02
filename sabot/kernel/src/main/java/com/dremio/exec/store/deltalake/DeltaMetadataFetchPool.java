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

package com.dremio.exec.store.deltalake;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.google.common.annotations.VisibleForTesting;

/**
 * Singleton thread pool used by {@link DeltaMetadataFetchJobManager} to run different {@link DeltaMetadataFetchJob} instances in parallel.
 */

public class DeltaMetadataFetchPool {
  @VisibleForTesting
  static Integer POOL_SIZE = 40;

  private static class LazyThreadPoolHolder {
    static final ThreadPoolExecutor THREAD_POOL = new ThreadPoolExecutor(0, POOL_SIZE, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new NamedThreadFactory("delta-metadata-fetch"));
  }

  public static ThreadPoolExecutor getPool() {
    return LazyThreadPoolHolder.THREAD_POOL;
  }
}
