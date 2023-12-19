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
package com.dremio.common.util;

import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.ForwardingLoadingCache;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;

/**
 * Loading cache while periodically checks and invalidates expired entries.
 *
 * @param <K> key
 * @param <V> value (should implement MayExpire)
 */
public class LoadingCacheWithExpiry<K, V extends MayExpire> extends ForwardingLoadingCache<K, V> implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoadingCacheWithExpiry.class);
  private final LoadingCache<K, V> inner;
  private final CloseableSchedulerThreadPool scheduler;

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  public LoadingCacheWithExpiry(String name, CacheLoader<K, V> loader, RemovalListener removalListener, long refreshDelayMs) {
    if (removalListener != null) {
      this.inner = CacheBuilder.newBuilder().removalListener(removalListener).build(loader);
    } else {
      this.inner = CacheBuilder.newBuilder().build(loader);
    }
    this.scheduler = new CloseableSchedulerThreadPool("cache-cleaner-" + name, 1);
    initEvictionAction(refreshDelayMs);
  }

  /**
   * schedules a thread that will asynchronously evict expired FragmentHandler from the cache.
   * First update will be scheduled after refreshDelayMs, and each subsequent update will start after
   * the previous update finishes + refreshDelayMs
   *
   * @param refreshDelayMs delay, in milliseconds, between successive eviction checks
   */
  private void initEvictionAction(long refreshDelayMs) {
    scheduler.scheduleWithFixedDelay(getEvictionAction(), refreshDelayMs, refreshDelayMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Do one round of eviction.
   */
  public void checkAndEvict() {
    getEvictionAction().run();
  }

  private Runnable getEvictionAction() {
    return () -> {
      for (Entry<K, V> entry : inner.asMap().entrySet()) {
        try {
          if (entry.getValue().isExpired()) {
            inner.invalidate(entry.getKey());
          }
        } catch (Throwable e) {
          logger.warn("Failed to evict entry");
        }
      }
    };
  }

  @Override
  protected LoadingCache<K, V> delegate() {
    return inner;
  }

  @Override
  public void close() throws Exception {
    inner.invalidateAll();
    AutoCloseables.close(scheduler);
  }
}
