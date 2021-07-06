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
package com.dremio.service.namespace;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;

/**
 * Optimises for batch lookups. Not multi-thread safe.
 */
public class BatchLookupOptimiser<K, V> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchLookupOptimiser.class);
  private static final int BATCH_SIZE = 100;
  private final Set<K> keySet = new HashSet<>();
  private final Cache<K, Optional<V>> cache = CacheBuilder.newBuilder()
    .expireAfterWrite(Duration.ofSeconds(300))
    .build();
  private final Function<List<K>, List<V>> bulkGetter;
  private final AtomicLong numMisses = new AtomicLong();
  private volatile boolean fetchAllDone;

  BatchLookupOptimiser(Function<List<K>, List<V>> bulkGetter) {
    Preconditions.checkNotNull(bulkGetter);
    this.bulkGetter = bulkGetter;
  }

  void mayLookup(K key) {
    keySet.add(key);
  }

  V lookup(K key) {
    checkAndFetchAll();

    // fetch from cache, or populate.
    try {
      Optional<V> value = cache.get(key, () -> {
        numMisses.getAndIncrement();
        logger.debug("unexpected cache miss after batch lookup, the cache expiry interval may need to be increased");
        return Optional.ofNullable(bulkGetter.apply(Collections.singletonList(key)).get(0));
      });
      return value.orElse(null);
    } catch (ExecutionException ex) {
      // not expected
      throw new RuntimeException(ex);
    }
  }

  void checkAndFetchAll() {
    if (fetchAllDone) {
      return;
    }

    // fetch in batches, and add to cache.
    for (List<K> batch : Iterables.partition(keySet, BATCH_SIZE)) {
      logger.debug("batch lookup of size {}", batch.size());
      Iterator<V> values = bulkGetter.apply(batch).iterator();
      for (K key : batch) {
        // using Optional so that the cache can have null values too.
        cache.put(key, Optional.ofNullable(values.next()));
      }
    }
    fetchAllDone = true;
  }

  long getNumMisses() {
    return numMisses.get();
  }
}
