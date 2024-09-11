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
package org.apache.arrow.memory;

import com.dremio.common.VM;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.config.DremioConfig;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;

public final class RootAllocatorFactory {

  private static final String IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY =
      "io.netty.allocator.useCacheForAllThreads";

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(RootAllocatorFactory.class);

  public static final String TOP_LEVEL_MAX_ALLOC = "dremio.memory.top.max";

  /**
   * Internal factory class to configure Netty allocator *only once*. This class is loaded lazily
   * after the first call to {@code RootAllocatorFactory#newRoot()}
   */
  private static final class InternalFactory {
    static {
      // DX-11065: the Netty 4.1 caching algorithm changes causes more objects to be cached, which
      // in turn causes the
      // DirectMemoryArena(s) to contain a lot of PoolChunk objects that are mostly free, with just
      // a few kilobytes locked
      // in the caches. These chunks then become unavailable to the other arenas, which causes
      // execution to run out of
      // memory.
      // Try to change Netty's behavior before the default allocator is being created by Netty. This
      // needs to be done once.
      LOGGER.debug("Configuring Netty allocator");
      final String previousProperty =
          System.getProperty(IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY);
      try {
        System.setProperty(IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY, "false");
        checkPoolConfiguration();
      } finally {
        if (previousProperty == null) {
          System.getProperties().remove(IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY);
        } else {
          System.setProperty(
              IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY, previousProperty);
        }
      }
    }

    private static DremioRootAllocator newRoot(
        long maxAllocBytes, long estBytesPerBuf, int maxOccupancyPercent) {
      long maxBuffers =
          estBytesPerBuf > 0
              ? (long) (VM.getMaxHeapMemory() * maxOccupancyPercent * 0.01d / estBytesPerBuf)
              : Long.MAX_VALUE;
      long nettyMaxDirectMemory = SystemPropertyUtil.getLong("io.netty.maxDirectMemory", -1);
      long maxDirectMemory = 0;
      if (nettyMaxDirectMemory == 0) {
        maxDirectMemory = VM.getMaxDirectMemory();
      } else {
        maxDirectMemory = PlatformDependent.maxDirectMemory();
      }
      return DremioRootAllocator.create(Math.min(maxDirectMemory, maxAllocBytes), maxBuffers);
    }

    private static void checkPoolConfiguration() {
      boolean value = PooledByteBufAllocator.defaultUseCacheForAllThreads();
      if (value) {
        LOGGER.error(
            "!!! Root allocator configured to use cache for all threads !!! Expect memory issues");
      }
    }
  }

  /** Constructor to prevent instantiation of this static utility class. */
  private RootAllocatorFactory() {}

  /**
   * Create a new Root Allocator
   *
   * @param config the SabotConfig
   * @return a new root allocator
   */
  @VisibleForTesting // for old tests.
  public static BufferAllocator newRoot(final SabotConfig config) {
    return InternalFactory.newRoot(config.getLong(TOP_LEVEL_MAX_ALLOC), 0, 100);
  }

  /**
   * Create a new RootAllocator using a DremioConfig
   *
   * @param config The config to initialize with
   * @return a new root allocator
   */
  public static BufferAllocator newRoot(final DremioConfig config) {
    return InternalFactory.newRoot(
        config.getSabotConfig().getLong(TOP_LEVEL_MAX_ALLOC),
        config.getLong("debug.alloc.est_heap_buf_size_bytes"),
        config.getInt("debug.alloc.max_occupancy_percent"));
  }
}
