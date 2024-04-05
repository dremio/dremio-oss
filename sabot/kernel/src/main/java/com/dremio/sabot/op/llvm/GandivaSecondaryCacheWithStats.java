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
package com.dremio.sabot.op.llvm;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.gandiva.evaluator.JavaSecondaryCacheInterface;

public class GandivaSecondaryCacheWithStats implements JavaSecondaryCacheInterface {
  private final GandivaSecondaryCache instance;

  public enum CacheState {
    PRIMARY_CACHE,
    SECONDARY_CACHE,
    GANDIVA_CODEGEN
  }

  public CacheState builtFromCache = CacheState.PRIMARY_CACHE;
  private final Stopwatch readTime = Stopwatch.createUnstarted();

  private GandivaSecondaryCacheWithStats() {
    instance = GandivaSecondaryCache.getInstance();
  }

  public long getReadTime() {
    return readTime.elapsed(TimeUnit.MILLISECONDS);
  }

  public CacheState getBuiltFromCache() {
    return builtFromCache;
  }

  public static GandivaSecondaryCacheWithStats createCache() {
    // the cache should only be created when the singleton cache instance exists
    GandivaSecondaryCacheWithStats ret = new GandivaSecondaryCacheWithStats();
    if (ret.instance == null) {
      return null;
    }
    return ret;
  }

  @Override
  public BufferResult get(long addrKey, long sizeKey) {
    readTime.start();
    BufferResult res = instance.get(addrKey, sizeKey);
    readTime.stop();

    if (res != null) {
      builtFromCache = CacheState.SECONDARY_CACHE;
    } else {
      builtFromCache = CacheState.GANDIVA_CODEGEN;
    }

    return res;
  }

  @Override
  public void set(long addrKey, long sizeKey, long addrValue, long sizeValue) {
    instance.set(addrKey, sizeKey, addrValue, sizeValue);
  }

  @Override
  public void releaseBufferResult(long address) {
    instance.releaseBufferResult(address);
  }
}
