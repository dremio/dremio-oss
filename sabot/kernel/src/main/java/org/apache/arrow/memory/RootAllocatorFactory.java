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
package org.apache.arrow.memory;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.memory.DremioRootAllocator;

public class RootAllocatorFactory {

  public static final String TOP_LEVEL_MAX_ALLOC = "dremio.memory.top.max";

  /**
   * Constructor to prevent instantiation of this static utility class.
   */
  private RootAllocatorFactory() {}

  /**
   * Create a new Root Allocator
   * @param config
   *          the SabotConfig
   * @return a new root allocator
   */
  public static BufferAllocator newRoot(final SabotConfig config) {
    return new DremioRootAllocator(Math.min(SabotConfig.getMaxDirectMemory(), config.getLong(TOP_LEVEL_MAX_ALLOC)));
  }
}
