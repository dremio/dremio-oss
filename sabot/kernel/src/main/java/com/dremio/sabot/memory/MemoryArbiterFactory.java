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
package com.dremio.sabot.memory;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.memory.DremioRootAllocator;

/**
 * Factory class to create an instance of the MemoryArbiter
 */
public class MemoryArbiterFactory {
  private static String DREMIO_MEMORY_ARBITER_CLASS = "dremio.memory.arbiter.class";

  public static MemoryArbiter newInstance(SabotConfig sabotConfig, DremioRootAllocator rootAllocator) {
    MemoryArbiter memoryArbiter;
    if (sabotConfig.hasPath(DREMIO_MEMORY_ARBITER_CLASS)) {
      memoryArbiter = sabotConfig.getInstance(DREMIO_MEMORY_ARBITER_CLASS, MemoryArbiter.class, rootAllocator);
    } else {
      memoryArbiter = new DefaultMemoryArbiter(rootAllocator);
    }

    return memoryArbiter;
  }
}
