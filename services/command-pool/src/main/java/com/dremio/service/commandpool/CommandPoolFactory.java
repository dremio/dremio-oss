/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.commandpool;

import com.dremio.common.VM;
import com.dremio.config.DremioConfig;

/**
 * {@link CommandPool} factory.
 */
public class CommandPoolFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CommandPoolFactory.class);

  public static final CommandPoolFactory INSTANCE = new CommandPoolFactory();

  private static final String ENABLED = "services.coordinator.command-pool.enabled";
  private static final String POOL_SIZE = "services.coordinator.command-pool.size";
  /**
   * @return new {@link CommandPool} instance
   */
  public CommandPool newPool(final DremioConfig config) {
    if (config.getBoolean(ENABLED)) {
      final int poolSize = getPoolSize(config);
      logger.info("Starting bound command pool of size {}", poolSize);
      return new BoundCommandPool(poolSize);
    }

    logger.info("Starting unbound command pool");
    return new SameThreadCommandPool();
  }

  private int getPoolSize(final DremioConfig config) {
    int poolSize = config.getInt(POOL_SIZE);
    poolSize = poolSize > 0 ? poolSize : VM.availableProcessors() - 1; // make sure we don't use all cores by default
    return Math.max(1, poolSize); // in the unlikely case where the cpu has a single core
  }
}
