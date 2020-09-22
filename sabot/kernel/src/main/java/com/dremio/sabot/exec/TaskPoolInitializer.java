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
package com.dremio.sabot.exec;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.liveness.LiveHealthMonitor;
import com.dremio.config.DremioConfig;
import com.dremio.options.OptionManager;
import com.dremio.sabot.task.TaskPool;
import com.dremio.sabot.task.TaskPoolFactory;
import com.dremio.sabot.task.TaskPools;
import com.dremio.service.Service;

/**
 * Instantiates {@link TaskPool} and adds to the providers' registry
 */
public class TaskPoolInitializer implements Service, LiveHealthMonitor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TaskPoolInitializer.class);
  private final Provider<OptionManager> optionManager;
  private final DremioConfig dremioConfig;

  private TaskPool pool;

  public TaskPoolInitializer(
      Provider<OptionManager> optionManager,
      DremioConfig dremioConfig
  ) {
    this.optionManager = optionManager;
    this.dremioConfig = dremioConfig;
  }

  @Override
  public void start() throws Exception {
    final TaskPoolFactory factory = TaskPools.newFactory(dremioConfig.getSabotConfig());
    pool = factory.newInstance(optionManager.get(), dremioConfig);
  }

  /**
   * Return the TaskPool instance to use.
   *
   * @return the TaskPool instance
   */
  public TaskPool getTaskPool() {
    return pool;
  }

  public boolean isTaskPoolHealthy() {
    if (pool == null) {
      return true;
    }

    return pool.areAllThreadsAlive();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(pool);
  }

  @Override
  public boolean isHealthy() {
    boolean healthy = isTaskPoolHealthy();
    if (!healthy) {
      logger.error("One of the slicing threads is dead, returning an error");
    }
    return healthy;
  }
}
