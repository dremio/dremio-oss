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
package com.dremio.resource;

import com.dremio.options.OptionResolver;
import com.dremio.options.TypeValidators;
import com.dremio.service.Service;

/**
 * Resource information about executors. These are approximate/average, and can be used
 * for planning purposes.
 */
public interface GroupResourceInformation extends Service {
  // duplicate of name in ExecConstants. Taking a dependency on sabot-kernel will cause
  // circular dependency.
  TypeValidators.LongValidator MAX_WIDTH_PER_NODE_KEY =
      new TypeValidators.LongValidator("planner.width.max_per_node", 0L);

  /**
   * Get the average maximum direct memory of executors in the cluster.
   *
   * @return average maximum direct memory of executors
   */
  long getAverageExecutorMemory();

  /**
   * Get the number of executors.
   * @return Number of registered executors.
   */
  int getExecutorNodeCount();

  /**
   * Get the average number of cores in executor nodes.
   * This will be used as the default value of MAX_WIDTH_PER_NODE
   *
   * @return average number of executor cores
   */
  long getAverageExecutorCores(final OptionResolver optionManager);
}
