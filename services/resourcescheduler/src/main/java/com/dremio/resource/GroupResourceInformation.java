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
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.Service;

/**
 * Resource information about executors. These are approximate/average, and can be used
 * for planning purposes.
 */
@Options
public interface GroupResourceInformation extends Service {
  /**
   * Limits the maximum level of parallelization to this factor time the number of Nodes.
   * The default value is internally computed based on number of cores per executor, using MAX_WIDTH_PER_NODE_FRACTION_KEY.
   * The default value mentioned here is meaningless and is only used to ascertain if user has explicitly set the value
   * or not.
   */
  String MAX_WIDTH_PER_NODE_KEY = "planner.width.max_per_node";
  TypeValidators.PositiveLongValidator MAX_WIDTH_PER_NODE = new TypeValidators.PositiveLongValidator(MAX_WIDTH_PER_NODE_KEY, Integer.MAX_VALUE, 0);

  String MAX_WIDTH_PER_NODE_FRACTION_KEY = "planner.width.max_per_node_fraction";
  TypeValidators.RangeDoubleValidator MAX_WIDTH_PER_NODE_FRACTION = new TypeValidators.RangeDoubleValidator(MAX_WIDTH_PER_NODE_FRACTION_KEY, 0.1, 2.0, 0.75);


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

  /**
   * Compute the number o cores available for dremio execution.
   *
   * @param coresPerNode total cores reported by the executor node.
   * @param optionManager options
   * @return number of cores to be used by dremio for execution.
   */
  static long computeCoresAvailableForExecutor(int coresPerNode, OptionResolver optionManager) {
    // If the MAX_WIDTH_PER_NODE_KEY is set to a non-zero value, that takes precedence.
    long configuredMaxWidthPerNode = optionManager.getOption(MAX_WIDTH_PER_NODE);
    if (configuredMaxWidthPerNode != 0) {
      return configuredMaxWidthPerNode;
    }

    return Math.round(coresPerNode * optionManager.getOption(MAX_WIDTH_PER_NODE_FRACTION));
  }
}
