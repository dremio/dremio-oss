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

import java.util.Collection;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.options.OptionManager;

/**
 * GroupResourceInformation implementation when only a subset of executors are selected for the query.
 * This happens in Software/DCS edition where engines can be spawned as required and query will be
 * scheduled on particular engines only.
 */
public class SelectedExecutorsResourceInformation implements GroupResourceInformation {
  private long averageExecutorMemory;
  private int averageExecutorCores;
  private int executorCount;
  public SelectedExecutorsResourceInformation(final Collection<CoordinationProtos.NodeEndpoint> executors) {
    if (executors == null || executors.isEmpty()) {
      averageExecutorMemory = 0;
      averageExecutorCores = 0;
      executorCount = 0;
    } else {
      executorCount = executors.size();
      long totalDirectMemory = 0;
      int totalCoresAcrossExecutors = 0;
      for (final CoordinationProtos.NodeEndpoint endpoint : executors) {
        totalCoresAcrossExecutors += endpoint.getAvailableCores();
        totalDirectMemory += endpoint.getMaxDirectMemory();
      }
      averageExecutorCores = totalCoresAcrossExecutors / executorCount;
      averageExecutorMemory = totalDirectMemory / executorCount;
    }
  }

  @Override
  public long getAverageExecutorMemory() {
    return averageExecutorMemory;
  }

  @Override
  public int getExecutorNodeCount() {
    return executorCount;
  }

  @Override
  public long getAverageExecutorCores(OptionManager optionManager) {
    long configuredMaxWidthPerNode = optionManager.getOption(MAX_WIDTH_PER_NODE_KEY).getNumVal();
    if (configuredMaxWidthPerNode == 0) {
      /* user has not overridden the default, use the default MAX_WIDTH_PER_NODE which is average
       * number of cores as computed by ClusterResourceInformation.
       */
      return Math.round(averageExecutorCores * 0.7);
    } else {
      return configuredMaxWidthPerNode;
    }
  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public void close() throws Exception {

  }
}
