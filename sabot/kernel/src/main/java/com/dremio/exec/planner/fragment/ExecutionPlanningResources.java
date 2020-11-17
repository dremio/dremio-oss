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
package com.dremio.exec.planner.fragment;

import com.dremio.resource.GroupResourceInformation;
import com.dremio.service.execselector.ExecutorSelectionHandle;

/**
 * Resources needed for execution planning. Closed when the query completes
 */
public class ExecutionPlanningResources implements AutoCloseable {
  private final PlanningSet planningSet;
  private final ExecutorSelectionHandle executorSelectionHandle;
  private final GroupResourceInformation groupResourceInformation;

  public ExecutionPlanningResources(PlanningSet planningSet, ExecutorSelectionHandle executorSelectionHandle, GroupResourceInformation groupResourceInformation) {
    this.planningSet = planningSet;
    this.executorSelectionHandle = executorSelectionHandle;
    this.groupResourceInformation = groupResourceInformation;
  }

  public PlanningSet getPlanningSet() {
    return planningSet;
  }

  public ExecutorSelectionHandle getExecutorSelectionHandle() {
    return executorSelectionHandle;
  }

  public com.dremio.resource.GroupResourceInformation getGroupResourceInformation() {
    return groupResourceInformation;
  }

  public void close() throws Exception {
    executorSelectionHandle.close();
  }
}
