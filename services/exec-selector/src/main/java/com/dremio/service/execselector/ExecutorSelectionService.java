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
package com.dremio.service.execselector;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.TypeValidators.StringValidator;
import com.dremio.service.Service;

/**
 * A service that allows execution scheduling to choose a set of executors that will run a query
 */
@Options
public interface ExecutorSelectionService extends Service {
  String DEFAULT_SELECTOR_TYPE = "default";

  StringValidator EXECUTOR_SELECTION_TYPE = new StringValidator("exec.selection.type", DEFAULT_SELECTOR_TYPE);
  PositiveLongValidator TARGET_NUM_FRAGS_PER_NODE = new PositiveLongValidator("exec.selection.frags_per_node", Integer.MAX_VALUE, 8);

  /**
   * Get the executor endpoints that can execute a query of size 'querySize'
   * @param desiredNumExecutors  Desired number of executors
   * @param executorSelectionContext Context information
   */
  ExecutorSelectionHandle getExecutors(int desiredNumExecutors, ExecutorSelectionContext executorSelectionContext);

  /**
   * Get all active executor endpoints. Used for "select sys.*" queries.
   *
   * @param desiredNumExecutors
   * @param executorSelectionContext
   * @return
   */
  ExecutorSelectionHandle getAllActiveExecutors(ExecutorSelectionContext executorSelectionContext);
}
