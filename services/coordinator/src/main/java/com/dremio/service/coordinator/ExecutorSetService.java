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
package com.dremio.service.coordinator;

import com.dremio.exec.enginemanagement.proto.EngineManagementProtos;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.service.Service;

/**
 * Service that provides executor sets.
 */
@Options
public interface ExecutorSetService extends Service {

  // Whether to exclude or include incompatible executors in query planning and execution
  // true - exclude
  // false - Include
  BooleanValidator DREMIO_VERSION_CHECK = new BooleanValidator("dremio.coordinator.enable_version_check", true);

  /**
   * Get the set of executors with the given queue tag.
   *
   * @param engineId top level engine identifier
   * @param subEngineId the actual subengine of that engine
   * @return set of executors.
   */
  ListenableSet getExecutorSet(EngineManagementProtos.EngineId engineId, EngineManagementProtos.SubEngineId subEngineId);

  /**
   * Get the set of executors when engineId and subEngineId are not applicable
   * @return
   */
  default ListenableSet getExecutorSet() {
    return getExecutorSet(null, null);
  }
}
