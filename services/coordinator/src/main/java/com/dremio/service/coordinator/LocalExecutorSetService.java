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

import javax.inject.Provider;

import com.dremio.exec.enginemanagement.proto.EngineManagementProtos;
import com.dremio.service.coordinator.ClusterCoordinator.Role;

/**
 * Product implementation of ExecutorSetService.
 */
public class LocalExecutorSetService implements ExecutorSetService {
  private final Provider<ClusterCoordinator> coordinator;

  public LocalExecutorSetService(Provider<ClusterCoordinator> coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  public void start() {
  }

  @Override
  public ListenableSet getExecutorSet(EngineManagementProtos.EngineId engineId, EngineManagementProtos.SubEngineId subEngineId) {
    return coordinator.get().getServiceSet(Role.EXECUTOR);
  }

  @Override
  public void close() throws Exception {
  }
}
