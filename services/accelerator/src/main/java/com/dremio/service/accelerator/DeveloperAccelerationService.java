/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.accelerator;

import com.dremio.service.accelerator.store.MaterializationStore;

/**
 * Extra developer interface for accelerator service.
 *
 * To use with caution
 */
public interface DeveloperAccelerationService {
  /**
   * Returns if query acceleration is enabled.
   *
   * Returns true if query acceleration is enabled, which means that
   * accelerated versions of datasets are provided to the query engine
   * in order to take advantage of them
   *
   * @return true if query acceleration is enabled, false otherwise.
   */
  boolean isQueryAccelerationEnabled();

  /**
   * Configure if query acceleration is enabled or not
   *
   * Set it to true to allow the acceleration service to provide the list
   * of accelerated versions to the query engine.
   *
   * @param enabled true if accelerator service provides the list of accelerated versions
   *                to the query engine
   */
  void enableQueryAcceleration(boolean enabled);

  /**
   *
   * @return
   */
  DependencyGraph getDependencyGraph();

  /**
   * get the materialization store
   * @return
   */
  MaterializationStore getMaterializationStore();

  /**
   * Trigger an acceleration build
   *
   * The service will asynchronously creates materialization tasks for all registered accelerated datasets.
   */
  void triggerAccelerationBuild();

  /**
   * Trigger a compaction
   *
   * All unnecessary materializations will be pruned from the accelerator service.
   */
  void triggerCompaction();

  /**
   * Clears all accelerations.
   */
  void clearAllAccelerations();

  /**
   * removes all accelerations which is not associated with a dataset
   */
  void removeOrphanedAccelerations();
}
