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

import com.dremio.datastore.IndexedStore;
import com.dremio.service.Service;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationId;
import com.dremio.service.accelerator.proto.SystemSettings;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

/**
 * An indexed service that manages accelerations.
 */
public interface AccelerationService extends Service {

  /**
   * Returns a sequence of accelerations conforming to given query.
   *
   * @param condition list query
   */
  Iterable<Acceleration> getAccelerations(IndexedStore.FindByCondition condition);

  /**
   * Returns an optional of acceleration associated with the given id
   * @param id look-up id
   */
  Optional<Acceleration> getAccelerationById(AccelerationId id);

  /**
   * Returns an optional of acceleration entry associated with the given id
   * @param id look-up id
   */
  Optional<AccelerationEntry> getAccelerationEntryById(AccelerationId id);

  /**
   * Returns an optional of acceleration entry associated with the given dataset path
   * @param path look-up path
   */
  Optional<AccelerationEntry> getAccelerationEntryByDataset(NamespaceKey path);

  /**
   * Returns an optional of acceleration entry associated with the given layout id
   * @param layoutId job id for look-up
   */
  Optional<AccelerationEntry> getAccelerationEntryByLayoutId(final LayoutId layoutId);

  /**
   * Returns an optional of acceleration entry associated with the given job id
   * @param id job id for look-up
   */
  Optional<AccelerationEntry> getAccelerationEntryByJob(JobId id);


  /**
   * Creates the given acceleration asynchronously via acceleration pipeline and returns corresponding acceleration entry.
   * @param acceleration acceleration to create
   * @return resulting acceleration entry
   */
  AccelerationEntry create(Acceleration acceleration);

  /**
   * Updates acceleration corresponding to the given entry asynchronously via acceleration pipeline and returns the updated entry
   * @param entry  entry to updated
   * @return updated entry
   */
  AccelerationEntry update(AccelerationEntry entry);

  /**
   * forces a layout to re-plan.<br>
   * In practice this will force the whole acceleration to re-plan, causing all materializations associated with
   * it to be effectively dropped
   * @param layoutId layout to re-plan
   */

  void replan(LayoutId layoutId);
  /**
   * Returns a sequence of materializations associated with the given layout id.
   * @param layoutId  layout id whose materializations are requested.
   */
  Iterable<Materialization> getMaterializations(LayoutId layoutId);

  /**
   * Removes acceleration, its layouts and drops it materializations.
   * @param id acceleration id to wipe off
   */
  void remove(AccelerationId id);

  /**
   * Rebuilds and returns acceleration refresh graph
   */
  DependencyGraph buildRefreshDependencyGraph();

  /**
   * Returns global settings.
   */
  SystemSettings getSettings();

  /**
   * Configures service with the given system settings.
   */
  void configure(SystemSettings settings);


  /**
   * Returns state of AccelerationPipeline (in progress or notstarted/completed
   * Should be used only for testing, as it may not give precise answer
   * @param id
   * @return
   */
  boolean isPipelineCompletedOrNotStarted(AccelerationId id);

  /**
   * return true if the materialization is cached
   * @param materializationId
   * @return
   */
  boolean isMaterializationCached(MaterializationId materializationId);

  /**
   * Access developer services
   *
   * To use with caution, only for test and development purposes
   *
   * @throws UnsupportedOperationException if the service doesn't support
   * a developer extra interface
   */
  @VisibleForTesting
  DeveloperAccelerationService developerService();

}
