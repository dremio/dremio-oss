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
package com.dremio.service.accelerator.pipeline;

import java.util.ConcurrentModificationException;
import java.util.concurrent.ExecutorService;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.config.SabotConfig;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.accelerator.AccelerationService;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

/**
 * An abstraction that manages long-running acceleration updates.
 *
 * Acceleration updates run in a pipeline. Each pipeline is driven by {@link PipelineDriver} in its designated thread.
 * Drivers consist of distinct stages. Stages are executed in serially by driver thread. Each stage commits mutated final
 * state to acceleration store.
 *
 * Acceleration pipeline adopts an optimistic commit protocol utilizing distributed CAS, that is, driver backs off if it
 * cannot commit its final state to the store considering someone else has preempted.
 */
public class PipelineManager implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(PipelineManager.class);
  private static final int MAX_ATTEMPTS = 100;

  private final AccelerationStore accelerationStore;
  private final PipelineDriverManager drivers;
  private final PipelineFactory factory = new PipelineFactory();

  public PipelineManager(final Provider<KVStoreProvider> provider,
                         final Provider<String> acceleratorStorageName,
                         final MaterializationStore materializationStore,
                         final Provider<JobsService> jobsService,
                         final Provider<NamespaceService> namespaceService,
                         final Provider<CatalogService> catalogService,
                         Provider<SabotConfig> config,
                         final Provider<OptionManager> optionManager,
                         final AccelerationService accelerationService,
                         final ExecutorService executor, Provider<FileSystemPlugin> acceleratorStoragePluginProvider) {
    this.accelerationStore = new AccelerationStore(provider);
    this.drivers = new PipelineDriverManager(executor, accelerationStore, acceleratorStorageName, materializationStore, jobsService,
        namespaceService, catalogService, config, optionManager, accelerationService, acceleratorStoragePluginProvider);
  }

  public void start() {
    accelerationStore.start();
  }

  @Override
  public void close() {
    drivers.close();
  }

  @VisibleForTesting
  public boolean isPipelineCompletedOrNotStarted(final AccelerationId id) {
   return  drivers.isPipelineCompletedOrNotStarted(id);
  }

  /**
   * Runs creation pipeline for the given acceleration in background.
   * @param acceleration acceleration to create
   */
  public Acceleration create(final Acceleration acceleration) {
    final Pipeline pipeline = factory.newCreationPipeline(acceleration);
    tryExecutingOnce(pipeline, false);
    return acceleration;
  }

  /**
   * Runs update pipeline for the given target.
   * @param descriptor target state to reach
   * @return  an optional of acceleration if an acceleration corresponding to descriptor exists, absent otherwise.
   */
  public Optional<Acceleration> update(final AccelerationDescriptor descriptor) {
    final Optional<Acceleration> acceleration = accelerationStore.get(descriptor.getId());
    if (!acceleration.isPresent()) {
      return acceleration;
    }

    final Pipeline pipeline = factory.newUpdatePipeline(acceleration.get(), descriptor);
    tryExecutingUpdate(pipeline, descriptor, false);
    return acceleration;
  }

  public void analyzeInForeground(final AccelerationDescriptor descriptor) {
    final Optional<Acceleration> acceleration = accelerationStore.get(descriptor.getId());
    final Pipeline pipeline = factory.newAnalysis(acceleration.get());
    tryExecuting(pipeline, true);
  }

  public void replan(final AccelerationDescriptor descriptor) {
    final Optional<Acceleration> accOptional = accelerationStore.get(descriptor.getId());
    if (!accOptional.isPresent()) {
      logger.warn("couldn't find acceleration {}, aborting re-planning", descriptor.getId());
      return;
    }
    final Acceleration acceleration = accOptional.get();
    final Pipeline pipeline = factory.newRePlanPipeline(acceleration, descriptor);
    // if a concurrent pipeline made changes to the acceleration already we don't try to replan the layouts
    // as the "bad" may have been replanned already, and if not then soon enough a deserialization failure
    // will force them to replan again
    tryExecutingOnce(pipeline, false);
  }

  /**
   * Returns an optional of acceleration corresponding to the given id
   */
  public Optional<Acceleration> get(final AccelerationId id) {
    return accelerationStore.get(id);
  }

  private boolean tryExecuting(final Pipeline pipeline, boolean foreground) {
    try {
      final Acceleration newAcceleration = pipeline.toModel();
      accelerationStore.save(newAcceleration);
      // we are good to go. let's start the pipeline
      if (foreground) {
        drivers.executeInForeground(pipeline.withAcceleration(newAcceleration));
      } else {
        drivers.execute(pipeline.withAcceleration(newAcceleration));
      }
      return true;
    } catch (final ConcurrentModificationException ex) {
      logger.debug("pipeline is preempted.", ex);
      return false;
    }
  }

  /**
   * tries to execute a pipeline once
   * @param pipeline pipeline to be executed
   * @param foreground true if pipeline should be executed synchronously
   * @throws PipelineException if execution was preempted
   */
  private void tryExecutingOnce(final Pipeline pipeline, boolean foreground) {
    if (tryExecuting(pipeline, foreground)) {
      return;
    }

    throw new PipelineException(String.format("unable to execute pipeline[acceleration: %s]", pipeline.getIdentifier()));
  }

  /**
   * Tries to execute an update pipeline up to {@link #MAX_ATTEMPTS} times
   *
   * @param pipeline pipeline to be executed
   * @param target target descriptor
   * @throws PipelineException if something goes wrong
   */
  private void tryExecutingUpdate(final Pipeline pipeline, final AccelerationDescriptor target, boolean foreground) {
    Pipeline currentPipeline = pipeline;
    int attempt = 0;
    do {
      if (tryExecuting(currentPipeline, foreground)) {
        return;
      }

      final AccelerationId id = pipeline.getIdentifier();
      final Optional<Acceleration> latest = accelerationStore.get(id);
      if (!latest.isPresent()) {
        throw new PipelineException(String.format("unable to find acceleration[id: %s]", id.getId()));
      }
      // renew the pipeline only if there is a target i.e. it is an update pipeline
      currentPipeline = factory.newUpdatePipeline(latest.get(), target);
    } while(++attempt < MAX_ATTEMPTS);

    throw new PipelineException(String.format("unable to execute pipeline[acceleration: %s]", pipeline.getIdentifier()));
  }

}
