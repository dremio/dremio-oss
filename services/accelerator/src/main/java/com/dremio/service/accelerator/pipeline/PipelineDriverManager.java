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

import java.util.concurrent.ExecutorService;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.config.SabotConfig;
import com.dremio.concurrent.RenamingCallable;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.accelerator.AccelerationService;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * An abstraction that manages {@link PipelineDriver drivers}.
 */
class PipelineDriverManager implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(PipelineDriverManager.class);

  private static final String THREAD_NAME_PATTERN = "accel-pipeline-%s:%s";

  private final Cache<AccelerationId, PipelineDriver> drivers = CacheBuilder.newBuilder()
      .build();

  private final PipelineStatusListener stateListener = new PipelineStateListener();

  private final ExecutorService executor;
  private final AccelerationStore accelerationStore;
  private final Provider<String> acceleratorStorageName;
  private final MaterializationStore materializationStore;
  private final Provider<JobsService> jobsService;
  private final Provider<NamespaceService> namespaceService;
  private final Provider<CatalogService> catalogService;
  private final Provider<SabotConfig> config;
  private final Provider<OptionManager> optionManager;
  private final AccelerationService accelerationService;
  private final Provider<FileSystemPlugin> acceleratorStoragePluginProvider;

  public PipelineDriverManager(final ExecutorService executor, final AccelerationStore accelerationStore, final Provider<String> acceleratorStorageName,
                               final MaterializationStore materializationStore, final Provider<JobsService> jobsService,
                               final Provider<NamespaceService> namespaceService, final Provider<CatalogService> catalogService,
                               Provider<SabotConfig> config, final Provider<OptionManager> optionManager,
                               final AccelerationService accelerationService, Provider<FileSystemPlugin> acceleratorStoragePluginProvider) {
    this.executor = executor;
    this.accelerationStore = accelerationStore;
    this.acceleratorStorageName = acceleratorStorageName;
    this.materializationStore = materializationStore;
    this.jobsService = jobsService;
    this.namespaceService = namespaceService;
    this.config = config;
    this.optionManager = optionManager;
    this.accelerationService = accelerationService;
    this.acceleratorStoragePluginProvider = acceleratorStoragePluginProvider;
    this.catalogService = catalogService;
  }

  /**
   * Executes the given pipeline in background.
   *
   * This call retires previous pipeline working on the same acceleration, if any.
   */
  public void execute(final Pipeline pipeline) {
    final AccelerationId id = pipeline.getIdentifier();
    final PipelineContext context = new PipelineContext(accelerationStore, acceleratorStorageName.get(), materializationStore, jobsService.get(),
        namespaceService.get(), catalogService.get(), config.get(), optionManager.get(), accelerationService, executor, acceleratorStoragePluginProvider.get());
    final PipelineDriver newDriver = new PipelineDriver(pipeline, context, stateListener);
    final PipelineDriver oldDriver = drivers.asMap().put(id, newDriver);
    if (oldDriver != null) {
      logger.info("retiring old driver for {}", id.getId());
      oldDriver.retire();
    }
    executor.submit(RenamingCallable.of(newDriver, makeThreadName(pipeline.getAccelerationClone())));
  }

  public void executeInForeground(final Pipeline pipeline) {
    final PipelineContext context = new PipelineContext(accelerationStore, acceleratorStorageName.get(), materializationStore, jobsService.get(),
      namespaceService.get(), catalogService.get(), config.get(), optionManager.get(), accelerationService, executor, acceleratorStoragePluginProvider.get());
    final PipelineDriver newDriver = new PipelineDriver(pipeline, context, stateListener);
    newDriver.doRun();
    newDriver.retire();
  }

  @Override
  public void close() {
    for (final PipelineDriver driver : drivers.asMap().values()) {
      driver.retire();
    }
  }

  @VisibleForTesting
  public boolean isPipelineCompletedOrNotStarted(final AccelerationId id) {
    // either did not start or finished
    return drivers.asMap().get(id) == null;
  }

  // static methods

  private static String makeThreadName(final Acceleration acceleration) {
    return String.format(THREAD_NAME_PATTERN, acceleration.getId().getId(), acceleration.getVersion());
  }

   /**
   * Monitors pipeline state, updates running drivers.
   */
  private class PipelineStateListener implements PipelineStatusListener {

    @Override
    public void onStart(final Pipeline pipeline) {}

    @Override
    public void onFailure(final Pipeline pipeline, final Throwable cause) {
      remove(pipeline.getIdentifier());
    }

    @Override
    public void onPreemption(final Pipeline pipeline) {
      remove(pipeline.getIdentifier());
    }

    @Override
    public void onComplete(final Pipeline pipeline) {
      remove(pipeline.getIdentifier());
    }

    private void remove(final AccelerationId id) {
      drivers.asMap().remove(id);
    }
  }

}
