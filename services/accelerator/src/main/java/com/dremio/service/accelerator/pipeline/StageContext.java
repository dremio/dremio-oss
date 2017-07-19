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

import org.apache.calcite.rel.RelNode;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.accelerator.AccelerationService;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.pipeline.PipelineState;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceService;


/**
 * Context passed to individual {@link Stage}
 */
public class StageContext {
  private final Acceleration original;
  private final Acceleration acceleration;
  private final AccelerationStore accelerationStore;
  private final String acceleratorStorageName;
  private final MaterializationStore materializationStore;
  private final JobsService jobsService;
  private final NamespaceService namespaceService;
  private final SabotConfig config;
  private final OptionManager optionManager;
  private final AccelerationService accelerationService;
  private final ExecutorService executorService;
  private final boolean isLast;
  private RelNode accelerationAnalysisPlan;
  private final FileSystemPlugin acceleratorStoragePlugin;
  private final CatalogService catalogService;

  public StageContext(final Acceleration original,
                      final Acceleration acceleration,
                      final AccelerationStore accelerationStore,
                      final String acceleratorStorageName,
                      final MaterializationStore materializationStore,
                      final JobsService jobsService,
                      final NamespaceService namespaceService,
                      final CatalogService catalogService,
                      final SabotConfig config,
                      final OptionManager optionManager,
                      final AccelerationService accelerationService,
                      final ExecutorService executorService,
                      final boolean isLast,
                      final FileSystemPlugin fileSystemPlugin) {
    this.original = original;
    this.acceleration = acceleration;
    this.accelerationStore = accelerationStore;
    this.acceleratorStorageName = acceleratorStorageName;
    this.materializationStore = materializationStore;
    this.jobsService = jobsService;
    this.namespaceService = namespaceService;
    this.config = config;
    this.optionManager = optionManager;
    this.accelerationService = accelerationService;
    this.executorService = executorService;
    this.isLast = isLast;
    this.accelerationAnalysisPlan = null;
    this.acceleratorStoragePlugin = fileSystemPlugin;
    this.catalogService = catalogService;
  }

  public Acceleration getOriginalAcceleration() {
    return PipelineUtils.clone(original);
  }

  public Acceleration getCurrentAcceleration() {
    return acceleration;
  }

  public String getAcceleratorStorageName() {
    return acceleratorStorageName;
  }

  public MaterializationStore getMaterializationStore() {
    return materializationStore;
  }

  public JobsService getJobsService() {
    return jobsService;
  }

  public NamespaceService getNamespaceService() {
    return namespaceService;
  }

  public CatalogService getCatalogService() {
    return catalogService;
  }

  public SabotConfig getConfig() {
    return config;
  }

  public OptionManager getOptionManager() {
    return optionManager;
  }

  public AccelerationService getAccelerationService() {
    return accelerationService;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public RelNode getAccelerationAnalysisPlan() {
    return accelerationAnalysisPlan;
  }

  public void setAccelerationAnalysisPlan(RelNode accelerationAnalysisPlan) {
    this.accelerationAnalysisPlan = accelerationAnalysisPlan;
  }

  public void commit(final Acceleration acceleration) {
    if (isLast) {
      acceleration.getPipeline().setState(PipelineState.DONE);
    }
    accelerationStore.save(acceleration);
  }

  public FileSystemPlugin getAcceleratorStoragePlugin() {
    return acceleratorStoragePlugin;
  }

}
