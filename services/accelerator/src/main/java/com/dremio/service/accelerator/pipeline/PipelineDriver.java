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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.AsyncTask;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.pipeline.AccelerationPipeline;
import com.dremio.service.accelerator.proto.pipeline.PipelineState;
import com.google.common.base.Optional;

/**
 * Pipeline driver that manages an individual {@link Pipeline}
 */
public class PipelineDriver extends AsyncTask {
  private static final Logger logger = LoggerFactory.getLogger(PipelineDriver.class);

  private final AtomicBoolean isRetired = new AtomicBoolean(false);
  private final Pipeline pipeline;
  private final PipelineContext context;
  private final PipelineStatusListener listener;

  public PipelineDriver(final Pipeline pipeline, final PipelineContext context, final PipelineStatusListener listener) {
    this.pipeline = pipeline;
    this.context = context;
    this.listener = listener;
  }

  @Override
  public void doRun() {
    final AccelerationId id = pipeline.getIdentifier();
    logger.info("acceleration pipeline started. [id: {}]", id.getId());

    listener.onStart(pipeline);
    final Acceleration acceleration = pipeline.getAccelerationClone();
    try {
      runPipeline(acceleration);
      listener.onComplete(pipeline);
      logger.info("acceleration pipeline is completed. [id: {}]", id.getId());
    } catch (final Throwable cause) {
      logger.error("acceleration pipeline failed. [id: {}]", id.getId(), cause);
      failPipeline(acceleration, cause);
      listener.onFailure(pipeline, cause);
    }
  }

  private void failPipeline(final Acceleration acceleration, final Throwable cause) {
    final AccelerationPipeline pipeline = Optional.fromNullable(acceleration.getPipeline()).or(new AccelerationPipeline());

    acceleration
        .setState(AccelerationState.ERROR)
        .setMode(AccelerationMode.MANUAL);

    pipeline
        .setState(PipelineState.FAILED)
        .setFailureDetails(new AccelerationPipeline.FailureDetails()
            .setMessage(cause.getMessage())
            .setStackTrace(AccelerationUtils.getStackTrace(cause))
        );

    try {
      context.getAccelerationStore().save(acceleration);
    } catch (final ConcurrentModificationException ex) {
      logger.info("unable to persist pipeline failure due to pre-emption.", ex);
    }
  }

  /**
   * Runs pipeline for the given acceleration
   */
  private void runPipeline(final Acceleration acceleration) {
    final List<Stage> stages = pipeline.getStages();
    final Acceleration original = pipeline.getAccelerationClone();

    acceleration.getPipeline()
        .setState(PipelineState.WORKING)
        .setFailureDetails(null);
    for (int i = 0, n = stages.size(); i < n; i++) {
      if (isRetired.get()) {
        logger.info("acceleration pipeline is retired. [id: {}]", pipeline.getIdentifier().getId());
        return;
      }

      final Stage stage = stages.get(i);
      final boolean isLast = i == n-1;

      // note that we use a mutable acceleration instance across stages. this is by design.
      final StageContext stageContext = new StageContext(original, acceleration, context.getAccelerationStore(),
          context.getAcceleratorStorageName(), context.getMaterializationStore(), context.getJobsService(), context.getNamespaceService(),
          context.getCatalogService(), context.getConfig(), context.getOptionManager(), context.getAccelerationService(),
          context.getExecutorService(), isLast, context.getAcceleratorStoragePlugin());

      try {
        stage.execute(stageContext);
      } catch (final ConcurrentModificationException ex) {
        logger.info("acceleration pipeline is preempted. yielding...", ex);
        listener.onPreemption(pipeline);
        return;
      }
    }
  }

  public void retire() {
    isRetired.set(true);
  }

}
