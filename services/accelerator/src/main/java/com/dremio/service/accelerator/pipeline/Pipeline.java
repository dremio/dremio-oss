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

import java.util.List;

import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.pipeline.AccelerationPipeline;
import com.dremio.service.accelerator.proto.pipeline.PipelineState;
import com.google.common.collect.ImmutableList;

/**
 * Acceleration pipeline
 */
public final class Pipeline {

  private final Acceleration base;
  private final List<Stage> stages;

  private Pipeline(final Acceleration acceleration, final List<Stage> stages) {
    this.base = acceleration;
    this.stages = stages;
  }

  public AccelerationId getIdentifier() {
    return base.getId();
  }

  public List<Stage> getStages() {
    return stages;
  }

  public Acceleration getAccelerationClone() {
    return PipelineUtils.clone(base);
  }

  public Pipeline withAcceleration(final Acceleration newAcceleration) {
    return Pipeline.of(newAcceleration, stages);
  }

  public Acceleration toModel() {
    final AccelerationPipeline pipeline = new AccelerationPipeline()
        .setState(PipelineState.PENDING);
    return getAccelerationClone().setPipeline(pipeline);
  }

  public static Pipeline of(final Acceleration acceleration, final List<Stage> stages) {
    return new Pipeline(PipelineUtils.clone(acceleration), ImmutableList.copyOf(stages));
  }
}
