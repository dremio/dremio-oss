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
package com.dremio.service.accelerator.pipeline.stages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.accelerator.pipeline.Stage;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.Layout;
import com.google.common.collect.ImmutableList;

/**
 * removes all layouts from the acceleration
 */
public class LayoutDeleteStage implements Stage {
  private static final Logger logger = LoggerFactory.getLogger(LayoutDeleteStage.class);

  protected LayoutDeleteStage() {}

  @Override
  public void execute(StageContext context) {
    final Acceleration acceleration = context.getCurrentAcceleration();
    logger.info("deleting layouts for acceleration {}...", acceleration.getId());
    acceleration.setMode(AccelerationMode.MANUAL);
    acceleration.getRawLayouts().setLayoutList(ImmutableList.<Layout>of());
    acceleration.getAggregationLayouts().setLayoutList(ImmutableList.<Layout>of());

    context.commit(acceleration);
  }

  public static LayoutDeleteStage of() {
    return new LayoutDeleteStage();
  }
}
