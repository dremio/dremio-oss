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

import com.dremio.service.accelerator.analysis.AccelerationSuggestor;
import com.dremio.service.accelerator.pipeline.Stage;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.proto.Acceleration;

/**
 * Suggests layouts
 */
public class SuggestionStage implements Stage {
  private static final Logger logger = LoggerFactory.getLogger(SuggestionStage.class);
  private static final int MAX_DIMENSION_FIELDS = 3;

  @Override
  public void execute(final StageContext context) {
    logger.info("suggesting....");
    final Acceleration acceleration = context.getCurrentAcceleration();

    final AccelerationSuggestor suggestor = new AccelerationSuggestor(MAX_DIMENSION_FIELDS);

    suggestor.writeAggregationLayouts(acceleration);
    suggestor.writeRawLayouts(acceleration);

    context.commit(acceleration);
  }

  public static SuggestionStage of() {
    return new SuggestionStage();
  }

}
