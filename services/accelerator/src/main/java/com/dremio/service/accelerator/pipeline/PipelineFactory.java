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

import com.dremio.service.accelerator.AccelerationMapper;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.pipeline.stages.ActivationStage;
import com.dremio.service.accelerator.pipeline.stages.AnalysisStage;
import com.dremio.service.accelerator.pipeline.stages.BasicLayoutUpdateStage;
import com.dremio.service.accelerator.pipeline.stages.LayoutDeleteStage;
import com.dremio.service.accelerator.pipeline.stages.LayoutUpdateStage;
import com.dremio.service.accelerator.pipeline.stages.PlanningStage;
import com.dremio.service.accelerator.pipeline.stages.SuggestionStage;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LogicalAggregation;
import com.dremio.service.accelerator.proto.LogicalAggregationDescriptor;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A factory class that creates a {@link Pipeline} for creating or updating an acceleration.
 */
public class PipelineFactory {

  /**
   * Returns a new creation pipeline that will create the given acceleration.
   *
   * @param acceleration  acceleration to create
   */
  public Pipeline newCreationPipeline(final Acceleration acceleration) {
    final List<Stage> stages = ImmutableList.of(
        AnalysisStage.of(),
        SuggestionStage.of(),
        PlanningStage.of(),
        ActivationStage.of()
    );

    return Pipeline.of(acceleration, stages);
  }

  public Pipeline newAnalysis(final Acceleration acceleration) {
    final List<Stage> stages = ImmutableList.<Stage>of(AnalysisStage.of());
    return Pipeline.of(acceleration, stages);
  }

  /**
   * Returns a new pipeline that transitions from the given acceleration to target acceleration descriptor.
   *
   * @param acceleration current acceleration
   * @param target target state to converge
   */
  public Pipeline newUpdatePipeline(final Acceleration acceleration, final AccelerationDescriptor target) {
    final AccelerationMode targetMode = target.getMode();
    final List<Stage> stages = Lists.newArrayList();
    final AccelerationContext oldContext = acceleration.getContext();

    if (targetMode == AccelerationMode.AUTO) {
      // make sure we have analysis
      if (oldContext.getAnalysis() == null) {
        stages.add(AnalysisStage.of());
      }

      final LogicalAggregationDescriptor oldLogical = AccelerationMapper
          .toLogicalAggregationDescriptor(Optional
              .fromNullable(oldContext.getLogicalAggregation())
              .or(new LogicalAggregation())
          );

      final LogicalAggregationDescriptor newLogical = target.getContext().getLogicalAggregation();

      if (oldLogical.getDimensionList().isEmpty() && oldLogical.getDimensionList().isEmpty()) {
        // no logical aggregation was ever provided so system will suggest
        stages.add(SuggestionStage.of());
      } else if (!oldLogical.equals(newLogical)) {
        // There is an existing logical aggregation and the new one is different
        stages.add(BasicLayoutUpdateStage.of(newLogical));
      }
    } else if (targetMode == AccelerationMode.MANUAL) {
      final List<LayoutDescriptor> oldRawLayouts = AccelerationMapper.toLayoutDescriptors(AccelerationUtils.selfOrEmpty(acceleration.getRawLayouts().getLayoutList()));
      final List<LayoutDescriptor> oldAggLayouts = AccelerationMapper.toLayoutDescriptors(AccelerationUtils.selfOrEmpty(acceleration.getAggregationLayouts().getLayoutList()));

      final List<LayoutDescriptor> newRawLayouts = AccelerationUtils.selfOrEmpty(target.getRawLayouts().getLayoutList());
      final List<LayoutDescriptor> newAggLayouts = AccelerationUtils.selfOrEmpty(target.getAggregationLayouts().getLayoutList());

      if (!oldRawLayouts.equals(newRawLayouts) || !oldAggLayouts.equals(newAggLayouts)) {
        stages.add(LayoutUpdateStage.of(newRawLayouts, newAggLayouts));
      }
    } else {
      throw new UnsupportedOperationException(String.format("unsupported acceleration mode: %s", targetMode));
    }

    final boolean rawEnabled = target.getRawLayouts().getEnabled();
    final boolean aggEnabled = target.getAggregationLayouts().getEnabled();

    // If we have queued something to the stages before, it means that we need to plan again (e.g. layout has been updated
    // or we have queued analysis/suggestion stages).  So, queue planning if acceleration is enabled.
    //
    // If an error caused the previous pipeline to fail before PlanningStage completed, then we will need to redo planning.
    if ((rawEnabled || aggEnabled) && (!stages.isEmpty()|| AccelerationUtils.anyLayoutNeedPlanning(acceleration))) {
      stages.add(PlanningStage.of());
    }


    final AccelerationState finalState = computeFinalState(target, rawEnabled, aggEnabled);

    stages.add(ActivationStage.of(rawEnabled, aggEnabled, finalState, targetMode));

    return Pipeline.of(acceleration, stages);
  }

  public Pipeline newRePlanPipeline(final Acceleration acceleration, final AccelerationDescriptor descriptor) {
    final AccelerationMode targetMode = descriptor.getMode();
    final List<Stage> stages = Lists.newArrayList();

    // first delete all existing layouts to prevent further deserialization issues until next stage completes
    stages.add(LayoutDeleteStage.of());

    final List<LayoutDescriptor> rawLayouts = AccelerationUtils.selfOrEmpty(descriptor.getRawLayouts().getLayoutList());
    final List<LayoutDescriptor> aggLayouts = AccelerationUtils.selfOrEmpty(descriptor.getAggregationLayouts().getLayoutList());

    // add back all existing layouts
    stages.add(LayoutUpdateStage.of(rawLayouts, aggLayouts, true));

    final boolean rawEnabled = descriptor.getRawLayouts().getEnabled();
    final boolean aggEnabled = descriptor.getAggregationLayouts().getEnabled();
    if (rawEnabled || aggEnabled) {
      stages.add(PlanningStage.of());
    }

    final AccelerationState finalState = computeFinalState(descriptor, rawEnabled, aggEnabled);

    stages.add(ActivationStage.of(rawEnabled, aggEnabled, finalState, targetMode));

    return Pipeline.of(acceleration, stages);
  }

  private static AccelerationState computeFinalState(final AccelerationDescriptor descriptor,
      boolean rawEnabled, boolean aggEnabled) {
    final AccelerationState finalState;
    switch (descriptor.getState()) {
      case ENABLED_SYSTEM:
        finalState = AccelerationState.ENABLED_SYSTEM;
        break;
      case REQUESTED:
        finalState = AccelerationState.REQUESTED;
        break;
      default: {
        if (rawEnabled || aggEnabled) {
          finalState = AccelerationState.ENABLED;
        } else {
          finalState = AccelerationState.DISABLED;
        }
      }
    }
    return finalState;
  }
}
