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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dremio.service.accelerator.AccelerationMapper;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.pipeline.PipelineUtils;
import com.dremio.service.accelerator.pipeline.Stage;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetails;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.LayoutType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Updates layouts by planning and serializing them.
 */
public class LayoutUpdateStage implements Stage {
  private final List<LayoutDescriptor> rawLayouts;
  private final List<LayoutDescriptor> aggLayouts;
  private final boolean deletedLayouts; // true if DeleteLayouts stage was executed prior to this stage

  protected LayoutUpdateStage(final List<LayoutDescriptor> rawLayouts, final List<LayoutDescriptor> aggLayouts, boolean deletedLayouts) {
    this.rawLayouts = rawLayouts;
    this.aggLayouts = aggLayouts;
    this.deletedLayouts = deletedLayouts;
  }

  @Override
  public void execute(final StageContext context) {
    final Acceleration acceleration = context.getCurrentAcceleration();
    final AccelerationMapper mapper = AccelerationMapper.create(acceleration.getContext().getDatasetSchema());

    // make sure to use the original acceleration's layouts if DeleteLayout stage was run
    // otherwise all layouts will be considered as new and receive version = 0
    final Map<LayoutId, Layout> mappings = PipelineUtils.generateLayoutIdMapping(
      deletedLayouts ? context.getOriginalAcceleration() : acceleration);

    final Set<Layout> newRawLayouts = FluentIterable
        .from(rawLayouts)
        .transform(new Function<LayoutDescriptor, Layout>() {
          @Override
          public Layout apply(final LayoutDescriptor input) {
            final LayoutDetailsDescriptor details = Optional.fromNullable(input.getDetails())
                .or(new LayoutDetailsDescriptor());
            if (input.getId() == null) {
              input.setId(AccelerationUtils.newRandomId());
            }
            return new Layout()
                .setLayoutType(LayoutType.RAW)
                .setId(input.getId())
                .setName(input.getName())
                .setDetails(
                    new LayoutDetails()
                        .setDimensionFieldList(mapper.toLayoutDimensionFields(details.getDimensionFieldList()))
                        .setMeasureFieldList(mapper.toLayoutFields(details.getMeasureFieldList()))
                        .setPartitionFieldList(mapper.toLayoutFields(details.getPartitionFieldList()))
                        .setSortFieldList(mapper.toLayoutFields(details.getSortFieldList()))
                        .setDisplayFieldList(mapper.toLayoutFields(details.getDisplayFieldList()))
                        .setDistributionFieldList(mapper.toLayoutFields(details.getDistributionFieldList()))
                        .setPartitionDistributionStrategy(details.getPartitionDistributionStrategy())
                );
          }
        })
        .toSet();

    final Set<Layout> newAggLayouts = FluentIterable
        .from(aggLayouts)
        .transform(new Function<LayoutDescriptor, Layout>() {
          @Override
          public Layout apply(final LayoutDescriptor input) {
            final LayoutDetailsDescriptor details = Optional.fromNullable(input.getDetails())
                .or(new LayoutDetailsDescriptor());
            if (input.getId() == null) {
              input.setId(AccelerationUtils.newRandomId());
            }
            return new Layout()
                .setLayoutType(LayoutType.AGGREGATION)
                .setId(input.getId())
                .setName(input.getName())
                .setDetails(
                    new LayoutDetails()
                        .setDimensionFieldList(mapper.toLayoutDimensionFields(details.getDimensionFieldList()))
                        .setMeasureFieldList(mapper.toLayoutFields(details.getMeasureFieldList()))
                        .setPartitionFieldList(mapper.toLayoutFields(details.getPartitionFieldList()))
                        .setSortFieldList(mapper.toLayoutFields(details.getSortFieldList()))
                        .setDisplayFieldList(mapper.toLayoutFields(details.getDisplayFieldList()))
                        .setDistributionFieldList(mapper.toLayoutFields(details.getDistributionFieldList()))
                        .setPartitionDistributionStrategy(details.getPartitionDistributionStrategy())
                );
          }
        })
        .toSet();

    setVersion(mappings, newRawLayouts, newAggLayouts);

    acceleration.setMode(AccelerationMode.MANUAL);
    acceleration.getRawLayouts().setLayoutList(ImmutableList.copyOf(newRawLayouts));
    acceleration.getAggregationLayouts().setLayoutList(ImmutableList.copyOf(newAggLayouts));

    context.commit(acceleration);
  }

  private void setVersion(final Map<LayoutId, Layout> mappings, final Set<Layout> newRawLayouts,
      final Set<Layout> newAggLayouts) {
    for (final Layout layout : Iterables.concat(newRawLayouts, newAggLayouts)) {
      //new layout
      if (mappings.get(layout.getId()) == null) {
        layout.setVersion(0);
      } else {
        Layout existingLayout = mappings.get(layout.getId());
        //update the version if there is a changed in LayoutDetails or if we deleted all layout as part of a replan
        if (deletedLayouts || !layout.getDetails().equals(existingLayout.getDetails())) {
          layout.setVersion(existingLayout.getVersion() + 1);
        } else {
          layout.setVersion(existingLayout.getVersion());
        }
      }
    }
  }

  public static LayoutUpdateStage of(final List<LayoutDescriptor> rawLayouts, final List<LayoutDescriptor> aggLayouts,
      boolean deletedLayouts) {
    return new LayoutUpdateStage(rawLayouts, aggLayouts, deletedLayouts);
  }

  public static LayoutUpdateStage of(final List<LayoutDescriptor> rawLayouts, final List<LayoutDescriptor> aggLayouts) {
    return of(rawLayouts, aggLayouts, false);
  }
}
