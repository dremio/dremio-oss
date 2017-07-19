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

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.pipeline.Stage;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.DimensionGranularity;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutDimensionField;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutFieldDescriptor;
import com.dremio.service.accelerator.proto.LogicalAggregation;
import com.dremio.service.accelerator.proto.LogicalAggregationDescriptor;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

/**
 * Updates Basic layout
 */
public class BasicLayoutUpdateStage implements Stage {
  private static final Logger logger = LoggerFactory.getLogger(BasicLayoutUpdateStage.class);

  private final Optional<LogicalAggregationDescriptor> logicalAggregation;

  protected BasicLayoutUpdateStage(final Optional<LogicalAggregationDescriptor> logicalAggregation) {
    this.logicalAggregation = logicalAggregation;
  }

  @Override
  public void execute(final StageContext context) {
    final Acceleration acceleration = context.getCurrentAcceleration();

    final Map<String, ViewFieldType> schema = FluentIterable
        .from(acceleration.getContext().getDatasetSchema().getFieldList())
        .uniqueIndex(new Function<ViewFieldType, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final ViewFieldType input) {
            return input.getName();
          }
        });

    acceleration.getContext().setLogicalAggregation(makeLogicalAggregation(schema));

    if (acceleration.getAggregationLayouts().getLayoutList().isEmpty()) {
      logger.error("Aggregation Layouts is empty for acceleration with id {}", acceleration.getId());
      return;
    }

    Layout aggregationLayout = acceleration.getAggregationLayouts().getLayoutList().get(0);

    //Modify the existing layout, but increment its version
    aggregationLayout.setVersion(aggregationLayout.getVersion() + 1);

    aggregationLayout.getDetails()
    .setDimensionFieldList(toLayoutDimensionFields(acceleration.getContext().getLogicalAggregation().getDimensionList()))
    .setMeasureFieldList(acceleration.getContext().getLogicalAggregation().getMeasureList());

    context.commit(acceleration);
  }

  private List<LayoutDimensionField> toLayoutDimensionFields(List<LayoutField> dimensionList) {
    return FluentIterable.from(dimensionList)
        .transform(new Function<LayoutField, LayoutDimensionField>() {
          @Nullable
          @Override
          public LayoutDimensionField apply(@Nullable final LayoutField input) {
            return new LayoutDimensionField()
                .setName(input.getName())
                .setTypeFamily(input.getTypeFamily())
                .setGranularity(DimensionGranularity.DATE);
          }
        })
        .toList();
  }

  private LogicalAggregation makeLogicalAggregation(final Map<String, ViewFieldType> schema) {
    return logicalAggregation.transform(new Function<LogicalAggregationDescriptor, LogicalAggregation>() {
      @Nullable
      @Override
      public LogicalAggregation apply(@Nullable final LogicalAggregationDescriptor input) {
        // remove duplicates from dimensions and measures
        // remove dimensions from measures
        Predicate<LayoutFieldDescriptor> duplicateRemover = new AccelerationUtils.DuplicateRemover<>();
        return new LogicalAggregation()
            .setDimensionList(convert(input.getDimensionList(), schema, duplicateRemover))
            .setMeasureList(convert(input.getMeasureList(), schema, duplicateRemover));
      }
    }).get();
  }

  private static List<LayoutField> convert(final List<LayoutFieldDescriptor> fieldDescriptors, final Map<String, ViewFieldType> schema, Predicate<LayoutFieldDescriptor> duplicateRemover) {
    return FluentIterable
        .from(AccelerationUtils.selfOrEmpty(fieldDescriptors))
        .filter(duplicateRemover)
        .transform(new Function<LayoutFieldDescriptor, LayoutField>() {
          @Nullable
          @Override
          public LayoutField apply(@Nullable final LayoutFieldDescriptor input) {
            final String name = input.getName();
            final String typeFamily = Preconditions.checkNotNull(schema.get(name), String.format("unable to find field[%s]", name)).getTypeFamily();
            return new LayoutField().setName(name).setTypeFamily(typeFamily);
          }
        })
        .toList();
  }

  public static BasicLayoutUpdateStage of(final LogicalAggregationDescriptor descriptor) {
    return new BasicLayoutUpdateStage(Optional.fromNullable(descriptor));
  }

}
